import { randomUUID } from "node:crypto";
import { BeaconService, type BeaconAdvertisement } from "./beacon.ts";
import { ConcordService, ContractValidityStatus, type ConcordAgreement, type ContractHandle, type ContractPointer, type ContractRecord } from "./concord.ts";
import { parseEndpointAddress } from "./endpoint.ts";
import { StateConflict, StateUnavailable, ValidationError } from "./errors.ts";
import { canonicalJson, requireJsonObject, type JsonObject } from "./json.ts";
import { findHardwareCapability, validateHardwareCapabilityPayload, type HardwareCapability } from "./hardware-descriptors.ts";
import { HARDWARE_LANE, validateHardwareDeviceRef, validateHardwareMessageBody, type HardwareMessageType } from "./hardware-messages.ts";
import { HARDWARE_ADVERTISEMENT_PROFILE, HARDWARE_CLAIM_PROFILE, HARDWARE_FEATURE_ID, hardwareClaimMatches, hardwareDeviceMatches, parseHardwareDescriptor, validateHardwareAdvertisementPayload, validateHardwareClaimTerms, type HardwareAdvertisementDevice, type HardwareClaimTerms, type HardwareDescriptor } from "./hardware-profile.ts";
import { entitySubject, messageIsDeliverableTo, type DeckrMessage } from "./lanes.ts";
import { validateLaneMessage } from "./lane-validation.ts";
import type { EndpointLane, MessageDelivery } from "./messaging.ts";
import { abortable, bounded, StateCell, type RuntimeFailure } from "./runtime.ts";
import { defaultPayloadSchemas, type PayloadSchemas } from "./schema.ts";

export * from "./hardware-descriptors.ts";
export * from "./hardware-messages.ts";
export * from "./hardware-profile.ts";
export * from "./hardware-capabilities.ts";

export interface HardwareState {
  readonly status: "starting" | "ready" | "reconnecting" | "failed" | "closed";
  readonly failure?: RuntimeFailure;
  readonly contract?: ContractPointer;
}
export interface HardwareOperationContext {
  readonly signal: AbortSignal;
  readonly contract: ContractPointer;
  readonly controllerEndpoint: string;
  readonly controllerSessionId: string;
  readonly device: HardwareAdvertisementDevice;
  readonly capability: HardwareCapability;
  assertActive(): Promise<void>;
}
export interface HardwareReply { messageType: HardwareMessageType; body: JsonObject; }
export interface ManagedHardwareManagerOptions {
  endpoint: EndpointLane; beacon: BeaconService; concord: ConcordService;
  devices: Record<string, HardwareAdvertisementDevice>;
  labels?: Record<string, string>;
  /** Domain admission and connection ownership remain host policy. */
  acceptClaim?: (terms: HardwareClaimTerms) => boolean | Promise<boolean>;
  handle?: (message: DeckrMessage, context: HardwareOperationContext) => HardwareReply | void | Promise<HardwareReply | void>;
  schemas?: PayloadSchemas;
  onError?: (error: unknown) => void;
}

/** Protocol ownership around host-supplied devices. No driver, routing, or UI policy. */
export class ManagedHardwareManager {
  private readonly options: ManagedHardwareManagerOptions;
  private readonly manager;
  private readonly abort = new AbortController();
  private readonly states = new StateCell<HardwareState>({ status: "starting" });
  private devices: Record<string, HardwareAdvertisementDevice>;
  private readonly reservations = new Map<string, HardwareClaimTerms>();
  private readonly reservedContracts = new Map<string, ContractHandle>();
  private readonly operations = new Set<Promise<unknown>>();
  private readonly blockedDevices = new Set<string>();
  private advertisement?: BeaconAdvertisement;
  private starting?: Promise<void>;
  private listening?: Promise<void>;
  private updateTask: Promise<void> = Promise.resolve();
  private closing?: Promise<void>;
  private epoch = 0;
  private readonly deviceVersions = new Map<string, number>();
  constructor(options: ManagedHardwareManagerOptions) {
    assertHardwareEndpoint(options.endpoint, "hardware_manager");
    this.options = options;
    this.devices = this.inventory(options.devices);
    this.manager = options.concord.participantManager({ participant: options.endpoint.endpoint, sessionId: options.endpoint.sessionId,
      profile: HARDWARE_CLAIM_PROFILE, acceptContract: (contract, record) => this.admit(contract, record),
      currentSessions: () => ({ [options.endpoint.endpoint]: options.endpoint.sessionId }), onError: error => this.report(error),
    });
  }
  get state() { return this.states.current; }
  get ready(): boolean { return this.state.status === "ready" && !this.abort.signal.aborted; }
  watch(options: { signal?: AbortSignal } = {}) { return this.states.watch(options); }
  waitFor(predicate: (state: HardwareState) => boolean, options: { signal?: AbortSignal; timeoutMs?: number } = {}) {
    return this.states.wait(predicate, { ...options, scheduler: this.options.concord.scheduler });
  }
  private inventory(devices: Record<string, HardwareAdvertisementDevice>): Record<string, HardwareAdvertisementDevice> {
    return validateHardwareAdvertisementPayload({ profile: HARDWARE_ADVERTISEMENT_PROFILE,
      managerEndpoint: this.options.endpoint.endpoint, managerId: parseEndpointAddress(this.options.endpoint.endpoint).endpointId,
      sessionId: this.options.endpoint.sessionId, devices, labels: this.options.labels ?? {} }, this.options.schemas).devices;
  }
  start(): Promise<void> {
    if (this.abort.signal.aborted) return Promise.reject(new StateUnavailable("Hardware manager is closed"));
    if (!this.starting) this.starting = abortable(this.startNow(), this.abort.signal).catch(async error => {
      if (!this.abort.signal.aborted) {
        this.states.set({ status: "failed", failure: { code: "hardware_bootstrap_failed", message: String(error) } });
        this.abort.abort();
        await bounded(Promise.allSettled([this.manager.aclose(), this.advertisement?.close(), this.listening]), 5000, this.options.concord.scheduler).catch(() => {});
      }
      throw error;
    });
    return this.starting;
  }
  private async startNow(): Promise<void> {
    await abortable(Promise.all([this.options.beacon.start(), this.options.concord.start()]), this.abort.signal);
    await this.bootstrap(); this.assertOpen();
    this.manager.start();
    // Subscribe before becoming discoverable. Domain callbacks never own this loop.
    if (this.options.handle) this.listening = this.listen();
    await this.publishInventory(); this.assertOpen();
    this.states.set({ status: "ready" });
  }
  private async bootstrap(): Promise<void> {
    try {
      await this.options.concord.bootstrap(this.options.endpoint.endpoint, this.options.endpoint.sessionId, {
        profile: HARDWARE_CLAIM_PROFILE, exclusive: true,
      });
    } catch (error) {
      if (error instanceof StateConflict && error.code === "participant_owned") throw new StateConflict("Another hardware-manager lifetime owns participation", "hardware_owner_active");
      throw error;
    }
  }

  private async publishInventory(): Promise<void> {
    this.assertOpen();
    const payload = { profile: HARDWARE_ADVERTISEMENT_PROFILE, managerEndpoint: this.options.endpoint.endpoint,
      managerId: parseEndpointAddress(this.options.endpoint.endpoint).endpointId, sessionId: this.options.endpoint.sessionId, devices: this.devices, labels: this.options.labels ?? {} } as unknown as JsonObject;
    if (!this.advertisement) this.advertisement = await this.options.beacon.ensureAdvertisement({ featureId: HARDWARE_FEATURE_ID,
      endpoint: this.options.endpoint.endpoint, sessionId: this.options.endpoint.sessionId, advertisementId: randomUUID(), payload });
    if (this.abort.signal.aborted) { await this.advertisement.close(); return; }
    await this.advertisement.publish({ payload });
    if (this.abort.signal.aborted) await this.advertisement.close();
  }
  /** Explicit wakeup after a host policy change. */
  async reconcile(): Promise<void> { await this.manager.reconcile(); }

  private async admit(contract: ContractHandle, record: ContractRecord): Promise<boolean> {
    let terms: HardwareClaimTerms;
    try { terms = validateHardwareClaimTerms(record.terms); } catch { return false; }
    if (!hardwareClaimMatches(record, terms) || terms.managerEndpoint !== this.options.endpoint.endpoint || this.abort.signal.aborted) return false;
    const epoch = this.epoch;
    if (terms.devices.some(item => this.blockedDevices.has(item.deviceRef.deviceId) || !this.devices[item.deviceRef.deviceId] || !hardwareDeviceMatches(item.deviceRef, this.devices[item.deviceRef.deviceId]!))) return false;
    if (this.options.acceptClaim && !(await this.options.acceptClaim(terms))) { this.reservations.delete(contract.key); return false; }
    if (this.abort.signal.aborted || epoch !== this.epoch) return false;
    // Read current agreements, not advertisement counters. In-flight admissions
    // reserve capacity too, so concurrent token attachments cannot over-admit.
    const counted = new Map<string, HardwareClaimTerms>();
    const contracts = await this.options.concord.contracts(HARDWARE_CLAIM_PROFILE, { participant: terms.managerEndpoint, state: "open" });
    for (const other of contracts) {
      if (other.key === contract.key) continue;
      const validity = await this.options.concord.validate(other);
      if (validity.status === ContractValidityStatus.UNAVAILABLE) return false;
      if (validity.valid || (validity.status === ContractValidityStatus.NOT_YET_FULFILLED && validity.tokens[terms.managerEndpoint])) {
        try { const existing = validateHardwareClaimTerms(validity.contract?.terms); if (hardwareClaimMatches(validity.contract!, existing)) counted.set(other.key, existing); } catch { /* Invalid profile terms confer no device scope. */ }
      }
    }
    const openKeys = new Set(contracts.map(item => item.key));
    for (const [key, reservation] of this.reservations) {
      if (!openKeys.has(key)) { this.reservations.delete(key); this.reservedContracts.delete(key); }
      else if (key !== contract.key) counted.set(key, reservation);
    }
    if (this.abort.signal.aborted || epoch !== this.epoch) return false;
    for (const item of terms.devices) {
      const total = this.devices[item.deviceRef.deviceId]?.capacity?.totalInstances;
      const claimed = [...counted.values()].reduce((sum, claim) => sum + (claim.devices.find(device => device.deviceRef.deviceId === item.deviceRef.deviceId)?.instanceCount ?? 0), 0);
      if (total != null && claimed + item.instanceCount > total) { this.reservations.delete(contract.key); return false; }
    }
    this.reservations.set(contract.key, terms); this.reservedContracts.set(contract.key, contract);
    return true;
  }

  /** Disconnect or material change revokes contexts before any new advertisement. */
  setDevices(devices: Record<string, HardwareAdvertisementDevice>): Promise<void> {
    this.assertOpen();
    const next = this.inventory(devices);
    const changed = new Set(Object.keys(this.devices).filter(id => !next[id] || canonicalJson(this.devices[id]!.descriptor as unknown as JsonObject) !== canonicalJson(next[id]!.descriptor as unknown as JsonObject) || this.devices[id]!.capacity?.totalInstances !== next[id]!.capacity?.totalInstances));
    this.epoch++;
    for (const id of changed) { this.blockedDevices.add(id); this.deviceVersions.set(id, (this.deviceVersions.get(id) ?? 0) + 1); }
    const closing: Promise<void>[] = [];
    const affected: ContractHandle[] = [];
    for (const [key, terms] of this.reservations) {
      if (terms.devices.some(item => changed.has(item.deviceRef.deviceId))) {
        closing.push(this.manager.release(key)); affected.push(this.reservedContracts.get(key)!);
      }
    }
    this.updateTask = this.updateTask.catch(() => {}).then(async () => {
      await Promise.allSettled(closing); this.assertOpen();
      // Local revocation is immediate, but publishing a changed candidate also
      // requires exact evidence that the previous claimed scope lost authority.
      await bounded((async () => {
        // Pending proposals can also have captured the old descriptor. Retire
        // them while admission for the changed devices is fenced off.
        for (const contract of await this.options.concord.contracts(HARDWARE_CLAIM_PROFILE, { participant: this.options.endpoint.endpoint, state: "open" })) {
          const record = await this.options.concord.contractRecord(contract);
          if (!record || (record.participantSessions[this.options.endpoint.endpoint] && record.participantSessions[this.options.endpoint.endpoint] !== this.options.endpoint.sessionId)) continue;
          let terms: HardwareClaimTerms;
          try { terms = validateHardwareClaimTerms(record.terms); } catch { continue; }
          if (!terms.devices.some(item => changed.has(item.deviceRef.deviceId)) || !hardwareClaimMatches(record, terms)) continue;
          if (!affected.some(item => item.key === contract.key)) affected.push(contract);
          void this.manager.release(contract).catch(error => this.report(error));
        }
        await Promise.all(affected.map(async contract => {
          const current = await this.options.concord.validateExact(contract);
          if (current.valid || current.status === ContractValidityStatus.NOT_YET_FULFILLED) {
            await this.options.concord.cancelContract(contract, this.options.endpoint.endpoint, { reason: "hardware_device_changed" });
          }
          const confirmed = await this.options.concord.validateExact(contract);
          if (confirmed.valid || confirmed.status === ContractValidityStatus.NOT_YET_FULFILLED || confirmed.status === ContractValidityStatus.UNAVAILABLE) throw new StateUnavailable("Hardware device-change cleanup is incomplete");
          this.reservations.delete(contract.key); this.reservedContracts.delete(contract.key);
        }));
      })(), 5000, this.options.concord.scheduler);
      this.assertOpen();
      this.devices = next;
      for (const id of changed) this.blockedDevices.delete(id);
      if (this.ready) {
        await this.advertisement?.close(); this.advertisement = undefined;
        await this.publishInventory();
      }
      this.manager.wake();
    });
    return this.updateTask;
  }

  async authorize(message: DeckrMessage): Promise<HardwareOperationContext> {
    if (!this.ready) throw new StateUnavailable("Hardware manager is not ready");
    const validated = validateLaneMessage(message);
    if (!messageIsDeliverableTo(validated, this.options.endpoint.endpoint, { endpointSessionId: this.options.endpoint.sessionId, now: new Date(this.options.concord.scheduler.now()) }) || validated.lane !== HARDWARE_LANE || !validated.contract || !["controlCommand", "capabilityStateRequest"].includes(validated.messageType)) throw new ValidationError("Not an authorized hardware request");
    const contract = await this.options.concord.getContract(validated.contract);
    if (!contract) throw new StateUnavailable("Unknown hardware claim");
    const managed = await this.manager.reconcileOne(contract);
    if (!managed) throw new StateUnavailable("No locally owned hardware claim");
    const terms = validateHardwareClaimTerms(managed.record.terms);
    if (!hardwareClaimMatches(managed.record, terms) || terms.controllerEndpoint !== validated.sender) throw new ValidationError("Hardware claim sender mismatch");
    const authority = this.manager.authority(contract);
    const validity = await authority.check();
    if (validity.tokens[terms.controllerEndpoint]?.sessionId !== validated.senderSessionId) throw new ValidationError("Hardware sender session mismatch");
    const { device, capability } = validateScopedHardwareBody(terms, this.devices, validated.messageType, validated.body, this.options.schemas);
    const version = this.deviceVersions.get(device.deviceRef.deviceId) ?? 0;
    const context = { signal: authority.signal, contract: { contractId: contract.contractId, generation: contract.generation },
      controllerEndpoint: terms.controllerEndpoint, controllerSessionId: validated.senderSessionId, device, capability,
      assertActive: async () => {
        if (!this.ready || this.blockedDevices.has(device.deviceRef.deviceId) || authority.signal.aborted || version !== (this.deviceVersions.get(device.deviceRef.deviceId) ?? 0)) throw new StateUnavailable("Hardware context is no longer active");
        await authority.check();
        if (!this.ready || authority.signal.aborted || version !== (this.deviceVersions.get(device.deviceRef.deviceId) ?? 0)) throw new StateUnavailable("Hardware context is no longer active");
      },
    };
    await context.assertActive();
    return Object.freeze(context);
  }
  async dispatch(delivery: MessageDelivery): Promise<void> {
    const context = await this.authorize(delivery.message);
    await context.assertActive();
    const reply = await this.options.handle?.(delivery.message, context);
    if (reply) {
      const contract = await this.options.concord.getContract(context.contract);
      if (!contract) throw new StateUnavailable("Hardware claim disappeared before reply");
      validateScopedHardwareBody(validateHardwareClaimTerms((await this.options.concord.contractRecord(contract))?.terms), this.devices, reply.messageType, reply.body, this.options.schemas);
      if (!replyMatchesRequest(delivery.message, reply.messageType, reply.body)) throw new ValidationError("Hardware reply does not match request");
      await context.assertActive(); await delivery.reply(reply.body, { messageType: reply.messageType });
    }
  }
  async send(contract: ContractHandle, messageType: HardwareMessageType, body: JsonObject): Promise<void> {
    if (!this.ready) throw new StateUnavailable("Hardware manager is not ready");
    const managed = await this.manager.reconcileOne(contract);
    if (!managed) throw new StateUnavailable("No managed hardware claim");
    const terms = validateHardwareClaimTerms(managed.record.terms);
    if (["controlCommand", "capabilityStateRequest"].includes(messageType)) throw new ValidationError("A hardware manager cannot send controller operations");
    validateScopedHardwareBody(terms, this.devices, messageType, body, this.options.schemas);
    const authority = this.manager.authority(contract), current = await authority.check();
    if (!this.ready || authority.signal.aborted) throw new StateUnavailable("Hardware authority terminated");
    await this.options.endpoint.publish({ recipient: terms.controllerEndpoint, recipientSessionId: current.tokens[terms.controllerEndpoint]!.sessionId,
      contract: { contractId: contract.contractId, generation: contract.generation }, subject: hardwareCapabilitySubject(body), messageType, body });
  }
  /** Route device input through locally owned claims, with authority checked at send time. */
  async publishInput(body: JsonObject): Promise<number> {
    const ref = validateHardwareDeviceRef(body.deviceRef);
    await this.manager.reconcile();
    let delivered = 0;
    for (const managed of this.manager.managedContracts()) {
      const terms = validateHardwareClaimTerms(managed.record.terms);
      if (!terms.devices.some(device => device.deviceRef.deviceId === ref.deviceId)) continue;
      await this.send(managed.contract, "controlInput", body);
      delivered++;
    }
    return delivered;
  }
  private async listen(): Promise<void> {
    try {
      for await (const delivery of this.options.endpoint.watch({ signal: this.abort.signal })) {
        if (this.operations.size >= 256) { this.report(new StateUnavailable("Hardware handler limit reached")); continue; }
        const task = this.dispatch(delivery).catch(error => this.report(error));
        this.operations.add(task); void task.finally(() => this.operations.delete(task));
      }
    } catch (error) { if (!this.abort.signal.aborted) this.report(error); }
  }
  private assertOpen(): void { if (this.abort.signal.aborted) throw new StateUnavailable("Hardware manager is closed"); }
  private report(error: unknown): void { try { this.options.onError?.(error); } catch { /* Diagnostics are isolated. */ } }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.abort.abort(); this.states.set({ status: "closed" }); this.states.close();
    this.closing = bounded(Promise.allSettled([this.manager.aclose(), this.advertisement?.close(), this.listening, this.updateTask, ...this.operations]), 5000, this.options.concord.scheduler).then(() => {}, () => {});
    return this.closing;
  }
}

export interface ManagedHardwareClientOptions {
  endpoint: EndpointLane; beacon: BeaconService; concord: ConcordService;
  managerId: string; devices: { deviceId: string; instanceCount: number; fingerprint?: string }[];
  negotiationTimeoutMs?: number; schemas?: PayloadSchemas;
}
/** One managed claim. Discovery loss never revokes an established agreement. */
export class ManagedHardwareClient {
  private readonly options: ManagedHardwareClientOptions;
  private readonly abort = new AbortController();
  private readonly states = new StateCell<HardwareState>({ status: "starting" });
  private candidate?: HardwareDescriptor;
  private agreement?: ConcordAgreement;
  private readonly owned = new Set<ConcordAgreement>();
  private starting?: Promise<void>;
  private discovery?: Promise<void>;
  private negotiation?: Promise<void>;
  private closing?: Promise<void>;
  private cancelRetry?: () => void;
  private unwatch?: () => void;
  constructor(options: ManagedHardwareClientOptions) { assertHardwareEndpoint(options.endpoint, "controller"); this.options = options; }
  get state() { return this.states.current; }
  get claim(): ConcordAgreement | undefined { return this.agreement?.valid ? this.agreement : undefined; }
  watch(options: { signal?: AbortSignal } = {}) { return this.states.watch(options); }
  waitFor(predicate: (state: HardwareState) => boolean, options: { signal?: AbortSignal; timeoutMs?: number } = {}) { return this.states.wait(predicate, { ...options, scheduler: this.options.concord.scheduler }); }
  start(): Promise<void> {
    if (this.abort.signal.aborted) return Promise.reject(new StateUnavailable("Hardware client is closed"));
    if (!this.starting) this.starting = (async () => {
      await abortable(Promise.all([this.options.beacon.start(), this.options.concord.start()]), this.abort.signal);
      if (!this.abort.signal.aborted) this.discovery = this.discover();
    })().catch(error => {
      if (!this.abort.signal.aborted) this.states.set({ status: "failed", failure: { code: "hardware_start_failed", message: String(error) } });
      throw error;
    });
    return this.starting;
  }
  private async discover(): Promise<void> {
    try {
      const candidates = new Map<string, HardwareDescriptor>();
      for await (const item of this.options.beacon.watchFeature(HARDWARE_FEATURE_ID, { signal: this.abort.signal })) {
        if (item.type === "snapshot") {
          candidates.clear();
          for (const candidate of item.candidates) { const descriptor = parseHardwareDescriptor(candidate, this.options.schemas); if (descriptor?.managerId === this.options.managerId) candidates.set(candidate.key, descriptor); }
        } else if (item.candidate) {
          const descriptor = parseHardwareDescriptor(item.candidate, this.options.schemas);
          if (descriptor?.managerId === this.options.managerId) candidates.set(item.key, descriptor); else candidates.delete(item.key);
        } else candidates.delete(item.key);
        this.candidate = [...candidates.values()].sort((a, b) => b.candidate.revision - a.candidate.revision).find(candidate => this.options.devices.every(item => candidate.devices[item.deviceId] && (!item.fingerprint || candidate.devices[item.deviceId]!.descriptor.fingerprint === item.fingerprint)));
        this.negotiate();
      }
    } catch (error) { if (!this.abort.signal.aborted && !this.claim) this.states.set({ status: "failed", failure: { code: "hardware_discovery_failed", message: String(error) } }); }
  }
  private negotiate(): void {
    if (this.negotiation || this.claim || !this.candidate || this.abort.signal.aborted) return;
    const descriptor = this.candidate;
    this.negotiation = this.negotiateCandidate(descriptor).catch(error => {
      if (!this.abort.signal.aborted) this.states.set({ status: "reconnecting", failure: { code: "hardware_negotiation_failed", message: String(error) } });
    }).finally(() => {
      this.negotiation = undefined;
      if (!this.claim && !this.abort.signal.aborted && !this.cancelRetry) this.cancelRetry = this.options.concord.scheduler.schedule(1000, () => { this.cancelRetry = undefined; this.negotiate(); });
    });
  }
  private async negotiateCandidate(descriptor: HardwareDescriptor): Promise<void> {
    const terms = validateHardwareClaimTerms({ profile: HARDWARE_CLAIM_PROFILE, claimId: randomUUID(), controllerEndpoint: this.options.endpoint.endpoint,
      managerEndpoint: descriptor.managerEndpoint, devices: this.options.devices.map(item => ({ instanceCount: item.instanceCount,
        deviceRef: { managerId: descriptor.managerId, deviceId: item.deviceId, fingerprint: descriptor.devices[item.deviceId]!.descriptor.fingerprint } })) });
    const attempt = new AbortController();
    const signal = AbortSignal.any([attempt.signal, this.abort.signal]);
    let agreement: ConcordAgreement | undefined;
    try {
      await bounded((async () => {
        agreement = await this.options.concord.ensureAgreement({ profile: HARDWARE_CLAIM_PROFILE, participants: [terms.controllerEndpoint, terms.managerEndpoint],
          localParticipant: terms.controllerEndpoint, localSessionId: this.options.endpoint.sessionId, terms: terms as unknown as JsonObject,
          participantSessions: { [terms.controllerEndpoint]: this.options.endpoint.sessionId, [terms.managerEndpoint]: descriptor.sessionId },
          currentSessions: { [terms.controllerEndpoint]: this.options.endpoint.sessionId, [terms.managerEndpoint]: descriptor.sessionId } },
        { signal, onCreated: created => { agreement = created; this.owned.add(created); created.signal.addEventListener("abort", () => this.owned.delete(created), { once: true }); } });
        for await (const state of agreement.watch({ signal })) {
          if (state.closed) break;
          if (agreement.valid) return;
        }
        throw new StateUnavailable("Hardware claim ended before readiness");
      })(), this.options.negotiationTimeoutMs ?? 2000, this.options.concord.scheduler);
      if (!agreement?.valid || this.abort.signal.aborted) throw new StateUnavailable("Hardware claim is unavailable");
      this.agreement = agreement;
      this.claimDescriptors.set(agreement, descriptor);
      this.unwatch?.();
      this.unwatch = agreement.onChange(() => {
        if (agreement!.closed && !this.abort.signal.aborted) {
          this.agreement = undefined; this.states.set({ status: "reconnecting" }); this.negotiate();
        }
      });
      this.states.set({ status: "ready", contract: pointer(agreement.contract) });
    } catch (error) { attempt.abort(); await agreement?.close(); throw error; }
  }
  async ready(timeoutMs = 10_000): Promise<ConcordAgreement> {
    const abort = new AbortController(), signal = AbortSignal.any([abort.signal, this.abort.signal]);
    try {
      await bounded((async () => { await abortable(this.start(), signal); await this.waitFor(state => ["ready", "failed", "closed"].includes(state.status), { signal }); })(), timeoutMs, this.options.concord.scheduler);
      if (!this.claim) throw new StateUnavailable(this.state.failure?.message ?? "Hardware claim lost authority");
      return this.claim;
    } finally { abort.abort(); }
  }
  private async active(): Promise<{ agreement: ConcordAgreement; terms: HardwareClaimTerms; devices: Record<string, HardwareAdvertisementDevice> }> {
    const agreement = await this.ready();
    const validity = await agreement.refresh();
    if (!validity.valid || agreement.closed) throw new StateUnavailable("Hardware claim lost authority");
    const terms = validateHardwareClaimTerms(agreement.spec.terms);
    // The descriptor is captured with the claim; later discovery is never authority.
    const descriptor = this.claimDescriptors.get(agreement);
    if (!descriptor) throw new StateUnavailable("No claimed hardware descriptor");
    return { agreement, terms, devices: descriptor.devices };
  }
  private readonly claimDescriptors = new WeakMap<ConcordAgreement, HardwareDescriptor>();
  async send(messageType: "controlCommand" | "capabilityStateRequest", body: JsonObject): Promise<void> {
    const { agreement, terms, devices } = await this.active();
    validateScopedHardwareBody(terms, devices, messageType, body, this.options.schemas);
    if (!agreement.valid) throw new StateUnavailable("Hardware claim lost authority");
    await this.options.endpoint.publish({ recipient: terms.managerEndpoint, recipientSessionId: agreement.validity.tokens[terms.managerEndpoint]!.sessionId,
      contract: pointer(agreement.contract), subject: hardwareCapabilitySubject(body), messageType, body });
  }
  async request(messageType: "controlCommand" | "capabilityStateRequest", body: JsonObject, options: { timeoutMs?: number; signal?: AbortSignal } = {}): Promise<DeckrMessage> {
    const { agreement, terms, devices } = await this.active();
    validateScopedHardwareBody(terms, devices, messageType, body, this.options.schemas);
    const reply = await this.options.endpoint.request({ recipient: terms.managerEndpoint, recipientSessionId: agreement.validity.tokens[terms.managerEndpoint]!.sessionId,
      contract: pointer(agreement.contract), subject: hardwareCapabilitySubject(body), messageType, body,
      timeout: options.timeoutMs, signal: AbortSignal.any([agreement.signal, ...(options.signal ? [options.signal] : [])]), accept: reply => {
        try { validateScopedHardwareBody(terms, devices, reply.messageType, reply.body, this.options.schemas); return replyMatchesRequest({ messageType, body }, reply.messageType, reply.body); } catch { return false; }
      } });
    if (!agreement.valid) throw new StateUnavailable("Hardware claim lost authority");
    return reply;
  }
  async *messages(options: { signal?: AbortSignal } = {}): AsyncIterable<DeckrMessage> {
    const signal = AbortSignal.any([this.abort.signal, ...(options.signal ? [options.signal] : [])]);
    for await (const delivery of this.options.endpoint.watch({ signal })) {
      const agreement = this.claim, message = delivery.message;
      if (!agreement || message.contract?.contractId !== agreement.contract.contractId || message.contract.generation !== agreement.contract.generation) continue;
      try {
        const terms = validateHardwareClaimTerms(agreement.spec.terms), descriptor = this.claimDescriptors.get(agreement)!;
        if (message.sender !== terms.managerEndpoint || message.senderSessionId !== agreement.validity.tokens[terms.managerEndpoint]?.sessionId || ["controlCommand", "capabilityStateRequest"].includes(message.messageType)) continue;
        validateScopedHardwareBody(terms, descriptor.devices, message.messageType, message.body, this.options.schemas);
        if (agreement.valid) yield message;
      } catch { /* Inapplicable and malformed deliveries never reach the host. */ }
    }
  }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    const owned = [...this.owned];
    this.abort.abort(); this.cancelRetry?.(); this.unwatch?.();
    const closing = owned.map(agreement => agreement.close());
    this.states.set({ status: "closed" }); this.states.close();
    this.closing = bounded(Promise.allSettled([...closing, this.discovery, this.negotiation]), 5000, this.options.concord.scheduler).then(() => {}, () => {});
    return this.closing;
  }
}

function assertHardwareEndpoint(endpoint: EndpointLane, family: string): void {
  if (endpoint.name !== HARDWARE_LANE || parseEndpointAddress(endpoint.endpoint).family !== family) throw new ValidationError(`Expected ${family} hardware lane`);
}
function pointer(contract: ContractPointer): ContractPointer { return { contractId: contract.contractId, generation: contract.generation }; }

export function validateScopedHardwareBody(terms: HardwareClaimTerms, devices: Record<string, HardwareAdvertisementDevice>, messageType: string, value: unknown, schemas = defaultPayloadSchemas): { device: HardwareAdvertisementDevice; capability: HardwareCapability } {
  const body = validateHardwareMessageBody(messageType, value), ref = validateHardwareDeviceRef(body.deviceRef);
  const claimed = terms.devices.find(item => item.deviceRef.deviceId === ref.deviceId);
  const device = devices[ref.deviceId];
  if (!claimed || !device || !hardwareDeviceMatches(ref, device) || !hardwareDeviceMatches(claimed.deviceRef, device)) throw new ValidationError("Device lies outside the hardware claim");
  const capability = findHardwareCapability(device.descriptor, { capabilityId: String(body.capabilityId), controlId: body.controlId as string | undefined });
  if (messageType === "controlInput") validateHardwareCapabilityPayload(capability, "event", String(body.eventType), body.value ?? null, schemas);
  if (messageType === "controlCommand") validateHardwareCapabilityPayload(capability, "command", String(body.commandType), body.params ?? {}, schemas);
  if (messageType.startsWith("command") && !capability.commandTypes?.includes(String(body.commandType))) throw new ValidationError("Reply names an unsupported command");
  if (messageType.startsWith("capabilityState")) {
    if (capability.direction !== "state" || (messageType === "capabilityStateRequest" && !capability.access.some(access => ["requestable", "readable"].includes(access))) || (messageType === "capabilityStateChanged" && !capability.access.includes("emits"))) throw new ValidationError("Unsupported state operation");
    if (messageType !== "capabilityStateRequest" && (body.status === undefined || body.status === "ok")) validateHardwareCapabilityPayload(capability, "state", body.stateType as string | undefined, body.value ?? null, schemas);
  }
  return { device, capability };
}

function replyMatchesRequest(request: { messageType: string; body: JsonObject }, messageType: string, body: JsonObject): boolean {
  const allowed = request.messageType === "controlCommand" ? ["commandAccepted", "commandRejected", "commandReply"] : ["capabilityStateReply"];
  return allowed.includes(messageType) && canonicalJson(request.body.deviceRef!) === canonicalJson(body.deviceRef!) &&
    (request.body.controlId ?? null) === (body.controlId ?? null) && request.body.capabilityId === body.capabilityId &&
    (request.messageType === "controlCommand" ? request.body.commandType === body.commandType : (request.body.stateType ?? null) === (body.stateType ?? null));
}

/** Canonical hardware capability subject, shared with the Python and Rust managers. */
export function hardwareCapabilitySubject(body: JsonObject) {
  const ref = validateHardwareDeviceRef(body.deviceRef);
  const identifiers: Record<string, string> = { managerId: ref.managerId, deviceId: ref.deviceId, capabilityId: String(body.capabilityId) };
  if (body.controlId != null) identifiers.controlId = String(body.controlId);
  return entitySubject("hardware_capability", identifiers);
}
