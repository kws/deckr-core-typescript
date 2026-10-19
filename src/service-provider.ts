import type { BeaconService } from "./beacon.ts";
import type { ConcordService, ContractHandle } from "./concord.ts";
import { ServiceUnavailable } from "./errors.ts";
import type { JsonObject } from "./json.ts";
import { ServiceAdvertiser, ServiceUseAuthorizer, type RegisteredEndpointLane, type ServiceProtocol, type ServiceUseTerms, type ServiceViewWriteContext } from "./services.ts";

/** Provider bootstrap, domain consent, advertisement, and participation lifetime. */
export class ManagedServiceProvider {
  private readonly advertiser: ServiceAdvertiser;
  private readonly authorizer: ServiceUseAuthorizer;
  private closed = false;
  private starting?: Promise<void>;
  private readyValue = false;
  constructor(options: {
    protocol: ServiceProtocol; serviceId: string; endpoint: RegisteredEndpointLane;
    beacon: BeaconService; concord: ConcordService;
    acceptContract?: (terms: ServiceUseTerms) => boolean | Promise<boolean>;
  }) {
    this.advertiser = new ServiceAdvertiser(options);
    this.authorizer = new ServiceUseAuthorizer({ ...options,
      acceptContract: async terms => !this.closed && (options.acceptContract ? await options.acceptContract(terms) : true),
    });
  }
  get ready(): boolean { return this.readyValue && !this.closed; }
  start(): Promise<void> {
    if (this.closed) return Promise.reject(new ServiceUnavailable("provider_closed", "Service provider is closed"));
    if (!this.starting) this.starting = (async () => {
      await this.authorizer.start();
      if (this.closed) return;
      await this.advertiser.publish("available");
      if (this.closed) { await this.advertiser.withdraw(); return; }
      this.readyValue = true;
    })().catch(async error => { await this.close(); throw error; });
    return this.starting;
  }
  /** Reevaluate domain consent; rejected participants terminate through Concord. */
  async reconcile(): Promise<void> { await this.authorizer.reconcileContracts(); }
  async authorizeMessage(...args: Parameters<ServiceUseAuthorizer["authorizeMessage"]>) {
    if (!this.ready) return "denied" as const;
    return this.authorizer.authorizeMessage(...args);
  }
  async viewContext(contract: ContractHandle): Promise<ServiceViewWriteContext> {
    if (!this.ready) throw new ServiceUnavailable("provider_unavailable", "Service provider is not ready");
    return this.authorizer.viewContext(contract);
  }
  async send(contract: ContractHandle, name: string, payload: JsonObject): Promise<void> {
    if (!this.ready) throw new ServiceUnavailable("provider_unavailable", "Service provider is not ready");
    await this.authorizer.send(contract, name, payload);
  }
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true; this.readyValue = false;
    // Starting both immediately revokes participation before remote withdrawal.
    await Promise.allSettled([this.authorizer.close(), this.advertiser.withdraw()]);
  }
}
