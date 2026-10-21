# @deckr/core

The TypeScript implementation of Deckr's shared core, built as an ESM package
with TypeScript declarations. This repository contains reusable wire models,
validation, discovery, agreement lifecycle, messaging, hardware, and service helpers.

The authoritative specifications, schemas, catalogs, fixtures, and conformance
scenarios live in **[kws/deckr](https://github.com/kws/deckr)**, normally checked
out beside this repository at `../deckr`. Protocol changes belong there first;
this repository implements and tests those contracts. It does not author or
vendor the specification bundle.

## Place in the project

The [project repository map](https://github.com/kws/deckr#project-repositories)
describes the reference, Python/Rust/TypeScript cores, and integration harness.
This repository owns the TypeScript API and its implementation tests.
Applications own controller routing, rendering, configuration policy, device
drivers, and action or integration behavior.

The cores use shared contracts with language-specific APIs and independent
package versions and coverage.
[deckr-integration-tests](https://github.com/kws/deckr-integration-tests)
owns the real-broker cross-language workloads and their recorded evidence.
Use the [schema guide](https://github.com/kws/deckr/blob/main/docs/schema-guide.md)
for reference artifacts and the
[architecture proposal](https://github.com/kws/deckr/blob/main/docs/deckr-architecture.md)
for informative component boundaries.

## Action Runtime contract support

`@deckr/core/actions` implements explicit `PageCommandTarget` values and required
page-session generations from the alpha contract at the Git revision in
[.deckr-spec-revision](.deckr-spec-revision). Open commands can target a current
binding or page session; replace and close commands require a page-session target.
All messages require the runtime service subject.

`matchCapabilityRequirements` and `validateBindingResources` accept a
`CapabilityMatchingScope` containing the negotiated descriptor, device reference,
control, and optional configured capability IDs and payload-schema registry.
They apply preference order, required/optional matching, views, provenance,
and source checks. No requirements means no matches. Every result names its
requirement. The convenience matcher chooses the first descriptor-order
equivalent candidate; the validator accepts other equally eligible choices.

`validatePageCommand` checks a `PageCommandContext` of current observations.
Managed Action Runtime reference resolvers must supply `pageCommand` for page
operations. Applications own serialization and must recheck the observations
when committing the transition. The helper neither changes pages nor acquires
authority.

Hardware discovery descriptors already expose immutable manager labels, and
managed hardware message streams deliver authorized command rejections with
their correlation. `ServiceClientState.contract` remains an observation;
managed operations check authority when used.

The tests execute every upstream Action Runtime semantic vector with both
ordinary and lossless JSON parsing. This verifies the covered pure checks;
the integration harness records broker workload evidence separately.

Rendering uses `binding_render` commands and correlated `binding_render_result`
events. `BindingAttachedBody.rendering` advertises named targets, formats, canvas
sizes, and limits. The public rendering types and `validateRenderCommand` helper
check current binding ownership, generation, hardware authority, named matches,
format support, envelope bytes, and Unicode code-point limits. Supply current
`renderCommand` observations in managed reference resolvers and recheck them at
final controller dispatch. `validateRenderingSupport` checks capability aliases.

Managed Action Runtime sends return a receipt containing the sent message ID.
Result sends set `causationId` from `requestMessageId`; results remain deliverable
under the current agreement when their target has detached. `messageWireSizeBytes`
from `@deckr/core/lanes` retains the received UTF-8 envelope size through transport
validation. Image decoding, rendering, worker limits, and output ordering remain
controller responsibilities. The tests consume the upstream rendering fixtures
and admission vectors and exercise managed command/result delivery.

## Requirements

- Node.js 22.18 or later, including native TypeScript type stripping for tests
- npm
- A checkout of `kws/deckr` for conformance tests

## Development

Run all commands from the repository root:

```bash
npm ci
npm run check
```

`npm ci` installs the locked development dependencies and builds the package.
`npm run check` runs type checking, all tests, and the build. The individual
commands are:

```bash
npm run typecheck
npm test
npm run build
npm pack --dry-run
```

Tests use Node's test runner. The default lifecycle and NATS adapter tests use
in-memory stores and fakes; no running broker is required. The optional
`tests/nats-live.test.ts` skips unless `DECKR_NATS_TEST_URL` is set. To run it
against a disposable JetStream broker that supports Deckr's bucket policies:

```bash
DECKR_NATS_TEST_URL=nats://127.0.0.1:4222 node --test tests/nats-live.test.ts
```

It checks numeric terms, consumer cleanup, renewal, and invalid-record isolation.
Neither the default tests nor this single-language test establish cross-language
coverage or every scenario in the upstream specification.

### Performance diagnostics

Run the opt-in workload before and after a runtime change:

```bash
node --expose-gc tests/performance.ts > performance.json
DECKR_PERF_ITERATIONS=10000 node --expose-gc tests/performance.ts > memory.json
DECKR_NATS_TEST_URL=nats://127.0.0.1:4222 node --expose-gc tests/nats-performance.ts > nats-performance.json
```

The first records read latency, observation opens/closes, repeated provider writes,
request/reply subscription cleanup, source update costs, RSS, heap, external
memory, GC activity, and event-loop delay.
GC samples use a fixed live workload; `arrayBuffers` is part of `external`, not an
additional memory total. Forced GC is for this diagnostic only. Compare matching
phases, since a failing baseline can complete fewer writes. These short local
measurements do not establish a long-running application's memory ceiling.

The broker diagnostic creates and deletes its own uniquely named bucket. It
records snapshot and update latency, API requests, timer allocation, consumer
cleanup, and whether the initial replay payload can be collected. Weak-reference
collection is diagnostic evidence, not a deterministic unit-test assertion.
Use the integration repository for cross-language recovery and soak evidence.

Managed client reads retain a shared observation until their accepted agreement
ends or the client closes. Supply stable `StateStore` objects from `stateFor` to
share observations. Provider operation writers retain their own authorization
guards and share agreement cleanup; closing one withdraws only its retained
revisions. Normal request handlers do not need to keep a writer registry.

## Specification dependency

Conformance tests read fixtures and vectors directly from
`../deckr/contract/v1`. To use a checkout elsewhere, set `DECKR_SPEC_ROOT` to the
root of that repository:

```bash
DECKR_SPEC_ROOT=/path/to/deckr npm test
```

An absent specification checkout is an error; conformance checks are never
silently skipped. Local tests use the selected checkout as it stands, so changes
to the specification can be tested before updating the pin.

CI checks out the immutable `kws/deckr` revision recorded in
[.deckr-spec-revision](.deckr-spec-revision). To reproduce it without changing an
existing sibling checkout:

```bash
git clone https://github.com/kws/deckr.git .deckr-spec
git -C .deckr-spec checkout --detach "$(cat .deckr-spec-revision)"
DECKR_SPEC_ROOT="$PWD/.deckr-spec" npm run check
```

Update `.deckr-spec-revision` alongside implementation changes after reviewing
the upstream contract changes and running these checks. Ensure the pinned commit
has been pushed to `kws/deckr` before running CI. If `kws/deckr` is
private, configure the `DECKR_SPEC_TOKEN` Actions secret with read access to that
repository; otherwise CI uses its default GitHub token.

When validating a coordinated contract or runtime change, update the source commits in
`deckr-integration-tests/sources.lock.json` and follow its
[source-pin workflow](https://github.com/kws/deckr-integration-tests#source-pins-and-coordinated-changes).
All three core specification pins must agree with the harness's reference pin.

## Package layout and usage

```text
src/                    TypeScript implementation
tests/                  Implementation tests and external conformance checks
dist/                   Generated JavaScript and declarations (ignored)
package.json            Package metadata, exports, version, and npm scripts
.deckr-spec-revision     Specification commit used by CI
.github/workflows/      TypeScript validation
```

The package retains its `@deckr/core` name and public entry points:

| Import | Purpose |
| --- | --- |
| `@deckr/core` | Shared core exports |
| `@deckr/core/actions` | Managed Action Runtime clients/providers, contract validation, metadata, and settings targets |
| `@deckr/core/beacon` | Discovery records and lifecycle helpers |
| `@deckr/core/concord` | Agreement records, participant tokens, and lifecycle helpers |
| `@deckr/core/endpoint` | Endpoint addresses and message targets |
| `@deckr/core/hardware` | Descriptors, claim negotiation, and managed hardware boundaries |
| `@deckr/core/json` | JSON validation and canonicalization |
| `@deckr/core/keys` | Storage key token encoding |
| `@deckr/core/lanes` | Message envelopes, routing, and header validation |
| `@deckr/core/messaging` | Endpoint sessions, shared lane subscriptions, requests, and in-process delivery |
| `@deckr/core/nats` | NATS connection, message transport, and state-store adapters |
| `@deckr/core/services` | Service discovery, authorization, and protected views |
| `@deckr/core/state` | State-store interfaces and in-memory implementation |
| `@deckr/core/testing` | Test harnesses |

For example, after installing a built package:

```typescript
import { controllerAddress, endpointTarget } from "@deckr/core/endpoint";

const target = endpointTarget(controllerAddress("main"));
```

The `nats` peer dependency is optional. Install it in applications that use
`connectNats`; importing the core and using pure contract or lifecycle helpers
does not load a NATS client.

`connectNats({ servers, signal, reconnectMaxIntervalSeconds: 60, timeout: 5000 })`
retries initial connection indefinitely with bounded dials and a fixed wait.
After a connection succeeds, reconnect waits use exponential windows with jitter
between half and the full window, capped at the configured maximum. Abort the
signal to cancel startup or close the owned connection. The retry adapter is
tested against the supported `nats` 2.x dial loop; it uses a narrow internal seam
because the public delay callback runs for both waits and connection attempts.

Wire JSON is decoded with `parseJson`. Integral floats such as `1.0`, negative
floating zero, and large integers use immutable `JsonNumber` values so cloning,
hashing, and JSON serialization retain their meaning. Use `Number(value)` when a
domain calculation intentionally needs binary64. Payload-schema validation uses
a separate numeric projection without changing the payload; integers that AJV
cannot represent exactly are rejected instead of silently rounded. Concord terms
hashing and transport preserve arbitrary-size integers independently of AJV.

## Managed runtime lifecycle

The implementation uses the boundaries in the upstream
[architecture proposal](https://github.com/kws/deckr/blob/main/docs/deckr-architecture.md),
which is informative.
The linked protocol specifications define required behavior. The host owns the
transport and shared stores. Applications supply service behavior and domain
consent; the managed runtimes own observation, negotiation, renewal, and cleanup.

| Boundary | Implementation |
| --- | --- |
| Ordered store observations | `MaterializedStateSource` and `sourceFor`, exported from `@deckr/core` |
| Endpoint delivery | `EndpointSession` with `MemoryMessageTransport` or `NatsMessageTransport` |
| Discovery and advertisements | `BeaconService` and `BeaconAdvertisement` |
| Agreement authority and participant tokens | `ConcordService`, `ConcordAgreement`, and participant managers |
| Optional maintenance | `ConcordReaperService` with separate exact store capabilities |
| Hardware discovery and claims | `ManagedHardwareClient`, exported from `@deckr/core/hardware` |
| Hardware admission and driver dispatch | `ManagedHardwareManager`, exported from `@deckr/core/hardware` |
| Service selection and recovery | `ManagedServiceClient`, exported from `@deckr/core/services` |
| Service consent and participation | `ManagedServiceProvider`, exported from `@deckr/core/services` |
| Protected view reads and writes | `ManagedServiceViewAccess` with a live lease capability |
| Action Runtime protocol boundary | `ManagedActionRuntimeClient` and `ManagedActionRuntimeProvider` |

Start shared Beacon and Concord runtimes before using their managed components,
or let the components start them on first use. `ManagedServiceProvider.start()`
checks prior participation, starts the backend and participation, then advertises
availability. Its `acceptContract` callback supplies domain consent; call
`reconcile()` when that policy changes. Set `exclusive: true` only for a profile
that requires one provider lifetime. While ownership is unresolved, startup
waits and retries bounded bootstrap attempts without advertising or cancelling
an incumbent's live participation. `close()` cancels this wait.
`ManagedServiceClient.ready()` waits for an accepted agreement. A client owns
one logical feature subscription, negotiates at most 64 unique endpoint/session candidates in
parallel, and releases the losing attempts before reporting readiness.
Its state retains `cleanupFailures` and `lastCleanupFailure` after recovery.
`close()` reports failures from current or pending shutdown cleanup; an earlier
completed recovery failure does not fail a later successful shutdown.
Pending agreements belong to their manager before index writes complete, so
closing a client also revokes attempts blocked in those writes. A service-use
scope index is optional and only a local proposal hint: another live session's participation is
preserved. Exact Concord evidence of permanent authority loss is needed before
cleaning up an unowned predecessor. Provider admission determines which
concurrent proposals receive consent.

Service-use wire records omit both `terms` and `termsHash`. The legacy
`ServiceUseTerms` type (also exported as `ServiceUseScope`) describes a local
capability, not a negotiated wire field. Consumer operation/view limits belong
to its lease; a provider's `scope` callback can narrow its local policy. Changing
a retained provider scope ends the old context and requires a new agreement.

Clients and providers expose immutable, versioned `state`, `watch()`, and
`waitFor()` APIs. The client's `ready()` timeout includes shared-runtime startup.
Supply `descriptor` for negotiation restricted to that exact endpoint/session;
successors need current discovery of the same pair. `request()` accepts an abort
signal. `messages()` and `watchView()` keep their accepted agreement for the
stream's lifetime and end when it loses authority.

A provider can own a `backend` with `start`/`close`, a `handle` callback, and an
optional `authorizeOperation` callback for application resource policy. Handlers
receive a context with cancellation, authority checks, guarded sends, and view
writers. `setBackendStatus("unavailable")` revokes these contexts immediately.
Recovery needs new agreements; old contexts stay revoked. Host-supplied backend
closure must be idempotent, including cleanup after a late startup completion.

Declared payload schemas are checked before handler dispatch or publication;
requests discard invalid replies. Supply a `PayloadSchemas` registry for external
schema references. Runtime validation never fetches schemas from the network or
changes payloads. Domain rules beyond the declaration remain application policy.

Consumers of the same Beacon feature share one cached selection and source
subscription within a `BeaconService`. The last consumer releases that view.
Managed components retain their shared sources from construction through closure,
including components that have not started. Close unused components as well.
`BeaconService.createAdvertisement()` creates an independent publication lifetime;
`ensureAdvertisement()` explicitly reuses one intent's owner. Managed providers
use independent lifetimes, so one provider's closure cannot withdraw another's
advertisement. Committed advertisement handles are retained before waiting for
observations, including cleanup of writes that complete after closure.
Concord participant managers select from the shared source and serialize work
per contract. A blocked domain callback for one contract does not stop another
contract from attaching. `reconcileOne()` waits for a particular contract;
`wake()` schedules a policy reevaluation without waiting for unrelated work.
Before selecting contracts, managers retry cleanup for participation they
previously owned and lost. A failed cancellation during a transport outage is
retried after recovery, even if the old token is still stored. This never adopts
an existing token or cancels a live token owned by another lifetime.

Agreement authority ends permanently when its required source becomes stale,
a token is lost, or renewal fails. Recovery establishes a new opaque contract
ID with generation 1. An exact read cannot restore a closed local agreement.
Client recovery does not replay commands. Beacon withdrawal or discovery
currentness loss leaves an accepted agreement intact while Concord remains valid.

Advertisements and participant leases renew automatically. Their schedules use
the store's broker TTL and injected `RuntimeScheduler`, with jitter and bounded
deadlines. Legacy `refreshIntervalSeconds` options remain accepted for source
compatibility; they no longer set the renewal cadence. Optional per-call TTL
values must agree with the store policy.

View contexts require a live `ServiceViewAuthority`, in addition to identity
fences. Derive consumer contexts with `serviceViewReadContextFromLease` or
`serviceViewWriteContextFromLease`; use `ManagedServiceProvider.viewContext()`
for provider publication. These contexts contain only locally authorized families and
prefixes. Revocation prevents further protected work and wakes idle watches.
View watch items include `current`, `version`, and `generation`; an empty initial
view or a currentness transition produces a `snapshot` item. View-source recovery
is independent of Concord authority.
Writer closure stops writes before cleanup and retains committed revisions for
guarded withdrawal, including a write whose store call completes after closure.

Close clients, view accessors/writers, and providers before closing shared
`BeaconService` and `ConcordService` instances. Close the transport last.
Closure revokes local authority immediately and bounds remote cleanup. An
endpoint session's closure alone does not withdraw advertisements or tokens.

### Managed hardware

Create a `ManagedHardwareManager` with a hardware-manager endpoint lane, shared
Beacon and Concord runtimes, and a map of `HardwareAdvertisementDevice` values.
The host supplies device descriptors, capacity limits, `acceptClaim` policy, and
an optional `handle(message, context)` driver callback. The boundary validates
the exact claim, sender session, device reference, capability, and payload before
calling the driver. A handler that awaits other work must call
`context.assertActive()` again before performing protected device work. Replies
are checked and fenced again after the handler returns.

Capacity admission uses valid Concord claims and reservations for concurrent
attachments. Advertisement counters are discovery hints. A live manager token
from another lifetime blocks bootstrap and advertisement publication; an empty
local registry does not permit adopting or cancelling it.
Unaccepted proposals naming an older discovery session do not block bootstrap;
they remain untouched for their owner to cancel and replace.

Use `publishInput(body)` to deliver a device event through currently owned
claims. It checks authority and descriptor scope at send time and returns the
number of deliveries (zero when unclaimed). Outgoing operations use canonical
hardware-capability subjects. `labels` supplies discovery labels for controller
matching. The exported `buttonMomentaryValueSchema`,
`encoderRelativeValueSchema`, `touchGestureValueSchema`, and
`rasterBitmapCommandSchema` builders let hosts advertise explicit payload
schemas to descriptor-driven consumers in other languages.

`setDevices()` reports connection and descriptor changes. It revokes affected
contexts immediately, retires pending proposals for the changed devices, and
requires exact confirmation of authority loss before publishing a replacement
candidate. If remote cleanup fails, retry the update after the store recovers.
Device drivers retain ownership of physical connections and device behavior.

`ManagedHardwareClient` selects a manager and requested device/count pairs,
negotiates one claim, and provides `send`, `request`, and `messages`. It captures
descriptors for the accepted claim. Discovery disappearance does not revoke
that claim. After Concord authority ends, the client negotiates a new agreement
without replaying commands. `ready(timeoutMs)` bounds startup and negotiation
readiness. Hardware components expose immutable, versioned `state`, initial-state
`watch()`, predicate-based `waitFor()`, structured failures, and `close()`.

Descriptor validators check identity, capability collections and references,
core vocabulary, constraints, and declared payload schemas. `PayloadSchemas`,
exported from the package root, supports JSON Schema drafts 7, 2019-09, and
2020-12 (the default for fragments). Supply referenced schemas through its
constructor and pass the registry as `schemas` to hardware components. Validation
does not fetch URLs, coerce values, insert defaults, or read the specification
checkout at runtime. Raster commands decode PNG/JPEG data and compare actual
encoding and dimensions; decoding is limited to 16 megapixels, with a 128 MiB
JPEG working-memory limit.

### Endpoint messaging

The host supplies a `MessageTransport` and creates an `EndpointSession` for each
local identity. `session.lane("services")` is directly usable as the endpoint
argument to the managed service helpers. `session.lane("hardware_messages")`
provides hardware lane delivery. A lane's `watch()` subscribers share its
underlying transport subscriptions; the last subscriber releases them.

```typescript
import { EndpointSession } from "@deckr/core/messaging";
import { NatsMessageTransport } from "@deckr/core/nats";

const transport = new NatsMessageTransport(connection); // Host-owned connection.
const session = new EndpointSession({ endpoint: "controller:main", transport });
const servicesEndpoint = session.lane("services");
```

`publish()` and `request()` supply the local sender identity and validate the
envelope and core lane body. `request()` arms its inbox before publication,
discards malformed or incorrectly correlated replies, and releases the inbox
on success, publication failure, timeout, cancellation, or closure. Expiry uses
the earlier of the explicit expiry and producer timestamp plus TTL. A received
delivery's `reply()` preserves the contract pointer and targets the requesting
endpoint session.

Transport validation does not grant Concord authority or validate arbitrary
service-specific payload schemas. Managed profile boundaries perform those
checks before domain work. Hosts can supply an additional synchronous `validate`
callback when registering a lane; an extension lane requires one. Envelope
helpers remain exported from `@deckr/core/lanes`; `validateLaneMessage` and
hardware body validators are also exported from `@deckr/core`.

### Store adapters and recovery

Custom stores used by managed runtimes must implement `StateStore.observe()`.
It supplies a complete snapshot with a captured revision, ordered changes,
explicit stale notifications, and cancellation through an `AbortSignal`.
Replay-time changes newer than the captured barrier belong in the snapshot's
`afterBarrier` field; the source applies them before publishing current state.
Returning only an updates stream does not establish currentness.

The NATS adapter monitors consumer continuity, heartbeats, and reconnects,
explicitly deletes its watch consumers, and rebuilds snapshots after loss.
Snapshots are immutable and remove keys absent after recovery. Exact mutations
do not patch the cache; started lifecycle writes wait for their observed revision.
`validateExact` remains available for diagnostics and maintenance. The in-memory
adapter also implements ordered observations and broker-style expiry, and accepts
an injected scheduler for deterministic lifecycle tests.

Concord services sharing the same store pair also share an indexed view. Queries
choose a profile, participant, state, or contract index; token updates invalidate
only the affected agreement's cached validity. `watchContracts({ profile,
participant, contractId, pointer, state, signal })` includes the full filtered
membership, currentness, version, changed identities, and `resnapshotRequired`.
Change detail is capped at 256 identities and registrations at 256; slow readers
retain a current snapshot. Beacon feature watches expose the same snapshot
metadata. Malformed records remain invalid observations and do not interrupt
unrelated authority. Explicit NATS lists use one snapshot consumer and delete it
before returning, without listing keys and fetching each value individually.

Required token renewal uses an absolute monotonic expiry. Delayed timers and late
write acknowledgements cannot extend a terminated lease. New proposals perform
owner bootstrap first, preserving live or pending participation and resolving
proven lost ownership before publication.

With the reference and three implementations checked out as siblings,
[`deckr-integration-tests`](https://github.com/kws/deckr-integration-tests)
uses public managed service APIs for its Compose smoke and recovery workloads.
Its [native Concord runner](https://github.com/kws/deckr-integration-tests#native-concord-lifecycle-test)
uses `scripts/interop-peer.ts` to test concurrent startup, numeric terms,
attachment, automatic renewal, broker restart, and fresh agreements across all
languages.

`BeaconService` and `ConcordService` expose immutable, versioned `state`, `watch()`,
and `waitFor()` with source-health failures and owned resource counts. These report
runtime health; agreement authority remains attached to each agreement. Managed
Beacon advertisements have their own state watches and publication failures.
Watches include current state immediately and deliver terminal state on closure.
Endpoint sessions and Concord participant managers also report lifecycle state,
structured failures, and resource counts. A delivery diagnostic does not make
an otherwise usable endpoint unavailable.

### Optional Concord maintenance

Construct `ConcordReaperService` with dedicated `contractState`, `tokenState`, and
`maintenanceState` capabilities. Construction opens no watches or workers; the
host schedules `scanOnce()` and owns `close()`. Participation never depends on a
maintenance store. The legacy constructor's first `ConcordService` argument is
accepted but unused. `ConcordService.maintenanceCancelContract()` now rejects
calls; maintenance mutations must use the dedicated capability.

The reaper preserves pending contracts that have valid participant tokens and
treats unavailable reads as unknown. Retained deletion validates token identities,
audits metadata without full terms, writes the upstream `cleanup.*` marker, then
deletes the contract with a revision guard. Each generation gets at most eight
immediate token cleanup passes. Conflicts, malformed state, or unavailable stores
leave markers for later scans, independently of contract enumeration. Scan results
distinguish listed audit counts, completed deletions, and pending cleanup.
Supply `audit(event)` to integrate the host's audit sink; the default writes
structured events to the console. Failed pre-deletion audit prevents deletion.
Malformed or identity-mismatched contract records retain stale evidence and emit
an `invalid_contract` scan diagnostic. Concord's malformed-record mutation rule
prevents automatic cancellation or deletion of those records; maintenance never
repairs their identities or terms to make a mutation possible.

The lifecycle and conformance tests exercise these architecture responsibilities:

| Responsibility | Evidence in `tests/` |
| --- | --- |
| Exact stores, ordered sources, renewal, permanent authority loss, independent ownership | `recovery.test.ts`, `nats.test.ts`, `nats-recovery.test.ts` |
| Maintenance access isolation, retention, audit, guarded cleanup and restart | `concord-maintenance.test.ts` |
| Endpoint lanes, expiry, correlation, cancellation and shared subscriptions | `messaging.test.ts`, `nats-messaging.test.ts` |
| Hardware bootstrap, admission, capacity, driver dispatch and device changes | `hardware.test.ts` |
| Service races, scope, backend lifecycle, messages and protected views | `service-client.test.ts`, `service-provider.test.ts`, `lifecycle.test.ts` |
| Action Runtime catalog, payloads, host-resolved references and availability recovery | `action-runtime.test.ts`, `action-runtime-lifecycle.test.ts` |
| Upstream key, canonical JSON, wire and schema artifacts | `contract-vectors.test.ts`, `json.test.ts` |

These are implementation tests, including controlled watch failures and in-process
transport. They do not claim cross-implementation conformance against a live broker.
Controller state allocation, capability selection, page composition, rendering,
settings placement, device drivers, and application behavior remain host-owned.

### Managed Action Runtime

`ManagedActionRuntimeProvider` and `ManagedActionRuntimeClient`, exported from
`@deckr/core/actions`, compose the managed service boundary with the core Action
Runtime catalog. Providers publish `action_availability` for each accepted
agreement before any lifecycle message is needed. `setAvailability()` updates
those protected views. Publication and cleanup for separate peers are independent.
Failed publication retains the latest desired availability and retries with bounded
backoff while the original agreement remains active. View-source recovery also
republishes that value. Closing or losing authority stops retries immediately;
availability recovery neither replaces agreements nor replays lifecycle messages.

Both boundaries require a `resolveReferences(message, authority)` callback. The
host returns current action-instance, binding, or page-session metadata already
scoped to that authorized peer, plus the action descriptor and negotiated hardware
descriptors when a binding is involved. Core checks IDs, capability requirements,
input values, output/overlay generations, and device scope against those records.
When related records are returned together, the message selects its own target:
action and page lifecycle operations do not acquire a binding's hardware dependency.
It never allocates identities or maintains a second controller routing registry.
The callback must resolve current state whenever called. A held handler's
`assertActive()` repeats these checks; agreement loss cancels blocked resolution.

Use `client.send()` for lifecycle events and `client.messages()` for authorized
runtime output requests. Handler contexts expose guarded `send()` and `end()`.
Payloads carry controller-owned context, binding, action, and page references;
the service subject contains only serviceId, namespace, and name. These APIs
never issue hardware commands. The controller resolves the output request and
sends a separate command through its hardware claim.

`validateActionRuntimeMessage()` validates all fourteen current core messages,
including direction, recipient session, payload position, and runtime metadata.
`validateActionRuntimeReferences()` checks a message against supplied current
host state. `ActionRuntimePayloadSchemas` resolves core catalog references using
native validators; instantiate it with additional schema references for extension
capability payloads and pass it as `schemas`. Runtime code does not load the
external specification checkout. Tests read its fixtures directly.

### Action Runtime migration

`actionRuntimeProtocol()` in `@deckr/core/actions` implements the current upstream
Action Runtime declaration. Protocol view families may omit `keyPrefix`;
`serviceProtocolViews(protocol, serviceId)` binds their concrete prefixes for
advertisements and provider authorization. Consumers use the prefixes from each
validated descriptor. Pass a provider instance ID to `actionRuntimeProtocol`
when a declaration for one specific provider is needed. `actionProviderAddress(id)`
now returns `service:action-runtime.<id>`. Lifecycle messages use snake_case
names in `serviceMessage` envelopes on the `services` lane and standard
service-use agreements.

`actionsBeaconPayload()` now builds a standard service advertisement. Its legacy
`actions`, `labels`, and `annotations` arguments do not publish action metadata.
Use `actionAvailabilityPayload()` to build the protected `action_availability`
view payload, including descriptors, labels, and annotations, and publish it
through an authorized provider view context. `actionProviderSessionTerms()` is
a compatibility builder for local service-use scope; `actionProviderSessionContractId()`
returns a fresh opaque ID. `actionProvidersBroadcast()` is deprecated and throws,
since current discovery uses Beacon. Legacy settings/extension constants remain
exported but are not operations in the current Action Runtime declaration.
Action descriptors now follow the current upstream field set. Legacy `hints`,
`capacity`, and `dynamicPageTemplates` values remain application-owned; descriptor
builders do not serialize them. Wire validators reject these obsolete fields,
along with obsolete `roleId` and `templateId` fields in runtime metadata.

For canonicalization of received JSON, use `canonicalJsonFromText` and
`canonicalJsonHashFromText` from `@deckr/core/json`. They preserve numeric token
kinds and large integers required by the upstream terms-hashing vectors.
The value-based helpers and lifecycle APIs preserve `JsonNumber` values produced
by `parseJson`, including through cloning and NATS serialization. Use `parseJson`
when receiving terms as text. Ordinary JavaScript numeric literals and
`JSON.parse` cannot retain the distinction between `1` and `1.0` or arbitrary-size
integers; precision already lost by those paths cannot be recovered by hashing.

Controller routing, rendering, layout persistence, and settings placement belong
in application repositories such as
[deckr-controller](https://github.com/kws/deckr-controller). Concrete drivers and
device behavior belong in hardware-manager applications.

## Package versions and builds

The root `package.json` owns the `@deckr/core` version. Keep `package-lock.json`
in sync with version or dependency changes using npm. Package versions are
independent of the upstream specification bundle version.

The package currently remains `private: true`. To produce a local distributable,
run `npm run check` and `npm pack`; the tarball contains JavaScript, declarations,
the README, and license. Registry publishing and release automation are not
configured.
