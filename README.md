# @deckr/core

The TypeScript implementation of Deckr's shared core, built as an ESM package
with TypeScript declarations. This repository contains reusable wire models,
validation, discovery, agreement lifecycle, messaging, and service helpers.

The authoritative specifications, schemas, catalogs, fixtures, and conformance
scenarios live in **[kws/deckr](https://github.com/kws/deckr)**, normally checked
out beside this repository at `../deckr`. Protocol changes belong there first;
this repository implements and tests those contracts. It does not author or
vendor the specification bundle.

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

Tests use Node's test runner. Lifecycle tests use in-memory stores, and NATS
adapter tests use fakes; no running broker is required. These tests cover the
implemented surfaces, not every scenario in the upstream specification.

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
| `@deckr/core/actions` | Action metadata, lifecycle payloads, and settings targets |
| `@deckr/core/beacon` | Discovery records and lifecycle helpers |
| `@deckr/core/concord` | Agreement records, participant tokens, and lifecycle helpers |
| `@deckr/core/endpoint` | Endpoint addresses and message targets |
| `@deckr/core/json` | JSON validation and canonicalization |
| `@deckr/core/keys` | Storage key token encoding |
| `@deckr/core/lanes` | Message envelopes, routing, and header validation |
| `@deckr/core/nats` | NATS connection and state-store adapter |
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

## Managed runtime lifecycle

The implementation follows the boundaries in the upstream
[architecture guide](../deckr/docs/deckr-architecture.md). The host owns the
transport and shared stores. Applications supply service behavior and domain
consent; the managed runtimes own observation, negotiation, renewal, and cleanup.

| Boundary | Implementation |
| --- | --- |
| Ordered store observations | `MaterializedStateSource` and `sourceFor`, exported from `@deckr/core` |
| Discovery and advertisements | `BeaconService` and `BeaconAdvertisement` |
| Agreement authority and participant tokens | `ConcordService`, `ConcordAgreement`, and participant managers |
| Service selection and recovery | `ManagedServiceClient`, exported from `@deckr/core/services` |
| Service consent and participation | `ManagedServiceProvider`, exported from `@deckr/core/services` |
| Protected view reads and writes | `ManagedServiceViewAccess` with a live lease capability |

Start shared Beacon and Concord runtimes before using their managed components,
or let the components start them on first use. `ManagedServiceProvider.start()`
starts participation before advertising availability. Its `acceptContract`
callback supplies domain consent; call `reconcile()` when that policy changes.
`ManagedServiceClient.ready()` waits for an accepted agreement. A client owns
one feature watch, negotiates at most 64 unique endpoint/session candidates in
parallel, and releases the losing attempts before reporting readiness.

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
for provider publication. These contexts contain only negotiated families and
prefixes. Revocation prevents further protected work and wakes idle watches.
View watch items include `current`, `version`, and `generation`; an empty initial
view or a currentness transition produces a `snapshot` item. View-source recovery
is independent of Concord authority.

Close clients, view accessors/writers, and providers before closing shared
`BeaconService` and `ConcordService` instances. Close the transport last.
Closure revokes local authority immediately and bounds remote cleanup. An
endpoint session's closure alone does not withdraw advertisements or tokens.

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
through a negotiated provider view context. `actionProviderSessionTerms()` is
a compatibility builder for service-use terms; `actionProviderSessionContractId()`
returns a fresh opaque ID. `actionProvidersBroadcast()` is deprecated and throws,
since current discovery uses Beacon. Legacy settings/extension constants remain
exported but are not operations in the current Action Runtime declaration.

For canonicalization of received JSON, use `canonicalJsonFromText` and
`canonicalJsonHashFromText` from `@deckr/core/json`. They preserve numeric token
kinds and large integers required by the upstream terms-hashing vectors.
The value-based helpers and lifecycle APIs use JavaScript numbers, which cannot
retain the distinction between `1` and `1.0`. Use safe integer or string-encoded
decimal terms with those APIs; generic numeric terms require further integration
of number-preserving decoding into the lifecycle APIs.

Controller orchestration, rendering policy, filesystem configuration, and device
management belong in [deckr-controller](https://github.com/kws/deckr-controller).

## Package versions and builds

The root `package.json` owns the `@deckr/core` version. Keep `package-lock.json`
in sync with version or dependency changes using npm. Package versions are
independent of the upstream specification bundle version.

The package currently remains `private: true`. To produce a local distributable,
run `npm run check` and `npm pack`; the tarball contains JavaScript, declarations,
the README, and license. Registry publishing and release automation are not
configured.
