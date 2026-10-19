# AGENTS.md

This repository is the TypeScript-only implementation of `@deckr/core`.
Use [README.md](./README.md) for setup, validation, and packaging guidance.

## Ownership and placement

- Keep implementation in `src/` and TypeScript tests in `tests/`.
- The authoritative specification, schemas, fixtures, vectors, and conformance
  scenarios live in `kws/deckr`, normally checked out at `../deckr`.
- Read upstream artifacts directly for conformance tests. Do not copy or generate
  a specification bundle here. `DECKR_SPEC_ROOT` selects another checkout;
  `.deckr-spec-revision` pins the revision used by CI.
- Change and review contracts upstream before implementing new wire behavior.
- Keep helpers reusable across Deckr components. Controller orchestration,
  rendering policy, filesystem configuration, and device-manager policy belong
  in `deckr-controller`.
- Do not add other language implementations or their toolchains here.

## Implementation guidance

- Preserve the public `@deckr/core` package exports when reorganizing internals.
- Use ESM and explicit `.ts` extensions for relative source imports; the build
  rewrites them to `.js` in `dist/`.
- Keep generic JSON, key, state, and protocol primitives independent of
  controller policy and application-specific action behavior.
- Keep the NATS client optional and load it only when connecting.
- Keep protocol definitions and normative documentation in `kws/deckr`;
  implementation usage and development instructions belong in this README.

## Validation and packaging

Use npm from the repository root:

```bash
npm ci
npm run check
npm pack --dry-run
```

`npm run check` runs TypeScript checking, tests, and the package build. Tests
must fail clearly if the external specification checkout is unavailable.
Keep `package.json` and `package-lock.json` synchronized. The package is private;
see the README before changing packaging or release behavior.
