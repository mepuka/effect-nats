# Upstream compatibility CI

This repository separates reproducible acceptance from early warning against new upstream
releases.

## Required CI

`.github/workflows/ci.yml` installs the native `nats-server` release pinned in that workflow and
runs the repository's locked NATS.js dependencies. The pin makes pull-request results
reproducible. It is a conformance-test input, not a claim that the pinned release is the minimum
supported server.

The runner installs an official `nats-io/nats-server` release artifact rather than relying on a
Docker image. Every run prints the Node, pnpm, and server versions before verification starts.

## Upstream canary

`.github/workflows/nats-upstream-canary.yml` runs for pull requests, pushes to `main`, a daily
schedule, and manual dispatches. It:

1. installs the latest stable native `nats-server` GitHub release;
2. updates every declared `@nats-io/*` dependency to its latest npm release in the ephemeral
   runner checkout;
3. runs the type-check and complete test suite; and
4. writes the exact tested versions to the GitHub job summary.

The canary deliberately follows only stable `latest` release channels. Its checkout changes are
ephemeral: it does not commit or publish them. Keeping this moving-input job separate from pinned
CI makes both outcomes visible on every pull request without conflating reproducibility with
forward-compatibility. A green canary is compatibility evidence, not proof that every newly
introduced upstream behavior has been modeled.

## Update flow

Dependabot groups declared `@nats-io/*` package releases into reviewable pull requests and also
tracks GitHub Action revisions. For each NATS.js update:

1. inspect the official release notes and source diff for new success, status, error, transport,
   wire, cancellation, reconnect, and terminal-stream branches;
2. update the affected outcome ledger, ADR, implementation, and deterministic tests where the
   public behavior changes; and
3. merge only after required CI and the upstream canary configuration both pass.

For a new stable `nats-server` release, the canary starts exercising it without changing required
CI. After reviewing release notes and conformance results, update `NATS_SERVER_VERSION` in
`.github/workflows/ci.yml` in a normal pull request and retain the exact test receipt. If the
project later declares a supported server-version floor, test that floor separately from this
current-release pin.
