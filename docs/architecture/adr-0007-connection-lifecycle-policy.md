# ADR-0007: Connection lifecycle policy

Status: Accepted (2026-08-21)

## Context

ADR-0002 (wrap the official client, accepted) chose to wrap the official client behind internal seams and deferred
four externally observable choices. This ADR accepts the first coherent set:
the supervised connect/retry policy and release-drain policy implemented in
`src/internal/NatsTransport.ts`, `src/internal/OfficialConnect.ts`, and
`src/internal/NodeOfficialConnect.ts`. Queue capacity and overflow ordering
for subscriptions are explicitly **not** decided here; they bind to the
subscription slice.

## Decisions

### 1. Connect retry supervision

- Default schedule: `Schedule.exponential("200 millis")`, jittered, hard-bounded
  by `Schedule.upTo({ duration: "30 seconds" })`.
- The `upTo` duration limit is detected on the step _following_ the one that
  crosses it, so one attempt may land past the bound; this follow-through is an
  accepted property, pinned by
  `test/internal/connectLifecycle.test.ts` (times `[0, 1000, 3000, 7000, 15000]`
  under a 1s/10s variant).
- Only classified-transient issues are retried. Fatal issues surface directly,
  unretried.
- Schedule exhaustion surfaces exactly once as `ConnectExhaustedError`
  (`operation`, `attempts` counting every invocation including the final one,
  `last` carrying the triggering transient issue) alongside a log record.
- The schedule is injectable per layer call; tests always inject deterministic
  non-jittered schedules.

### 2. Failure classification at the connect boundary

Classification lives behind `OfficialConnect` and maps official rejections as
follows (evidence: `@nats-io/transport-node@3.4.0/lib/node_transport.js:68-79`;
`@nats-io/nats-core@3.4.0/lib/errors.d.ts`):

| Official rejection                                     | Issue                   |
| ------------------------------------------------------ | ----------------------- |
| `AuthorizationError`, `UserAuthenticationExpiredError` | fatal `auth`            |
| `ProtocolError`                                        | fatal `protocol`        |
| `InvalidArgumentError`                                 | fatal `config`          |
| `TimeoutError`                                         | transient `timeout`     |
| dial failure, cause code `ECONNREFUSED`                | transient `refused`     |
| any other failure or unknown shape                     | transient `unavailable` |

Unknown shapes default to transient because retryability is the safer default
for unmapped failures; enumerated fatal classes are closed under evidence and
extended only with new citations.

### 3. Release drain policy

Release is idempotent and ordered: one drain attempt, then close, with close
running even when drain fails. Without an explicit timeout the drain waits
unboundedly (correctness over liveness); callers opt into a bounded drain,
which abandons - not cancels - the underlying promise at the deadline because
the official drain cannot be cancelled. Pinned by
`test/internal/natsTransport.test.ts`.

## Consequences

- Reconnect-style looping is bounded and observable by construction; the
  TO_FIX supervision gate (fatal non-retry, spacing, exhaustion observability,
  interruption) is discharged by the TestClock proofs listed above.
- Changing the 30-second bound or the 200ms base later is a parameter change,
  not a semantic one; changing classification requires new source evidence.
- Server-dependent behavior (handshake success against real servers) remains
  untested until the integration suite exists; nothing in this ADR claims it.
