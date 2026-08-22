# ADR-0001: Reconnect limits and policy

Status: accepted, 2026-08-21. Ruled by the project owner via delegation in-session.
Scope: reconnect attempt limits, backoff policy, classification, exhaustion, and observability
for the connection supervisor. Restoration behavior after a successful reconnect (subscription
re-establishment, in-flight request disposition, pending publish buffers) is a separate decision
and is explicitly not ruled here.

## Context

Three constraints already bind this decision:

- The style contract requires bounded, jittered retries with exposed exhaustion, and permits
  continuous reconnect only through explicit configuration with observable status
  ([STYLE.md:53-63](../STYLE.md)).
- The verified Effect pattern map instructs: classify retry eligibility by typed reason, own
  reconnect in one scoped supervisor with one combined attempt budget, and do not copy the
  first-party RPC client's unbounded default schedule
  ([official-effect-client-patterns.md, section 8](../research/official-effect-client-patterns.md)).
- The pinned official client's shape is bounded by default: `reconnect: true`,
  `DEFAULT_MAX_RECONNECT_ATTEMPTS = 10` (per server, with server rotation),
  `DEFAULT_RECONNECT_TIME_WAIT = 2000` ms, additive jitter `DEFAULT_JITTER = 100` ms
  (`DEFAULT_JITTER_TLS = 1000` ms), and a server is removed from rotation after its attempt
  budget unless the configured value is `-1`
  (`nats.js@3a7b736:core/src/options.ts:23-45,103-111`,
  `nats.js@3a7b736:core/src/protocol.ts:650-731`).

## Decision

1. One owner, one budget. The scoped connection supervisor is the only reconnect loop in the
   client. No other layer — request mux, JetStream substrate, KV, Object Store — wraps its
   operations in reconnect retries; higher layers observe connection status and apply their own
   operation-level policies only where an ADR grants them one. Stacked reconnect loops are a
   defect.
2. Bounded by default, at official-client parity. The default policy is attempt-bounded with
   server rotation: 10 attempts per configured/discovered server, base delay 2000 ms, additive
   jitter (100 ms, or 1000 ms under TLS), expressed as an Effect `Schedule` at implementation
   time. Parity with the pinned official defaults is deliberate: operators migrating from
   official clients see familiar failure behavior. The exact `Schedule` composition is an
   implementation detail; the budget, base delay, and jitter presence are the contract.
3. Continuous reconnect is opt-in and typed. Unbounded reconnection requires an explicit
   configuration value modeled as a typed alternative (an `"unbounded"` variant, not a `-1`
   sentinel; `-1` may be accepted only at a compatibility boundary that maps it to the typed
   variant). When active, every failed cycle emits a status event carrying the failure reason
   and the next delay, so an unbounded loop is always externally observable, never a silent
   spin.
4. Classified reasons. Reconnection is scheduled only for typed transient transport reasons.
   Fatal reasons — authorization/authentication rejection, TLS and configuration errors,
   protocol violations — terminate the reconnect loop with a typed terminal reason instead of
   consuming the transient budget. The exact fatal set is pinned against official client and
   server behavior during implementation; the invariant ruled here is that no fatal reason may
   loop unbounded and every classification branch carries a deterministic test.
5. Exhaustion is a typed terminal state. When the budget is exhausted (or a fatal reason ends
   the loop), the connection transitions to a terminal closed state carrying the last failure
   and attempt count; the status stream emits a terminal event; pending and subsequent
   operations fail with typed errors. Readiness latches keep their success-only release rule
   while retries remain; on terminal exhaustion the supervisor fails outstanding waiters with
   the terminal reason so no fiber waits forever on a connection that will never arrive.
6. Deterministic proof gate. `TestClock` tests must prove: exact base delays within jitter
   bounds, exact attempt counts at exhaustion, fatal-reason non-retry, schedule reset after a
   successful connection, bounded-time interruption during backoff, and the full status event
   sequence including the terminal event. These tests are part of the reconnect
   implementation's definition of done, per the stream-discipline exit criteria.

## Configuration surface

The pinned official option family governed by this ADR, with its surfacing rule
(`nats.js@3a7b736:core/src/core.ts:1167-1310`; defaults at `core/src/options.ts:23-45`):

| Official option                                                                             | Pinned meaning                                                     | Surfaced here as                                                               |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `reconnect` (default `true`)                                                                | enable reconnection                                                | typed reconnect policy present/absent                                          |
| `maxReconnectAttempts` (default `10`, `-1` = never give up)                                 | per-server attempt budget                                          | `attempts` in the bounded variant; `"unbounded"` variant replaces `-1`         |
| `reconnectTimeWait` (default `2000` ms)                                                     | base delay between attempts                                        | base delay of the policy `Schedule`                                            |
| `reconnectJitter` / `reconnectJitterTLS` (defaults `100` / `1000` ms)                       | additive random delay bound, TLS-specific bound                    | jitter bounds of the policy `Schedule`                                         |
| `reconnectDelayHandler`                                                                     | caller-supplied delay function                                     | custom `Schedule`, replacing base+jitter wholesale                             |
| `waitOnFirstConnect` (default `false`)                                                      | attempt budget does not trigger until one connection has succeeded | typed first-connect policy; default parity                                     |
| `ignoreAuthErrorAbort` (default `false`: abort after two identical consecutive auth errors) | auth-error fatality escape hatch                                   | classification override, explicit and observable like `"unbounded"`            |
| `noRandomize`, `reconnectToServer`                                                          | server-selection order/hook                                        | server-rotation options, orthogonal to limits but owned by the same supervisor |

Surfacing follows the style contract: concrete typed options on the connection layer, with an
optional `layerConfig` built on Effect `Config`; library code never reads `process.env`
([STYLE.md:67-68](../STYLE.md)). Every option above must be expressible in the typed surface —
an official capability silently dropped (for example, no way to express `waitOnFirstConnect`)
is a defect against official-client parity. The `ignoreAuthErrorAbort` fact also pins the
default fatal-auth rule for decision 4: two identical consecutive auth errors abort the loop
unless this override is set.

## Consequences

- The KV slice's open watch reconnect/resume question
  ([kv-slice-scoping.md](kv-slice-scoping.md), open decision 3) narrows: watches never own
  reconnection; they observe supervisor status, and only the resume-without-gaps question
  remains open.
- The TO_FIX watch item on unchosen reconnect limits is discharged for the limits portion;
  restoration behavior remains an open workbench item.
- Configuration surface gains a typed reconnect policy value (bounded attempts versus explicit
  unbounded) rather than a numeric sentinel, at the cost of a small mapping shim wherever
  official-client option compatibility is offered.
