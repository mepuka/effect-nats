# ADR-0001 implementation and conformance status

This is an evidence receipt, not a replacement for ADR-0001. It records what the current internal
slice compiles and proves, and keeps the remaining obligations explicit.

## Typed-to-official mapping

`src/internal/ReconnectPolicy.ts` is the one compatibility boundary. The public surface remains
closed.

| Typed intent                                          | Official `@nats-io/transport-node@3.4.0` option              |
| ----------------------------------------------------- | ------------------------------------------------------------ |
| `Disabled`                                            | `reconnect: false`                                           |
| bounded positive attempt count                        | `maxReconnectAttempts: n`                                    |
| `Unbounded`                                           | `maxReconnectAttempts: -1`, emitted only at this boundary    |
| additive jitter                                       | `reconnectTimeWait`, `reconnectJitter`, `reconnectJitterTLS` |
| fail-fast / waiting first connect                     | `waitOnFirstConnect: false / true`                           |
| abort / continue after consecutive auth-family errors | `ignoreAuthErrorAbort: false / true`                         |
| randomized / configured server order                  | `noRandomize: false / true`                                  |
| custom positive finite delay callback                 | `reconnectDelayHandler`                                      |

The compiler rejects non-positive or non-integral bounded attempts, non-finite or non-positive
base delays, negative/non-finite jitter, and non-positive/non-finite custom-delay results before
they reach the official client.

One ADR wording mismatch remains open: ADR-0001 calls the custom delay an Effect `Schedule`, but
the official `reconnectDelayHandler` is synchronous, argument-free, and owns its own state. A
stateful/effectful `Schedule` cannot be translated faithfully into that hook. The current internal
type therefore exposes the honest synchronous compatibility callback. Resolving this requires an
ADR amendment; it must not be disguised with `Effect.runSync` or a second reconnect loop.

## Native-server evidence

`test/ReconnectConformanceLive.test.ts` runs against the locally installed native
`nats-server` (verified in the implementation run as 2.14.5), never Docker-only. It starts two
independent servers for each transport mode and uses a committed test-only CA/server certificate
for TLS.

Covered for both plain TCP and TLS:

- configured server order connects to server one;
- stopping server one produces lifecycle order `disconnect`, `reconnecting`, `reconnect` (cluster
  `update` events may interleave and remain observable);
- the same callback subscription is restored on server two and delivers after a flush witness;
- with two attempts per server and both servers stopped, exactly four `reconnecting` events occur,
  followed by terminal `close` and an error-valued `closed()` result;
- the Windows server fixture recognizes both numeric exit and signal exit, so cleanup does not
  leave a finalizer waiting for an impossible second exit.

## Still open

- [ ] Live first-connect conformance for both `FailFast` and `Wait`, including interruption while
      no server is available.
- [ ] Two consecutive auth-family failures, the default abort, and the explicit continue override.
- [ ] Non-zero additive-jitter lower/upper-bound observations on plain and TLS paths. Current tests
      prove the exact compiled settings; the live suite uses zero jitter for deterministic attempt
      counts.
- [ ] In-flight Core request disposition across reconnect, including late replies and mux cleanup.
- [ ] Explicit Core publish loss/ambiguity exposure for writes accepted locally immediately before
      disconnect.
- [ ] Reconnect interruption and zero live socket/fiber counters during backoff and active dial.
- [ ] An adapter-neutral typed form of `reconnectToServer`; `noRandomize` is mapped, but the hook is
      not silently exposed with official `Server` types.
- [ ] Resolve the Effect `Schedule` versus synchronous `reconnectDelayHandler` mismatch above.
