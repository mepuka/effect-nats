/**
 * NatsConnection — the scoped, shared broker connection.
 *
 * Lifecycle is Layer-owned (acquire on build, close on scope exit) with
 * resilient reconnect defaults folded in (documented agent-runtime defaults:
 * retry indefinitely, wait on first connect — attribution to the Synadia SDK's
 * published defaults). Terminal `close` surfaces as a stream element, never a
 * silent hang.
 */
import { Context, Effect, Layer, Schema, Stream } from "effect"
import {
  connect,
  type NatsConnection as NC,
  type NodeConnectionOptions,
  type Status,
} from "@nats-io/transport-node"

export class ConnectionLost extends Schema.TaggedError<ConnectionLost>()(
  "effect-nats-agents/ConnectionLost",
  { reason: Schema.String },
) {}

/**
 * Agent-runtime reconnect defaults (values documented by @synadia-ai/agents'
 * `withAgentReconnectDefaults`; inlined to avoid depending on that package):
 * keep retrying forever, 2s between attempts, jitter, tolerate first-connect
 * failure while the broker is down.
 */
export const AGENT_RECONNECT_DEFAULTS = {
  maxReconnectAttempts: -1,
  reconnectTimeWait: 2000,
  reconnectJitter: 200,
  waitOnFirstConnect: true,
} as const

export interface NatsConnectionShape {
  readonly nc: NC
  readonly maxPayload: number
  /**
   * Connection state transitions. Ends normally when the connection closes
   * cleanly; a transport death is a TYPED failure — supervisors must be able
   * to observe it (silent-death is the failure mode this design rejects).
   */
  readonly status: Stream.Stream<Status, ConnectionLost>
}

export class NatsConnection extends Context.Service<NatsConnection, NatsConnectionShape>()(
  "effect-nats-agents/NatsConnection",
) {
  /** Build the connection service for the given broker options. */
  static readonly layer = (
    options: NodeConnectionOptions,
  ): Layer.Layer<NatsConnection, ConnectionLost> =>
    Layer.effect(
      NatsConnection,
      Effect.gen(function* () {
        const nc = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: () => connect({ ...AGENT_RECONNECT_DEFAULTS, ...options }),
            catch: (cause) =>
              new ConnectionLost({ reason: `connect failed: ${String(cause)}` }),
          }),
          (conn) =>
            Effect.promise(() =>
              conn
                .close()
                .catch(() => undefined)
                .then(() => undefined),
            ),
        )

        return {
          nc,
          maxPayload: nc.info?.max_payload ?? 1_048_576,
          status: Stream.fromAsyncIterable(
            nc.status(),
            (e) => new ConnectionLost({ reason: String(e) }),
          ),
        }
      }),
    )
}
