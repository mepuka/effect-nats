import { Context } from "effect"
import type { Effect } from "effect"
import type { ConnectIssue } from "./Errors.ts"
import type { Connection } from "./NatsTransport.ts"

/** Options handed to the underlying client's connect entry point. */
export interface ConnectOptions {
  readonly servers: ReadonlyArray<string>
}

export interface Interface {
  /**
   * Opens one connection, or fails with a classified issue.
   * Classification (transient versus fatal) belongs behind this seam; the
   * supervised acquire loop above never inspects raw adapter errors.
   */
  readonly connect: (
    options: ConnectOptions
  ) => Effect.Effect<Connection, ConnectIssue>
}

/**
 * The seam this client owns for acquiring connections. Production
 * adapters implement it once over the underlying connect entry point;
 * everything above stays adapter-free and test-fakeable.
 */
export class Connector extends Context.Service<Connector, Interface>()(
  "@nats-effect/internal/Connector"
) {}
