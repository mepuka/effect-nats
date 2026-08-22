import { Context } from "effect"
import type { Effect } from "effect"
import type { ConnectIssue } from "./Errors.ts"
import type { OfficialConnection } from "./NatsTransport.ts"

/** Options handed to the official client's connect entry point. */
export interface OfficialConnectOptions {
  readonly servers: ReadonlyArray<string>
}

export interface Interface {
  /**
   * Opens one official connection, or fails with a classified issue.
   * Classification (transient versus fatal) belongs behind this seam; the
   * supervised acquire loop above never inspects raw adapter errors.
   */
  readonly connect: (
    options: OfficialConnectOptions
  ) => Effect.Effect<OfficialConnection, ConnectIssue>
}

/**
 * The seam this client owns for acquiring official connections. Production
 * adapters implement it once over the official connect entry point;
 * everything above stays adapter-free and test-fakeable.
 */
export class OfficialConnect extends Context.Service<OfficialConnect, Interface>()(
  "@nats-effect/internal/OfficialConnect"
) {}
