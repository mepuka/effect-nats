import {
  AuthorizationError,
  InvalidArgumentError,
  ProtocolError,
  TimeoutError,
  UserAuthenticationExpiredError
} from "@nats-io/nats-core"
import { connect } from "@nats-io/transport-node"
import { Effect, Layer } from "effect"
import { ConnectFatalError, type ConnectIssue, ConnectTransientError } from "./Errors.ts"
import { type Interface, OfficialConnect } from "./OfficialConnect.ts"

const systemCodeOf = (cause: unknown): string | undefined => {
  if (typeof cause !== "object" || cause === null || !("cause" in cause)) return undefined
  const nested = (cause as { cause?: unknown }).cause
  if (typeof nested !== "object" || nested === null) return undefined
  const code = (nested as { code?: unknown }).code
  return typeof code === "string" ? code : undefined
}

const messageOf = (cause: unknown): string | undefined =>
  typeof cause === "object" && cause !== null && "message" in cause
    && typeof (cause as { message?: unknown }).message === "string"
    ? (cause as { message: string }).message
    : undefined

/**
 * Maps one official connect rejection to a classified issue.
 *
 * Evidence: `@nats-io/transport-node@3.4.0` wraps every dial failure in
 * `ConnectionError` (`lib/node_transport.js:68-79`) with the system error as
 * `cause`, whose `code` distinguishes refusal; authorization, protocol, and
 * argument failures reject as their own classes
 * (`@nats-io/nats-core@3.4.0/lib/errors.d.ts`). Unknown shapes stay transient:
 * an unmapped failure is treated as retryable until evidence says otherwise,
 * while genuinely fatal classes are enumerated above.
 */
export const classifyConnectCause = (cause: unknown): ConnectIssue => {
  const operation = "OfficialConnect.connect"
  const detail = messageOf(cause)
  if (
    cause instanceof AuthorizationError
    || cause instanceof UserAuthenticationExpiredError
  ) {
    return new ConnectFatalError({ operation, reason: "auth", detail })
  }
  if (cause instanceof ProtocolError) {
    return new ConnectFatalError({ operation, reason: "protocol", detail })
  }
  if (cause instanceof InvalidArgumentError) {
    return new ConnectFatalError({ operation, reason: "config", detail })
  }
  if (cause instanceof TimeoutError) {
    return new ConnectTransientError({ operation, reason: "timeout", detail })
  }
  const reason = systemCodeOf(cause) === "ECONNREFUSED" ? "refused" : "unavailable"
  return new ConnectTransientError({ operation, reason, detail })
}

/**
 * The production `OfficialConnect`: one wrapped call to the official Node
 * connect entry point. No retries here - supervision belongs to the acquire
 * loop above this seam.
 */
export const nodeOfficialConnect: Interface = {
  connect: ({ servers }: { readonly servers: ReadonlyArray<string> }) =>
    Effect.tryPromise({
      try: () => connect({ servers: [...servers] }),
      catch: classifyConnectCause
    })
}

export const layerNodeOfficialConnect = Layer.succeed(
  OfficialConnect,
  nodeOfficialConnect
)
