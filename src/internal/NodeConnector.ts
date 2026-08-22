import {
  AuthorizationError,
  InvalidArgumentError,
  ProtocolError,
  TimeoutError,
  UserAuthenticationExpiredError
} from "@nats-io/nats-core"
import { connect } from "@nats-io/transport-node"
import { Effect, Layer } from "effect"
import { Connector, type Interface } from "./Connector.ts"
import { ConnectFatalError, type ConnectIssue, ConnectTransientError } from "./Errors.ts"
import type { Connection } from "./NatsTransport.ts"

const codeAt = (value: unknown): string | undefined => {
  if (typeof value !== "object" || value === null) return undefined
  const code = (value as { code?: unknown }).code
  return typeof code === "string" ? code : undefined
}

// Direct code first, then the wrapped system error: transport-node nests the
// dial error under `cause`, but TLS and DNS failures can carry their code on
// the rejection itself.
const systemCodeOf = (cause: unknown): string | undefined =>
  codeAt(cause)
    ?? codeAt(
      typeof cause === "object" && cause !== null ? (cause as { cause?: unknown }).cause : undefined
    )

const tlsCodePattern = /^(ERR_TLS_|ERR_SSL_|UNABLE_TO_|SELF_SIGNED|CERT_|DEPTH_ZERO)/
const tlsMessagePattern = /certificate|self.signed|tls|ssl/i

// Server URLs may carry userinfo (nats://user:pass@host); never let it into
// error detail, logs, or schemas.
const redact = (text: string): string => text.replace(/\/\/[^@/\s]+@/g, "//<redacted>@")

const messageOf = (cause: unknown): string | undefined =>
  typeof cause === "object" && cause !== null && "message" in cause
    && typeof (cause as { message?: unknown }).message === "string"
    ? redact((cause as { message: string }).message)
    : undefined

/**
 * Maps one connect-time rejection to a classified issue.
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
  const operation = "Connector.connect"
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
  const code = systemCodeOf(cause)
  // TLS/certificate failures are configuration, not weather: retrying cannot
  // fix a rejected certificate, so they must not hide behind "unavailable"
  if (
    (code !== undefined && tlsCodePattern.test(code))
    || (detail !== undefined && tlsMessagePattern.test(detail))
  ) {
    return new ConnectFatalError({ operation, reason: "config", detail })
  }
  const reason = code === "ECONNREFUSED" ? "refused" : "unavailable"
  return new ConnectTransientError({ operation, reason, detail })
}

/**
 * Builds a `Connector` over any dial function. Interruption while the dial is
 * pending abandons it, and a connection that resolves after abandonment is
 * closed exactly once - a late success must not leak a socket with no owner
 * (Scope ownership; a dial cannot be cancelled, only disowned).
 */
export const connectorFromDial = (
  dial: (options: { readonly servers: ReadonlyArray<string> }) => Promise<Connection>
): Interface => ({
  connect: (options) =>
    Effect.suspend(() => {
      const pending = dial(options)
      return Effect.tryPromise({
        try: () => pending,
        catch: classifyConnectCause
      }).pipe(
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            pending.then(
              (connection) => {
                connection.close().catch(() => {})
              },
              () => {}
            )
          })
        )
      )
    })
})

/**
 * The production `Connector`: one wrapped call to the Node
 * connect entry point. No retries here - supervision belongs to the acquire
 * loop above this seam.
 */
export const nodeConnector: Interface = connectorFromDial(({ servers }) =>
  connect({ servers: [...servers] })
)

export const layerNodeConnector = Layer.succeed(
  Connector,
  nodeConnector
)
