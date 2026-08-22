import { Schema } from "effect"

/**
 * A subscription could not admit delivery within its declared capacity.
 *
 * Overflow terminates the subscription with drain-first ordering: already
 * admitted values remain observable before the error, while the triggering
 * value is rejected. Producer admission and consumer pulls observe the same
 * error instance.
 */
export class SlowConsumerError extends Schema.TaggedError<SlowConsumerError>()(
  "SlowConsumerError",
  {
    operation: Schema.String,
    subject: Schema.String,
    capacity: Schema.Number,
    pending: Schema.Number
  }
) {}

/**
 * The subscription or connection can no longer admit work.
 *
 * `cause` carries the underlying adapter failure when one exists; the closed
 * guard path has none.
 */
export class ConnectionClosedError extends Schema.TaggedError<ConnectionClosedError>()(
  "ConnectionClosedError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect())
  }
) {}

/**
 * A connect attempt failed for a reason a later attempt may resolve: server
 * unreachable, starting up, or the attempt timed out.
 */
export class ConnectTransientError extends Schema.TaggedError<ConnectTransientError>()(
  "ConnectTransientError",
  {
    operation: Schema.String,
    reason: Schema.Literals(["refused", "unavailable", "timeout"]),
    detail: Schema.optional(Schema.String)
  }
) {}

/**
 * A connect attempt failed for a reason no retry can fix: authorization,
 * protocol negotiation, or configuration.
 */
export class ConnectFatalError extends Schema.TaggedError<ConnectFatalError>()(
  "ConnectFatalError",
  {
    operation: Schema.String,
    reason: Schema.Literals(["auth", "protocol", "config"]),
    detail: Schema.optional(Schema.String)
  }
) {}

/** The classified outcome channel of one connect attempt. */
export type ConnectIssue = ConnectTransientError | ConnectFatalError

/**
 * The bounded connect schedule ran out before any attempt succeeded.
 *
 * `attempts` counts every invocation; `last` carries the transient issue that
 * triggered exhaustion. Fatal issues never reach this error - they surface
 * directly and unretried.
 */
export class ConnectExhaustedError extends Schema.TaggedError<ConnectExhaustedError>()(
  "ConnectExhaustedError",
  {
    operation: Schema.String,
    attempts: Schema.Number,
    last: ConnectTransientError
  }
) {}
