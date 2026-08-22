/**
 * Error taxonomy — docs/01-SPEC.md §8.
 *
 * Every failure that can inhabit an E channel is a `Schema.TaggedError`:
 * typed, serializable, and yieldable. Transport-level failures
 * (`StreamTimeout`, `TransportFailed`) have no protocol code but stay
 * distinguishable from protocol-level ones (`PromptFailed` carries the §9.2
 * status code taxonomy).
 */
import { Schema } from "effect"

/** Transport seam failure: the broker connection or subscription died. */
export class WireFailure extends Schema.TaggedError<WireFailure>()("WireFailure", {
  reason: Schema.String,
}) {}

/** §6.6: no message observed within the inactivity window. */
export class StreamTimeout extends Schema.TaggedError<StreamTimeout>()("StreamTimeout", {
  inactivityMs: Schema.Number,
}) {}

/** §9: the host replied with an error-headered message. */
export class PromptFailed extends Schema.TaggedError<PromptFailed>()("PromptFailed", {
  code: Schema.Number,
  message: Schema.String,
}) {}

/** The peer violated the wire contract (non-JSON chunk, malformed known type…). */
export class ProtocolViolation extends Schema.TaggedError<ProtocolViolation>()(
  "ProtocolViolation",
  { reason: Schema.String },
) {}

/** The transport failed mid-stream (mapped from {@link WireFailure}). */
export class TransportFailed extends Schema.TaggedError<TransportFailed>()("TransportFailed", {
  reason: Schema.String,
}) {}

/** §5.4: local validation failed BEFORE any wire traffic. */
export class LocalViolation extends Schema.TaggedError<LocalViolation>()("LocalViolation", {
  reason: Schema.String,
}) {}

/** `$SRV.INFO` collection failed at the transport level. */
export class DiscoveryFailed extends Schema.TaggedError<DiscoveryFailed>()("DiscoveryFailed", {
  reason: Schema.String,
}) {}

/** Agent-host construction rejected: malformed address fields or configuration. */
export class HostConfigError extends Schema.TaggedError<HostConfigError>()("HostConfigError", {
  reason: Schema.String,
}) {}

/**
 * Handler-declared prompt failure (host side): carries the §9.2 status code
 * relayed to the caller as error headers.
 */
export class AgentFailure extends Schema.TaggedError<AgentFailure>()("AgentFailure", {
  code: Schema.Number,
  message: Schema.String,
}) {}

export type PromptError = StreamTimeout | PromptFailed | ProtocolViolation | TransportFailed

export type PromptCallError = PromptError | LocalViolation
