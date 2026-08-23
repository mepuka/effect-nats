# Synadia Agent Protocol Adapter

This context is an independent Effect implementation of the externally owned
[Synadia Agent Protocol for NATS](https://github.com/synadia-ai/synadia-agent-sdk-docs). The
upstream specification owns protocol terms and wire semantics; this repository owns only its
adapter API, implementation, tests, and separately named extensions.

## Language

**Synadia Agent Protocol for NATS**:
The external wire contract for identifying, discovering, and communicating with AI agents over
NATS.
_Avoid_: Our protocol, Effect Agent Protocol, effect-nats protocol

**Caller**:
The protocol role that discovers agents, validates advertised endpoint capabilities, publishes
prompts, and consumes reply streams.
_Avoid_: Client when the protocol role—not a concrete software object—is meant

**Agent host**:
The protocol role that registers and serves an agent as the NATS micro service named `agents`.
_Avoid_: Server, Agent Protocol runtime

**Agent**:
The logical AI agent exposed through the protocol. An agent is not the caller SDK, host SDK,
transport connection, or a running service instance.

**Harness/runtime identifier (`metadata.agent`)**:
The canonical identifier for the harness or runtime exposing the agent. Its subject token may be an
abbreviation.
_Avoid_: Instance ID, agent name

**Owner (`metadata.owner`)**:
The operator or account namespace associated with the agent.

**Instance name (`name`)**:
The fifth token in the default endpoint subject, scoped within the agent/owner tuple. It is not
echoed in protocol metadata.
_Avoid_: Session, instance ID

**Session (`metadata.session`)**:
An optional harness-specific session label required for session-aware harnesses. It is not the
subject instance name or the running service instance ID.
_Avoid_: Instance name, instance ID

**Service instance ID (`instance_id`)**:
The NATS micro framework's identifier for one running service instance, also carried by heartbeats.
_Avoid_: Agent identity, instance name, session

**Endpoint**:
A named NATS micro service endpoint advertised by discovery. The protocol fixes endpoint names;
callers use advertised subjects verbatim except for the protocol-fixed heartbeat subject.

**Agent info**:
A validated discovery record containing protocol metadata, the service instance ID, and advertised
endpoints.
_Avoid_: Agent identity, when referring to the full discovery record

**Wire-contract model**:
Local codecs, validators, subject parsing, and reply-sequence checking derived from the upstream
protocol.
_Avoid_: Protocol kernel, our protocol model

**Local extension protocol**:
An opt-in contract owned by this repository, negotiated separately and kept outside the protocol's
reserved namespace. It is never evidence of Synadia Agent Protocol conformance.
_Avoid_: Protocol feature, protocol improvement, protocol-compatible mode

**Effect AI model**:
An implementation of Effect v4's `LanguageModel` capability. It may be local or provider-backed and
is configured independently from NATS transport and Agent Protocol registration.
_Avoid_: Agent, agent host

**Effect AI host adapter**:
The local adapter that serves an Effect AI model through the protocol's agent-host role. It maps
protocol prompt requests into model input and model output into protocol reply chunks.

**Remote agent model adapter**:
The local adapter that presents one discovered remote agent as an Effect AI model. It remains a
protocol caller underneath; it does not turn the remote agent into an in-process model.

**Prompt mapping policy**:
The explicit rule for translating between Effect AI's structured prompt model and the protocol's
text-plus-attachments request envelope. A mapping may be strict or intentionally lossy; loss must
never be implicit.
_Avoid_: Wire codec, because neither side is the other's encoded representation
