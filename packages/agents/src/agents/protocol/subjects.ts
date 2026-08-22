/**
 * Protocol-derived subject handling — docs/01-SPEC.md §1, upstream Protocol §2.
 *
 * Verb-first default subject tree: `agents.{verb}.{agent}.{owner}.{name}`.
 * Pure: no I/O. Wildcard matching is the single shared linear matcher from
 * `core/subjects.ts` (`*` = exactly one token, trailing `>` = one or more;
 * proved in github.com/mepuka/effect-nats-lean).
 *
 * Construction discipline (docs/01-SPEC.md §1): endpoint subjects are LEARNED
 * from `$SRV.INFO`, never constructed by callers. The only legal construction
 * paths are the protocol-fixed heartbeat tree and a HOST building its own
 * default endpoints — both validated here.
 */
import {
  matchSubject as matchCoreSubject,
  type Pattern,
  type PTok,
} from "../../core/subjects.js"

export type { Pattern, PTok }

export const isConcreteSubjectToken = (token: string): boolean =>
  token.length > 0 && !/[.*>\s]/u.test(token)

/** A publishable subject: one-or-more concrete tokens, no wildcards. */
export const isConcreteSubject = (subject: string): boolean =>
  subject.length > 0 && subject.split(".").every(isConcreteSubjectToken)

export const matchSubject = (
  subject: readonly string[],
  pattern: Pattern,
): boolean =>
  subject.every(isConcreteSubjectToken) &&
  pattern.toks.every((token) =>
    token._tag === "star" || isConcreteSubjectToken(token.value)
  ) &&
  matchCoreSubject(subject, pattern)

export const VERBS = ["prompt", "hb", "status", "attachments"] as const
export type Verb = (typeof VERBS)[number]

/**
 * Protocol §2.2 + NATS subject rules: nonempty, no `$` prefix, no dot, no
 * wildcard, and no whitespace/control characters (NATS forbids them in
 * subject tokens).
 */
export const isToken = (t: string): boolean => {
  if (t.length === 0 || t.startsWith("$") || t === "*" || t === ">") return false
  for (let i = 0; i < t.length; i++) {
    const c = t.charCodeAt(i)
    if (c <= 0x20 || c === 0x2e || c === 0x7f) return false // ws/ctl, ".", DEL
  }
  return true
}

/** Protocol §2.2 SHOULD-level charset: `a-z 0-9 - _`, 1–63 chars. */
export const isConventionalToken = (t: string): boolean =>
  /^[a-z0-9_-]{1,63}$/.test(t)

export interface AgentSubject {
  readonly verb: Verb
  readonly agent: string
  readonly owner: string
  readonly name: string
}

/**
 * Parse a fully-qualified protocol subject. Returns null unless it has the
 * exact 5-token shape with a known verb (Protocol §2).
 */
export const parseAgentSubject = (subject: string): AgentSubject | null => {
  const [root, verbRaw, agent, owner, name, ...rest] = subject.split(".")
  if (root !== "agents" || rest.length > 0) return null
  const verb = VERBS.find((v) => v === verbRaw)
  if (verb === undefined) return null
  if (agent === undefined || owner === undefined || name === undefined) return null
  if (!isToken(agent) || !isToken(owner) || !isToken(name)) return null
  return { verb, agent, owner, name }
}

/**
 * Host-side default endpoint subject (Protocol §2 table). Validates every
 * identity token; returns null rather than a malformed subject. Callers must
 * NOT use this for learned endpoints — those come from `$SRV.INFO` verbatim.
 */
export const hostDefaultSubject = (
  verb: Verb,
  agent: string,
  owner: string,
  name: string,
): string | null =>
  isToken(agent) && isToken(owner) && isToken(name)
    ? `agents.${verb}.${agent}.${owner}.${name}`
    : null

/**
 * The one construction path callers may use: the protocol-fixed heartbeat
 * subject for a known identity (Protocol §2).
 */
export const heartbeatSubject = (agent: string, owner: string, name: string): string | null =>
  hostDefaultSubject("hb", agent, owner, name)

/** The one protocol-fixed wildcard: every heartbeat (Protocol §2). */
export const HEARTBEAT_WILDCARD = "agents.hb.*.*.*"

/** `agents.hb.*.*.*` as a matcher pattern. */
export const HEARTBEAT_PATTERN: Pattern = {
  toks: [
    { _tag: "lit", value: "agents" },
    { _tag: "lit", value: "hb" },
    { _tag: "star" },
    { _tag: "star" },
    { _tag: "star" },
  ],
  gt: false,
}

export const literalPattern = (subject: string): Pattern => ({
  toks: subject.split(".").map((value) => ({ _tag: "lit", value })),
  gt: false,
})
