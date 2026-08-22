/**
 * Subject algebra — mirrors `EffectNats.Subject` in the proved companion
 * package (github.com/mepuka/effect-nats-lean).
 *
 * Subjects are CLOSED-WORLD (notes/0005): publishable subjects are concrete
 * token lists, validated at the TYPE level (`WfToken`) and at runtime by the
 * same rules. Wildcards (`*`, `>`) are consume-only and unrepresentable in a
 * well-formed subject. A subject's dotted name is computed in the type system
 * (`Join`) so downstream phantom requirements can reference it literally.
 */
declare const SubjectBrand: unique symbol

/**
 * Type-level well-formedness: nonempty, no dots, no wildcards.
 * A non-literal `string` yields `boolean` — statically unknowable, so callers
 * get the honest runtime union instead of a laundered success type.
 */
export type WfToken<S extends string> = string extends S
  ? boolean
  : S extends ""
    ? false
    : S extends `${string}.${string}`
      ? false
      : S extends "*" | ">"
        ? false
        : true

export type AllWf<Tokens> = Tokens extends readonly string[]
  ? number extends Tokens["length"]
    ? boolean // dynamic-length array: validity is a runtime fact
    : Tokens extends readonly [infer H extends string, ...infer Rest extends readonly string[]]
      ? WfToken<H> extends false
        ? false
        : WfToken<H> extends true
          ? AllWf<Rest>
          : boolean
      : true
  : false

/** `["a","b","c"] → "a.b.c"` in the type system. */
export type Join<Tokens extends readonly string[]> = Tokens extends []
  ? ""
  : Tokens extends readonly [infer Only]
    ? Only extends string
      ? Only
      : never
    : Tokens extends readonly [infer H, ...infer Rest]
      ? H extends string
        ? Rest extends readonly string[]
          ? `${H}.${Join<Rest>}`
          : never
        : never
      : string

export interface Subject<Tokens extends readonly string[] = []> {
  readonly tokens: Tokens
  readonly name: Join<Tokens>
  readonly [SubjectBrand]: "Subject"
}

const isWfToken = (t: string): boolean =>
  t.length > 0 && !t.includes(".") && t !== "*" && t !== ">"

/**
 * Result type of {@link mkSubject}: refined for validated literals, the honest
 * union when the tokens are dynamic (`AllWf` = `boolean`).
 */
export type MkResult<Tokens extends readonly string[]> = [AllWf<Tokens>] extends [true]
  ? Subject<Tokens>
  : [AllWf<Tokens>] extends [false]
    ? null
    : Subject<Tokens> | null

/** Smart constructor: rejects empty tokens, dots, and wildcards. */
export const mkSubject = <const Tokens extends readonly string[]>(
  tokens: Tokens,
): MkResult<Tokens> =>
  (tokens.every(isWfToken)
    ? ({ tokens, name: tokens.join(".") } as unknown as Subject<Tokens>)
    : null) as MkResult<Tokens>

export const renderSubject = <Tokens extends readonly string[]>(s: Subject<Tokens>): string =>
  s.name

// ---------------------------------------------------------------------------
// Patterns (subscription side — wildcards live here)
// ---------------------------------------------------------------------------

export type PTok =
  | { readonly _tag: "lit"; readonly value: string }
  | { readonly _tag: "star" }

export interface Pattern {
  /** Literal/star tokens; a trailing `>` is carried by `gt` so a mid-pattern `>` is unrepresentable. */
  readonly toks: readonly PTok[]
  readonly gt: boolean
}

/**
 * Interest matching: `*` matches exactly one token; final `>` matches one or
 * more remaining tokens. Index-based single pass: O(n) time, zero allocation
 * (the recursive `slice`-per-token formulation was O(n²) in both).
 */
export const matchSubject = (
  subject: readonly string[],
  pattern: Pattern,
): boolean => {
  const { toks, gt } = pattern
  if (subject.length < toks.length) return false
  for (let i = 0; i < toks.length; i++) {
    const p = toks[i]!
    if (p._tag === "lit" && p.value !== subject[i]) return false
  }
  return gt ? subject.length > toks.length : subject.length === toks.length
}
