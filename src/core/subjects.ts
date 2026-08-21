/**
 * Subject algebra — mirrors `lean/EffectNats/Subject.lean`.
 *
 * Closed-world policy (notes/0005): publishable subjects are CONCRETE token
 * lists; wildcards (`*`, `>`) are consume-only and unrepresentable in
 * `Subject`. The registry (service side) maps schemas to subjects and rejects
 * collisions — the runtime counterpart of Lean's proved `exact_singleton`.
 */
declare const SubjectBrand: unique symbol

/** A validated, wildcard-free subject as a token list. */
export type Subject = readonly string[] & { readonly [SubjectBrand]: "Subject" }

const isWfToken = (t: string): boolean =>
  t.length > 0 && !t.includes(".") && t !== "*" && t !== ">"

/** Smart constructor: rejects empty tokens, dots, and wildcards. */
export const mkSubject = (tokens: readonly string[]): Subject | null =>
  tokens.every(isWfToken) ? (tokens as Subject) : null

export const renderSubject = (s: Subject): string => s.join(".")

// ---------------------------------------------------------------------------
// Patterns (subscription side — wildcards live here)
// ---------------------------------------------------------------------------

export type PTok = { readonly _tag: "lit"; readonly value: string } | { readonly _tag: "star" }

export interface Pattern {
  /** Literal/star tokens; a trailing `>` is carried by `gt` (Lean's invariant). */
  readonly toks: readonly PTok[]
  readonly gt: boolean
}

const tokMatches = (p: PTok, t: string): boolean =>
  p._tag === "star" || p.value === t

/**
 * Interest matching: `*` matches exactly one token; final `>` matches one or
 * more remaining tokens.
 */
export const matchSubject = (
  subject: readonly string[],
  pattern: Pattern,
): boolean => {
  const { toks } = pattern
  if (toks.length === 0) return subject.length === 0 ? !pattern.gt : pattern.gt
  if (subject.length === 0) return false
  const [pTok, pRest] = [toks[0], toks.slice(1)]
  const [sTok, sRest] = [subject[0], subject.slice(1)]
  if (pTok === undefined || sTok === undefined) return false
  return tokMatches(pTok, sTok)
    ? matchSubject(sRest, { toks: pRest, gt: pattern.gt })
    : false
}
