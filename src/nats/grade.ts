/**
 * Delivery-guarantee ladder — mirrors `lean/EffectNats/Guarantee.lean`.
 *
 * The carrier matches the Lean inductive one-to-one so the two sides referee
 * the same encoding. `dedup-window` names the window-bounded dedup mechanism;
 * it does NOT claim exactly-once.
 */
export type Guarantee = "atMostOnce" | "atLeastOnce" | "dedupWindow" | "pure"

const rank: Readonly<Record<Guarantee, number>> = {
  atMostOnce: 0,
  atLeastOnce: 1,
  dedupWindow: 2,
  pure: 3,
}

/**
 * Sequential composition carries the MEET: a pipeline is only as strong as
 * its weakest delivery step. One fire-and-forget publish drags the whole
 * composition to the bottom rung — this is what makes "someone
 * fire-and-forgot inside my pipeline" visible instead of silent.
 */
export const meet = (a: Guarantee, b: Guarantee): Guarantee =>
  rank[a] <= rank[b] ? a : b

export const leq = (a: Guarantee, b: Guarantee): boolean => rank[a] <= rank[b]
