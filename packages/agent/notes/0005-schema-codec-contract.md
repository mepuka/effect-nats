# 0005 — Pass A contract: Schema-as-codec (slice 2)

> **Superseded by the simple-effects model.** The recursive `SchemaNode`
> representation below was attempted and rejected: its unconditional theorem
> depended on hidden canonical-order, unique-field, and reserved-`_tag`
> invariants. The current `lean/EffectNats/Codec.lean` instead exposes two
> reified requests, `CodecEffect.encode` and `CodecEffect.decode`, interpreted
> through a `Codec α`. A codec must prove its successful encode/decode law when
> it is constructed. Concrete Effect Schema/JSON agreement is now a later
> refinement and deployment-test obligation, not a claim of the core effect
> model. The material below is retained as the superseded Pass A record.

Produced by `lean-formalization-strategy` (Pass A). Target module: `lean/EffectNats/Codec.lean`
(+ supporting `Json.lean`). Handoff at end goes to `$lean-model-invariants`.

## 1. Domain contract

**Intent.** Weld NATS JetStream to Effect Schema by making the wire message contract explicit:
a small reified schema AST with two interpreters (`decode`, `encode`), where encoding is decoding
after flipping the transformation chain (per the effect-v4 schema-deep brief). Round-trip holds
exactly when every link is an iso — that conditional law IS the headline theorem.

**Objects.**

- `Json` — wire carrier inductive: `Null | Bool | Num | Str | Arr (List Json) | Obj (assoc list)`
- `SchemaNode` — AST subset of Effect v4's 21-node union:
  `Lit` (string literal / `_tag` values) · `Bool` · `Str` · `Num` · `Struct` (fields: name × node ×
  optional flag, insertion order preserved) · `TaggedUnion` (non-empty alternatives, each a Struct
  whose first field is its `_tag` literal) · `List_` (element node) · `Suspend` (thunk; see OQ-2)
- `DecodePath` — structured failure location: list of steps (`field name` / `index n` / `tag k`),
  so errors are actionable, not just inhabited.

**Operations.**

- `decode : SchemaNode → Json → Result A DecodePath` (total; `A` = semantic value domain)
- `flip : SchemaNode → SchemaNode` (mirror the transformation chain; here: swap struct-field
  optionality handling sides)
- `encode : SchemaNode → A → Result Json DecodePath` — defined as `decode ∘ flip` (the brief's
  identity, made definitional)
- `isIso : SchemaNode → Prop` — every link is lossless (structurally decidable for our subset;
  no closure-bearing transformations in slice 2, so everything is an iso except… nothing yet:
  see OQ-1)

**Observables & equivalences.** Success/failure of decode; equality of decoded values
(`DecidableEq` on the value domain); `DecodePath` equality for error tests.

**Scope.** Slice 2 says nothing about transport, grades, subjects, or who observes failures.
Those are slices 3–4 (subjects closed-world registry-derived; wildcards consume-only — ratified
in session; consumer algebra inherits `DecodeError` as an entry in the error channel, never a
silent skip).

## 2. Examples

Positive:

- `Struct{a: Str, b: optional Num}` decodes `{"a":"x","b":1}` and `{"a":"x"}`
- `TaggedUnion{Circle(r: Num), Quit}` decodes `{"_tag":"Quit"}` → `.quit`,
  rejects `{"_tag":"Nope"}` with path `[tag]`
- `List_(Str)` decodes `["p","q"]`

Forbidden:

- decoding `{"b":1}` (missing required `a`) fails with path `[field a]`
- decoding `"hello"` against `Struct{...}` fails at root
- duplicate `_tag` in a TaggedUnion — rejected **at construction**, not decode time
  (rests on Subject.exact_singleton discipline; construction returns `Option SchemaNode`)

Counterexample to the strongest tempting overclaim — *"schemas always round-trip"*:
once any lossy transformation exists (Effect's `DateTime` string↔object,
`optionalOption` missing-key↔none), `decode ∘ encode ≠ id`. Slice 2 keeps such nodes out of
`SchemaNode`, so `isIso` is vacuously total — but the THEOREM must still be conditional
(`isIso s → …`) so adding a lossy node later strengthens nothing silently.

Edge cases: empty struct `{}` ↔ `{}`; empty list; deeply nested paths accumulate left-to-right.

## 3. Assumptions / obligations split

| Class | Item |
|---|---|
| Domain assumption (trusted) | `Json.Num` adequacy for IEEE doubles is irrelevant below (we compare structurally); no float-equality laws |
| Fact to prove | `roundTripEncDec : isIso s → ∀ a, decode s (encode s a) = pure a` |
| Fact to prove | `roundTripDecEnc` restricted to decoded values: `∀ j, decode s j = pure a → encode s a = pure j` (needs injectivity of decode — check feasibility at Pass B) |
| Fact to prove | `flip` involutive: `flip (flip s) = s` |
| Deployment test (not theorem) | TS codec ⟷ Lean interpreter agreement on generated vectors (carried over from note 0003) |

## 4. Prior-art ledger

| Source | Revision | Guarantee | Mismatch | Class |
|---|---|---|---|---|
| effect-v4 schema-deep / variants briefs (Foldable research/briefs) | 2026-08-20 | Spec-level description of AST, `rebuild`, flip-based encoding | We drop closures/middleware entirely | pattern |
| `EffectNats.Subject.lean` (this repo, proved) | current | token/refines order, collision-freedom | none | reuse |
| `EffectNats.Guarantee.lean` / `Graded.lean` | current | grade ladder + graded Prog | orthogonal axis to slice 2 | reuse (slice 4) |
| Effect `@effect/schema` itself | 4.x | ground truth for TS side | not formalized; trust surface = tsc gates | adapt (bridge obligation) |

From scratch for the AST itself: materially smaller semantics than the real 21-node union.

## 5. Declaration DAG (signatures only)

```
Json                      (inductive)
SchemaNode                (inductive; constructors above; smart constructor for TaggedUnion)
Value                     (semantic domain; mirrors SchemaNode structure)
DecodePath                (inductive; Repr, DecidableEq)
decode      : SchemaNode → Json → Result Value DecodePath
flip        : SchemaNode → SchemaNode
encode      : SchemaNode → Value → Result Json DecodePath   := decode ∘ flip
isIso       : SchemaNode → Prop
---
flip_invol  : ∀ s, flip (flip s) = s
rt_ed       : isIso s → ∀ a, decode s (encode s a) = pure a     -- headline
rt_de       : ∀ s j a, decode s j = pure a → encode s a = pure j -- pending feasibility
```

Dependency order: Json → DecodePath → SchemaNode/Value → decode → flip → encode → theorems.

## 6. Open representation questions (for $lean-model-invariants)

- **OQ-1:** does slice 2 admit ANY non-iso node? Candidate: `optional` fields already break
  naive `decode∘encode` on absent keys unless encoded-side emits explicit `null`. Decide whether
  optionality lives in the node (encoded side always writes the key) — recommended — or in a
  transformation layer (imports the lossy-link machinery early).
- **OQ-2:** `Suspend`: include the constructor but EXCLUDE suspension from theorem scope
  (termination needs fuel; Effect memoizes first evaluation). Recursive round-trip = named
  refinement obligation, not silently assumed away. Alternative: cut Suspend from slice 2.
- **OQ-3:** value domain `Value` as separate inductive vs reusing `Json` as both carrier and
  semantic domain. Separate gives honest type distinction; shared halves the definitions.
- **OQ-4:** `Obj` as assoc list preserves field order (matches Effect insertion order) but makes
  field lookup O(n) in proofs. ListMap lemmas needed either way.

## 7. Pending (recorded, out of scope)

- Routing helpers (HttpApi-shaped route tables → subjects): users get ergonomics, registry stays
  closed-world underneath. Future slice; do not let it reopen subject derivation.
- `layerKv(schema)` typed KV handles; CAS conflicts as typed errors (note 0004 P2). KV put is
  `jsPublishDedup`; inherits `dedupWindow` rung.
- Effectable-primitive domain objects (runnable `Subject<T>` wrappers): heavy machinery, deferred
  until something concrete wants it.
