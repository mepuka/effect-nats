/**
 * Closed-world subject registry (notes/0005 §1, ratified).
 *
 * Subjects don't exist unless derived from a registered schema. The registry
 * owns the schema → subject map and rejects collisions at registration time —
 * the runtime counterpart of Lean's proved `exact_singleton`: an all-literal
 * pattern matches exactly its own subject, so two schemas claiming one
 * subject would be indistinguishable on the wire.
 */
import type { Schema } from "effect"
import { mkSubject, type AllWf, type Subject } from "./subjects.js"

export type RegistryError =
  | { readonly _tag: "collision"; readonly subject: string }
  | { readonly _tag: "badSubject"; readonly tokens: readonly string[] }

export class Registry {
  private readonly bySubject = new Map<string, Schema.Constraint>()

  /**
   * Register a schema under a concrete subject. Returns an error on collision
   * or malformed subject — call sites must handle it; the registry never
   * launders invalid states into valid-looking ones.
   */
  register<S extends Schema.Constraint>(
    tokens: readonly string[],
    schema: S,
  ): RegistryError | null {
    const subject = mkSubject(tokens)
    if (subject === null) return { _tag: "badSubject", tokens }
    if (this.bySubject.has(subject.name)) return { _tag: "collision", subject: subject.name }
    this.bySubject.set(subject.name, schema)
    return null
  }

  /** The registered schema for a wire subject, if the subject exists at all. */
  resolve(name: string): Schema.Constraint | undefined {
    return this.bySubject.get(name)
  }

  get size(): number {
    return this.bySubject.size
  }
}

declare const RegisteredBrand: unique symbol

/** Typed publish target: only subjects this registry issued satisfy this. */
export type RegisteredSubject<Tokens extends readonly string[] = []> = Subject<Tokens> & {
  readonly [RegisteredBrand]: "Registered"
}

/**
 * Refined success type only for validated literal tuples; dynamic token
 * arrays (`AllWf` = `boolean`) get the honest runtime union — registration
 * of unchecked input can always fail, and the types must say so.
 */
export type IssueResult<Tokens extends readonly string[]> = [AllWf<Tokens>] extends [true]
  ? RegisteredSubject<Tokens>
  : [AllWf<Tokens>] extends [false]
    ? RegistryError
    : RegisteredSubject<Tokens> | RegistryError

/** Register and, on success, get back the typed publish target. */
export const issue = <const Tokens extends readonly string[]>(
  registry: Registry,
  tokens: Tokens,
  schema: Schema.Constraint,
): IssueResult<Tokens> => {
  const err = registry.register(tokens, schema)
  if (err !== null) return err as IssueResult<Tokens>
  return mkSubject(tokens) as unknown as IssueResult<Tokens>
}
