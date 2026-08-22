# ADR-0003: KV naming strictness

Status: accepted, 2026-08-21. Ruled by the project owner in-session ("strict enforcement").
Evidence: [jetstream-kv-bucket-naming.md](../research/jetstream-kv-bucket-naming.md).

## Decision

1. Runtime enforcement via branded parse-don't-validate schemas at every input boundary.
   Bucket: `^[A-Za-z0-9_-]+$`, max 252 bytes (the server's 255-byte stream cap minus `KV_`).
   Key: `^[A-Za-z0-9_\-/=]+(\.[A-Za-z0-9_\-/=]+)*$` — the one-regex form that cannot be ported
   without its guards.
2. Filters are a separate branded type with the strict token-wise grammar
   (`filter := ">" | token ("." token)* ["." ">"]`, `token := [A-Za-z0-9_\-/=]+ | "*"`),
   deliberately stricter than every reference client because the looser forms fail silently.
3. Bucket names beginning with `KV_` are rejected. Parity-breaking and deliberate: the prefix
   is tested rather than parsed throughout the reference ecosystem, and the research doc
   records the resulting ambiguities (`KV_THINGS`, `KV_KV_THINGS`, bare `KV_`).
4. The empty string is never legal input for any naming type. Whole-bucket watch is expressed
   by omitting the selector (defaulting to `">"`), not by `""`.
5. Non-conformant names read off the wire fail the reading operation with a typed error
   carrying the raw subject. No silent skip, no unbranded escape hatch: a violation observed on
   the read path is surfaced, per the owner's ruling that failing is acceptable there.
6. `_kv` stays advisory — documented on the key type, not rejected — because the spec reserves
   it in prose only, no client or server enforces it, and rejection breaks wire parity with
   every reference client. This is the one place documentation-plus-typing is the enforcement,
   and it is a deliberate carve-out, not a default.
7. No key-length cap yet. The ceiling is protocol-level (control line, connection-closing
   failure mode) and read-path-asymmetric; a cap without the live-server probe would be a
   guess. The probe result amends this ADR.
8. Branded values are constructed only by parsing. Derived values inside the library may skip
   re-checking only when derived from already-parsed values; no input path ever bypasses
   checks.

## Consequences

- The naming module (branded schemas plus one total mapping function per direction) is the
  first implemented slice and an agreed internal test seam; the adversarial corpus in the
  research doc is its test bed.
- Strict-side deviations from reference clients (`..`, `a.>.b`, glued wildcards, `KV_` prefix,
  `""`) are documented behavior differences, each traceable to a research-doc ruling.
