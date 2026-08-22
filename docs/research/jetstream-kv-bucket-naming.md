# JetStream KV bucket and key naming

## Scope and pins

This map records the naming semantics of JetStream KV buckets and keys: validation grammars, the
bucket-to-stream and key-to-subject mappings, reserved names, length limits, server enforcement,
and the deltas between the specification and every reference implementation. It was produced by a
five-lane parallel research sweep (spec, nats.js, nats-server, cross-client, Effect modeling)
followed by an adversarial cross-check that re-verified every contradiction against primary
sources. "Fact" means the pinned source states or implements it. "Ruling" resolves a
contradiction between sources. "Recommendation" is an inference for this project.

Pins:

- ADR corpus: `nats-io/nats-architecture-and-design@e23afc3f1b0df57a79a81105a4dccc5363b8d655`
  (main, 2026-07-27).
- Official JS client: `nats-io/nats.js@3a7b736b175d9a387130210c88f851d71e1b014f` (the repository
  pin).
- Official Go client: `nats-io/nats.go@db1375fcffae2eb0b4ced1b7bad4d47c4447e4ac`.
- Server: `nats-server` v2.14.5 = `d3bc0453384d00d37991b084263e59af828fa90d` (latest stable at
  survey time).
- Effect: installed `effect@4.0.0-rc.111`, compiled under this repository's tsconfig.

## Where the grammar lives

**Fact.** ADR-8 ("JetStream based Key-Value Stores") contains no bucket-name or key-name
character rule. The normative grammar lives in ADR-6, which ADR-8 never cross-references; an
implementer reading only ADR-8 ships no validation at all. ADR-8's naming content is limited to
the two mapping MUSTs (stream and ingest subject) and the worked examples.
(`ADR-8.md`, all 510 lines; mappings at L238-239; ADR-6 absent from its reference list.)

**Fact.** ADR-6 defines, verbatim: `restricted-term = (A-Z, a-z, 0-9, dash, underscore)+`
(L50); `limited-term = (A-Z, a-z, 0-9, dash, underscore, fwd-slash, equals)+` (L48);
`dot = "."` (L31); `kv-bucket-name = restricted-term` (L64);
`kv-key-name = limited-term (dot limited-term)*` (L63). ADR-20 (L62-63) restates
`restricted-term` for Object Store buckets, confirming the grammar notation reads as a character
class.

**Ruling.** The client regexes are implementations of ADR-6's normative grammar, not mere
convention — a lane that read only ADR-8 concluded otherwise and was overruled by re-fetching
ADR-6. Cite ADR-6 for character sets, ADR-8 for mappings.

## Canonical grammars

**Fact — bucket.** `^[A-Za-z0-9_-]+$`, non-empty. Byte-identical or character-equivalent in all
four reference clients (nats.go `validBucketRe`, nats.js `/^[-\w]+$/` with ASCII-only `\w`,
nats.py, nats.rs) plus the spec — the strongest-attested rule in the corpus. The excluded dot is
load-bearing: it makes `<bucket>` exactly one subject token and the subject-to-key reverse
mapping unambiguous.

**Fact — key.** `^[A-Za-z0-9_\-/=]+(\.[A-Za-z0-9_\-/=]+)*$`. This single anchored regex is the
exact executable form of ADR-6's production and subsumes all four of nats.go's guards at once:
non-empty, no leading dot, no trailing dot, no empty token (`..`). It equals nats.go `keyValid`
and is stricter than nats.py, nats.rs, and nats.js `validateKey`, each of which omits the `..`
guard. Every reference client's raw regex is insufficient alone — the guards do the work — so
porting a regex without its guards is the easiest way to ship the `..` bug. Prefer the one-regex
form because it cannot be split from its guards.

**Fact — keys versus filters are two types.** `*` and `>` are not members of `limited-term`, so
no wildcard may appear in a stored key. ADR-8's own interface encodes the split in parameter
names: `key` for Put/Create/Update/Delete/Purge/Get/History, `keySpec` for Watch/Keys
(ADR-8 L118-157). The validators must not be shared.

**Fact — no filter grammar exists in the spec**, which is why every client invented a different
one and two are demonstrably wrong (below). The safe synthesis, stricter than both reference
clients:

```
filter := ">" | token ("." token)* ["." ">"]
token  := [A-Za-z0-9_\-/=]+ | "*"
```

`*` only as a whole token, `>` only as the final whole token, bare `>` legal, no empty tokens,
no leading or trailing dot. Justification: `foo>`, `a*b`, and `a.>.b` pass one or both reference
clients and pass the server (whose `isValidSubject` special-cases only length-1 tokens, so
`foo>` is a valid subject containing a literal `>`), then match nothing — a silent empty watch
with no error anywhere in the stack. Token-wise validation is the only layer that can catch it.

**Ruling — empty string.** ADR-8 twice asserts an empty key watches the whole bucket
(L143-145, L415-417); nats.go, nats.js, and nats.rs all reject it, and the implementations agree
with each other, so they win for interop. Normalize `""` to `">"` at the public boundary before
validation, or reject it typed; never pass it through.

**Fact — case and encoding.** Names are case-sensitive everywhere and never folded: `Foo` and
`foo` are distinct buckets on distinct streams. The charset is pure ASCII (ADR-6 L26 quotes the
protocol's no-guarantees-for-non-ASCII rule; nats.js's regexes carry no `u` flag; the other
clients spell explicit ASCII ranges), so byte length and character length coincide and any
length bound is unambiguous.

## Mappings

**Fact.** The two naming MUSTs of ADR-8 (L238-239): the main write stream is `KV_<bucket>`; the
ingest subjects are `$KV.<bucket>.>`. All four clients implement both identically.

**Fact.** Key to subject is plain concatenation with no escaping: `$KV.<bucket>.<key>`, the
key's dots becoming subject token separators (ADR-8 L311: key `auth.username` publishes to
`$KV.CONFIGURATION.auth.username`). The reverse map strips the literal prefix `$KV.<bucket>.`
and takes everything remaining, however many tokens, as the key — total and unambiguous only
because bucket names cannot contain a dot.

**Ruling — cross-account and cross-domain.** The API prefix is PREPENDED and `$KV` retained:
`<apiPrefix>.$KV.<bucket>.<key>`, a domain being the prefix `$JS.<domain>.API`. ADR-19's L43
("replaces the first token") describes the JetStream `$JS.API` case only; the server's mapping
table (`$JS.<domain>.API.$KV.>` → `$KV.>`) and an explicit server comment confirm the KV/OBJ
views deliberately append to avoid collisions. The prefix applies to writes only — direct reads
and consumer filter subjects use the bare `$KV.<bucket>.<key>`, and the backing stream is always
created with the unprefixed subjects. Key charset is unaffected by domain or account.

**Ruling — a bucket named like a mapping token is safe.** `DIRECT`, `STREAM`, `INFO` and the
other domain-mapping tokens cannot collide with a bucket name: the token after `API.` in the
mapping key is always the literal `$KV`, and the bucket appears one token later.

## Reserved names and structural hazards

**Fact.** `_kv` is the only reserved key prefix in the entire ADR corpus, reserved by one line
of ADR-6 prose ("Keys starting with `_kv` are limited to internal use", L82-84), enforced by no
client and no server; nats.js's test suite pins `_kv_foo` as a valid key. Treat as advisory —
rejecting it breaks wire parity with every reference client. It is unrelated to `_nats`, which
reserves stream-metadata map keys.

**Fact.** There is no bucket rename, no alias, and no collision rule protecting `KV_<name>` or
`$KV.<name>.>` from a plain non-KV stream. Bind-time KV-shape verification effectively does not
exist upstream: nats.go checks only `MaxMsgsPerSubject >= 1`, and nats.js `bindOnly` fetches no
stream info at all. A real guard — verify subjects equal `$KV.<bucket>.>` plus the KV config
markers at bind — must be built here if wanted.

**Recommendation.** Reject bucket names beginning with `KV_`. `KV_THINGS` is legal under the
grammar and round-trips through one `TrimPrefix`, but both reference clients test the `KV_`
prefix rather than parse it in mirror/source normalization, so `KV_THINGS` as an origin silently
resolves to bucket `THINGS`; and Go's strip-one `TrimPrefix` disagrees with Rust's strip-all
`trim_start_matches` on `KV_KV_THINGS`. The degenerate bucket named exactly `KV_` is worse: as
an origin it resolves to a stream whose derived bucket name is the empty string. This rejection
is stricter than every reference client, therefore parity-breaking, therefore ADR material —
not a free win.

**Fact.** KV and Object Store buckets share the same name grammar and may coexist under one
name with no conflict (disjoint prefixes `KV_`/`OBJ_`, disjoint subject roots). The bucket-name
validator is genuinely shareable across the two; the key validator is not (Object Store object
names are arbitrary bytes, base64-encoded).

## Limits

**Fact.** The server caps stream names at `JSMaxNameLen = 255` bytes
(`server/jetstream_api.go:354-363`), applied to the derived `KV_<bucket>`, so the effective
bucket-name cap is 252 bytes. No ADR performs this subtraction and no client enforces any length
at all — an over-long bucket surfaces as stream error 10052 ("stream name is too long, maximum
allowed is 255"), never as a naming error. Enforce 252 client-side to produce a good error.

**Fact.** No constant bounds subject byte length or token count. Key length is bounded only by
the protocol control line (`MAX_CONTROL_LINE_SIZE = 4096`, configurable), and exceeding it does
not return an API error — the server sends `-ERR 'maximum control line exceeded'` and closes
the connection. Worse, the read path is longer than the write path: subject-appended direct get
is `$JS.API.DIRECT.GET.KV_<b>.$KV.<b>.<k>`, carrying the bucket twice — 23 + len(bucket) bytes
longer than the write subject — so a key can be writable but not directly readable. Any
client-side key-length bound must budget against the read path. The practical ceiling and
failure mode need a live-server probe.

## Server enforcement versus client validation

**Fact.** The server validates stream names with `isValidAssetName`
(`server/jetstream.go:2787-2792`): rejects empty and exactly ten characters — space, tab, CR,
LF, form feed, `.`, `*`, `>`, `\`, `/`. That is far looser than `restricted-term`: `KV_a$b%c`
is a server-legal stream. Subject validation is looser still. Consequence: KV key validation is
purely client-side. Two clients with different validators can write keys the other cannot read,
and non-conformant keys are reachable by design — via ADR-57 non-KV sources with subject
transforms, via a lax client, or via a direct core-NATS publish to `$KV.<bucket>.<key>` (the
server does not validate publish subjects for non-pedantic connections).

**Fact.** The server joins the stream name verbatim into a filesystem path with no escaping
(`filepath.Join(storeDir, streamsDir, cfg.Name)`), while treating `KV_foo` and `KV_FOO` as
distinct streams. On a case-insensitive filesystem (NTFS, APFS) the two collide on one
directory; no server guard was found. This repository's environment is Windows — a live probe
is required before any test relies on case-distinct buckets. Windows reserved device names
(`CON`, `NUL`, …) yield streams like `KV_CON` and are probably safe, but unconfirmed.

**Fact.** Bucket re-creation with any differing config returns err_code 10058 (code 400) from a
deep-equal over post-defaulting configs, so idempotent create requires reproducing every server
default exactly. Which KV fields are legally updatable on a live bucket is unanswered and is
ADR material for the Create/Update/CreateOrUpdate surface.

## Operation identity travels in headers, never in names

**Fact.** Deletes and purges are messages on the same `$KV.<bucket>.<key>` subject carrying
`KV-Operation: DEL | PURGE` (plus `Nats-Rollup: sub` for purge); 2.11+ limit markers arrive on
the same subject with `Nats-Marker-Reason: MaxAge|Purge|Remove`. There is no reserved marker
key, marker subject, or tombstone naming convention — watch and key iteration must classify by
header, never by name shape. Limit-marker TTL minimum is exactly 1 second (server-authoritative:
"subject delete marker TTL must be at least 1 second"; ADR-8's "longer than a second" prose is
loose).

## Reference-client defects confirmed by the sweep

Each was verified verbatim in both sources and ruled by the cross-check; none should be ported.

| Defect                                                                                                                                        | Ruling                                                                                                                                        |
| --------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| nats.js `validateKey` accepts `a..b` (no `..` guard; its own `validateSearchKey` has one)                                                     | ADR-6 forbids empty tokens; nats.go is the only conformant client                                                                             |
| nats.js `validSearchKey` accepts `a.>.b`; its correct structural checker `hasWildcards` is wired only into delete/purge, not watch/keys       | non-terminal `>` is invalid; reject                                                                                                           |
| Both reference clients accept wildcard chars glued in literal tokens (`foo>`, `a*b`); server accepts them as literal subjects                 | matches nothing, fails silently; validate token-wise                                                                                          |
| nats.js sources branch uses `!external && names-differ` where nats.go uses `external == nil \|\| names-differ` to generate subject transforms | nats.go is correct; nats.js has a real cross-account sourcing bug (external-sourced entries land under the wrong prefix and become invisible) |
| nats.js unconditionally rewrites caller-supplied source names/transforms, violating ADR-57 rule 3 (nats.go honors it with an early-out)       | non-KV-stream sourcing is inexpressible in nats.js; follow nats.go if sources are ever modeled                                                |
| ADR-8 says empty key watches the bucket; every client rejects `""`                                                                            | implementations win; normalize or reject at the boundary                                                                                      |

## Effect 4 modeling notes (rc.111, compiled under this repo's tsconfig)

**Fact.** String constraints in this rc are `is*` filter functions attached with
`Schema.check(...)`, not schemas; there is no `Schema.filter` — custom predicates use
`Schema.makeFilter`. `Schema.brand` adds no runtime check. The canonical shape is
`Schema.String.pipe(Schema.check(Schema.isPattern(...), Schema.makeFilter(...)), Schema.brand("KvBucketName"))`
with a type alias.

**Fact — delta against the architecture research doc.** The interface pattern recommended there
(`export interface Subject extends Schema.Schema.Type<typeof Subject> {}`) does not compile in
this rc (TS2312), and `Schema.Opaque` fails on scalar string schemas (TS2509). Use type
aliases. `Schema.Union` takes an array; the variadic form crashes at runtime.

**Fact.** Checks run on encode as well as decode, so a branded naming type is validated in both
directions. `Schema.decodeUnknownEffect` fails with `SchemaError` in the typed channel;
per-schema `make` throws, `makeOption` returns `Option`, `makeEffect` fails with
`SchemaIssue.Issue`. Unannotated pattern checks embed the regex source verbatim in user-facing
messages, and `reportInput: true` embeds the rejected value — annotate messages and keep input
reporting off at the naming boundary. `Schema.toArbitrary` honors both pattern and custom
filters, giving property-test generators over the grammars for free.

**Fact.** Naming failures surface as a `Schema.TaggedError`-based typed reason per the style
contract, not a raw `SchemaError`.

## Decisions this research feeds

1. Validation strictness (scoping open decision 2): adopt the canonical bucket and key grammars
   (4/4 client + spec agreement), the strict synthesized filter grammar (deliberately stricter
   than both reference clients, justified by the silent-empty-watch trap), the 252-byte bucket
   cap, and `""`-to-`">"` normalization. Separately decide the parity-breaking `KV_`-prefix
   rejection.
2. Non-conformant keys read off the wire — the highest-impact Effect-specific question. A
   branded parse-don't-validate `KvKey` makes the read path fallible in a way the reference
   clients' plain strings never are, and non-conformant keys are reachable by design. Skip the
   entry, surface a raw unbranded variant, or fail the stream: ADR required. The encode-side
   check interaction sharpens it: a non-conformant key re-encoded into a subject fails far from
   its cause, so decide where already-parsed values may bypass checks (never on input paths).
3. Create/Update/CreateOrUpdate surface and idempotent-create defaults (10058 deep-equal).
4. Mirror naming is specified three incompatible ways across ADR-57/58/59 (all Proposed) —
   deferrable because the slice excludes mirrors, and a reason to keep excluding them.

Live-server probes required before the naming module's tests can claim server parity: practical
key-length ceiling and its connection-closing failure mode; read-path length budget;
case-colliding buckets on a Windows-hosted server; whether `$KV.B.a%b` written by a foreign
client is observable through a strict watch. Upstream issue worth filing: ADR-8 should
cross-reference ADR-6, and ADR-6 should gain a `kv-key-filter` production — its absence is why
every client invented a different, and in two cases wrong, filter validator.
