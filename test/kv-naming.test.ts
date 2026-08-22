import { expect, it } from "@effect/vitest"
import { Result, Schema } from "effect"
import { FastCheck } from "effect/testing"
import {
  bucketFromStream,
  keyFromSubject,
  KvBucketName,
  KvKey,
  KvKeyFilter,
  streamForBucket,
  subjectForKey,
  subjectPrefix
} from "../src/internal/kv/naming.ts"

const parseBucket = Schema.decodeUnknownResult(KvBucketName)
const parseKey = Schema.decodeUnknownResult(KvKey)
const parseFilter = Schema.decodeUnknownResult(KvKeyFilter)

const bucket = (name: string): KvBucketName => Result.getOrThrow(parseBucket(name))
const key = (name: string): KvKey => Result.getOrThrow(parseKey(name))

// Corpus sources: docs/research/jetstream-kv-bucket-naming.md (canonical grammars,
// edge-case inventory) and docs/architecture/adr-0003-kv-naming-strictness.md.

it("accepts canonical bucket names", () => {
  const valid = ["config", "Foo", "foo", "a", "A-1_b", "0", "_", "-", "a".repeat(252)]
  for (const input of valid) {
    expect(Result.isSuccess(parseBucket(input)), `bucket ${JSON.stringify(input)}`).toBe(true)
  }
})

it("rejects non-conformant bucket names", () => {
  const invalid = [
    "", // empty: restricted-term requires one or more characters
    "a.b", // dot would split the bucket across subject tokens
    "a b",
    "a/b", // legal in keys, illegal in buckets
    "a=b",
    "a*b",
    "a>b",
    "$KV",
    "café", // ASCII only
    "a".repeat(253), // 255-byte stream cap minus "KV_"
    "KV_things", // ADR-0003: reserved stream prefix
    "KV_" // degenerate reserved-prefix case
  ]
  for (const input of invalid) {
    expect(Result.isFailure(parseBucket(input)), `bucket ${JSON.stringify(input)}`).toBe(true)
  }
})

it("accepts canonical keys, including the advisory _kv prefix", () => {
  const valid = [
    "a",
    "A",
    "auth.username",
    "a-b/c=d",
    "a.b.c",
    "/",
    "=",
    "_kv_foo",
    "_kv.internal",
    "0.1.2"
  ]
  for (const input of valid) {
    expect(Result.isSuccess(parseKey(input)), `key ${JSON.stringify(input)}`).toBe(true)
  }
})

it("rejects non-conformant keys", () => {
  const invalid = [
    "", // empty
    ".", // lone dot
    "..",
    ".a", // leading dot
    "a.", // trailing dot
    "a..b", // empty token — the guard three of four reference clients forgot
    "*", // wildcards are filter syntax, never key syntax
    ">",
    "a.*",
    "a.>",
    "a.*.b",
    "foo>", // glued wildcard: server-legal literal subject that matches nothing
    "a*b",
    "a b",
    "a$b", // server-legal subject characters outside limited-term
    "a%b",
    "café"
  ]
  for (const input of invalid) {
    expect(Result.isFailure(parseKey(input)), `key ${JSON.stringify(input)}`).toBe(true)
  }
})

it("accepts token-wise filters", () => {
  const valid = [">", "*", "a", "a.>", "a.*", "*.b", "a.*.b", "auth.>", "a.b.c", "*.*", "_kv.>"]
  for (const input of valid) {
    expect(Result.isSuccess(parseFilter(input)), `filter ${JSON.stringify(input)}`).toBe(true)
  }
})

it("rejects the filter shapes that fail silently in reference clients", () => {
  const invalid = [
    "", // ADR-0003: whole-bucket watch is expressed by omitting the selector
    "a.>.b", // non-terminal > — accepted by nats.js, matches nothing
    ">.a",
    "foo>", // glued wildcards — accepted by nats.go and nats.js, match nothing
    "a*b",
    "*>",
    "a.>b",
    ".a",
    "a.",
    "a..b",
    "a b",
    "café"
  ]
  for (const input of invalid) {
    expect(Result.isFailure(parseFilter(input)), `filter ${JSON.stringify(input)}`).toBe(true)
  }
})

it("maps bucket names to stream names and ingest prefixes", () => {
  expect(streamForBucket(bucket("config"))).toBe("KV_config")
  expect(subjectPrefix(bucket("config"))).toBe("$KV.config.")
  expect(subjectForKey(bucket("config"), key("auth.username"))).toBe("$KV.config.auth.username")
  expect(subjectForKey(bucket("B"), key("k"))).toBe("$KV.B.k")
})

it("recovers the key from a subject by prefix stripping", () => {
  const b = bucket("config")
  const single = keyFromSubject(b, "$KV.config.username")
  expect(Result.getOrThrow(single)).toBe("username")
  const multi = keyFromSubject(b, "$KV.config.auth.tls.username")
  expect(Result.getOrThrow(multi)).toBe("auth.tls.username")
})

it("fails typed on subjects outside the bucket's prefix or carrying invalid keys", () => {
  const b = bucket("config")
  const cases: ReadonlyArray<readonly [string, "prefix-mismatch" | "invalid-key"]> = [
    ["$KV.other.username", "prefix-mismatch"], // different bucket
    ["foo.config.username", "prefix-mismatch"], // wrong root
    ["$KV.config", "prefix-mismatch"], // bare prefix, no trailing dot
    ["$KV.config.", "invalid-key"], // empty remainder
    ["$KV.config.a..b", "invalid-key"], // empty token written by a lax client
    ["$KV.config.a$b", "invalid-key"], // server-legal char outside the grammar
    ["$KV.configX.username", "prefix-mismatch"] // prefix must match a whole token
  ]
  for (const [subject, reason] of cases) {
    const result = keyFromSubject(b, subject)
    expect(Result.isFailure(result), subject).toBe(true)
    if (Result.isFailure(result)) {
      expect(result.failure.reason, subject).toBe(reason)
      expect(result.failure.subject, subject).toBe(subject)
    }
  }
})

it("recovers bucket names from stream names, refusing reserved-prefix residue", () => {
  expect(bucketFromStream("KV_config").pipe((o) => o._tag)).toBe("Some")
  const cases = ["notkv", "KV_", "KV_KV_x", "OBJ_config", "KV_a.b"]
  for (const stream of cases) {
    expect(bucketFromStream(stream)._tag, stream).toBe("None")
  }
})

it.prop(
  "every valid key round-trips through its subject",
  { b: Schema.toArbitrary(KvBucketName)(FastCheck), k: Schema.toArbitrary(KvKey)(FastCheck) },
  ({ b, k }) => {
    const back = keyFromSubject(b, subjectForKey(b, k))
    expect(Result.getOrThrow(back)).toBe(k)
  }
)
