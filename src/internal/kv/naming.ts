import { Option, Result, Schema } from "effect"

// Grammars per docs/architecture/adr-0003-kv-naming-strictness.md, evidence in
// docs/research/jetstream-kv-bucket-naming.md. The single-regex key form deliberately
// carries all four guards (non-empty, no leading/trailing dot, no empty token) so it
// cannot be ported without them.
const bucketPattern = /^[A-Za-z0-9_-]+$/
const keyPattern = /^[A-Za-z0-9_\-/=]+(?:\.[A-Za-z0-9_\-/=]+)*$/
const filterPattern = /^(?:>|(?:[A-Za-z0-9_\-/=]+|\*)(?:\.(?:[A-Za-z0-9_\-/=]+|\*))*(?:\.>)?)$/

const streamPrefix = "KV_"
const subjectRoot = "$KV."

export const KvBucketName = Schema.String.pipe(
  Schema.check(
    Schema.isPattern(bucketPattern, {
      message: "a KV bucket name is one or more ASCII letters, digits, dashes, or underscores"
    }),
    // 255-byte server stream-name cap minus the "KV_" prefix; enforced client-side
    // because the server reports it only as a stream error (err_code 10052).
    Schema.isMaxLength(252, {
      message: "a KV bucket name is at most 252 characters"
    }),
    Schema.makeFilter((input: string) => !input.startsWith(streamPrefix), {
      message: `a KV bucket name must not begin with the reserved stream prefix "${streamPrefix}"`
    })
  ),
  Schema.brand("KvBucketName")
)
export type KvBucketName = (typeof KvBucketName)["Type"]

/**
 * Keys beginning with `_kv` are reserved for internal use by the KV specification
 * (advisory only: no client or server enforces it, so neither does this schema).
 */
export const KvKey = Schema.String.pipe(
  Schema.check(
    Schema.isPattern(keyPattern, {
      message:
        "a KV key is dot-separated tokens of ASCII letters, digits, dashes, underscores, slashes, or equals signs"
    })
  ),
  Schema.brand("KvKey")
)
export type KvKey = (typeof KvKey)["Type"]

/**
 * Watch/keys selector: `*` only as a whole token, `>` only as the final token.
 * Stricter than the reference clients on purpose — the shapes they accept
 * (`foo>`, `a*b`, `a.>.b`) are treated as literal tokens by the server and
 * silently match nothing. Whole-bucket watching is expressed by omitting the
 * selector, never by an empty string.
 */
export const KvKeyFilter = Schema.String.pipe(
  Schema.check(
    Schema.isPattern(filterPattern, {
      message:
        "a KV key filter is dot-separated key tokens where * is a whole token and > is the final token"
    })
  ),
  Schema.brand("KvKeyFilter")
)
export type KvKeyFilter = (typeof KvKeyFilter)["Type"]

export class KvSubjectViolation
  extends Schema.TaggedError<KvSubjectViolation>()("KvSubjectViolation", {
    subject: Schema.String,
    bucket: Schema.String,
    reason: Schema.Literals(["prefix-mismatch", "invalid-key"])
  })
{}

export const streamForBucket = (bucket: KvBucketName): string => `${streamPrefix}${bucket}`

export const subjectPrefix = (bucket: KvBucketName): string => `${subjectRoot}${bucket}.`

export const subjectForKey = (bucket: KvBucketName, key: KvKey): string =>
  `${subjectRoot}${bucket}.${key}`

const decodeKey = Schema.decodeUnknownResult(KvKey)
const decodeBucket = Schema.decodeUnknownResult(KvBucketName)

/**
 * Recovers the key from a subject by stripping the bucket's literal prefix; total
 * and unambiguous because bucket names cannot contain a dot. Non-conformant keys
 * observed on the wire fail typed (ADR-0003), never silently skip.
 */
export const keyFromSubject = (
  bucket: KvBucketName,
  subject: string
): Result.Result<KvKey, KvSubjectViolation> => {
  const prefix = subjectPrefix(bucket)
  if (!subject.startsWith(prefix)) {
    return Result.fail(new KvSubjectViolation({ subject, bucket, reason: "prefix-mismatch" }))
  }
  return Result.mapError(
    decodeKey(subject.slice(prefix.length)),
    () => new KvSubjectViolation({ subject, bucket, reason: "invalid-key" })
  )
}

/**
 * Recovers a bucket name from a stream name by stripping one `KV_` prefix.
 * Residue that fails the bucket grammar — including nested `KV_` — is `None`.
 */
export const bucketFromStream = (stream: string): Option.Option<KvBucketName> =>
  stream.startsWith(streamPrefix)
    ? Result.getSuccess(decodeBucket(stream.slice(streamPrefix.length)))
    : Option.none()
