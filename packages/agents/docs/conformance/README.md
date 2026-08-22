# Protocol conformance

The protocol copy is immutable input. `protocol-requirements.json` is the
traceability ledger derived from it, and
`src/test/conformance/protocol-spec.test.ts` prevents the ledger from omitting
or silently rewriting a contract line.

## What becomes a requirement

The extractor includes every non-header wire table row, numbered procedure,
RFC 2119/8174 line, §12 checklist item, and line carrying an exactness marker
such as `exactly`, first/last ordering, zero-byte/headerless termination,
unknown-field behavior, fixed subjects, or byte/version rules. Appendix wire
fixtures are included. This deliberately over-approximates obligations: review
may classify a row as informative, but may not delete it to make coverage pass.

IDs are pinned-source line IDs (`AP03-L####`). A spec refresh may move them, so
it must be reviewed as a contract migration rather than mechanically accepted.

## Evidence rules

An entry remains `unmapped` until its `tests` array names executable tests that
exercise the repository through a public or wire boundary. The source catalog
and generated `test.todo` cases make every missing obligation visible; the
strict coverage test stays red until no entry is unmapped.

A mapping is sufficient only when its tests jointly cover the whole catalog
text. Bundled lines with multiple clauses need multiple assertions when one
behavior cannot prove them all. Assertions against a duplicate reference model
alone are not implementation evidence.

Use these evidence tiers where relevant:

- exact bytes/headers/subjects and upstream Appendix B fixtures;
- Schema decode/encode, unknown-field preservation, and property tests;
- stream state-machine/model tests for ack/error/terminator ordering;
- deterministic virtual-time and finalizer tests;
- real local NATS integration and differential checks against independent
  protocol producers/consumers.

Passing unit examples without the strict ledger gate is not conformance.
