/**
 * NATS subject matching: `*` matches exactly one token, `>` matches one or more
 * trailing tokens, and both act only as whole tokens — a `>` or `*` embedded in
 * a longer token is a literal character (server `isValidSubject` semantics).
 */
export const matches = (filter: string, subject: string): boolean => {
  const filterTokens = filter.split(".")
  const subjectTokens = subject.split(".")
  for (let i = 0; i < filterTokens.length; i++) {
    const token = filterTokens[i]
    if (token === ">") {
      return i < subjectTokens.length
    }
    if (i >= subjectTokens.length) {
      return false
    }
    if (token !== "*" && token !== subjectTokens[i]) {
      return false
    }
  }
  return filterTokens.length === subjectTokens.length
}
