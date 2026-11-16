/**
 * HTTP Basic authentication for the WebDAV endpoint (DETAILED_DESIGN §12).
 *
 * Users come from a static table in the WEBDAV_USERS env var (trusted-PC model,
 * no external identity provider). Format is comma-separated entries:
 *
 *   user:pass:group1|group2,other:pass
 *
 * The group list (third, `|`-separated field) is optional. Passwords may not
 * contain ':' or ','. A caller with no/!Basic header is treated as anonymous;
 * the WebDAV layer then decides whether the target ACL grants `*`.
 *
 * Hashed passwords (scrypt via node:crypto) are supported in addition to
 * plaintext, using the format:
 *
 *   user:$scrypt$N=16384,r=8,p=1$<base64-salt>$<base64-hash>:group
 *
 * Use `BasicAuth.hashPassword(pass)` to generate a hash string, or run
 * `node dist/auth/hash-password.js <password>` for CLI usage.
 */

import { Buffer } from 'node:buffer'
import { scryptSync, timingSafeEqual, randomBytes } from 'node:crypto'
import type { Principal } from '../acl/acl.js'
import { ANONYMOUS } from '../acl/acl.js'

interface UserEntry {
  pass: string
  groups: string[]
}

function parseEntry(entry: string): { user: string; pass: string; groups: string[] } {
  const i1 = entry.indexOf(':')
  if (i1 < 0) return { user: entry, pass: '', groups: [] }
  const user = entry.slice(0, i1)
  const rest = entry.slice(i1 + 1)
  const i2 = rest.indexOf(':')
  if (i2 < 0) return { user, pass: rest, groups: [] }
  const pass = rest.slice(0, i2)
  const groups = rest
    .slice(i2 + 1)
    .split('|')
    .map((g) => g.trim())
    .filter((g) => g.length > 0)
  return { user, pass, groups }
}

/**
 * Verify a candidate plaintext password against a stored value.
 *
 * Stored formats:
 *   - `$scrypt$N=<N>,r=<r>,p=<p>$<base64-salt>$<base64-hash>` — scrypt hash
 *   - anything else — treated as plaintext (backward-compatible)
 *
 * Both branches use timingSafeEqual to prevent timing attacks.
 */
export function verifyPassword(candidate: string, stored: string): boolean {
  if (stored.startsWith('$scrypt$')) {
    // Format: $scrypt$N=16384,r=8,p=1$<base64-salt>$<base64-hash>
    const parts = stored.split('$')
    // parts: ['', 'scrypt', 'N=16384,r=8,p=1', '<salt-b64>', '<hash-b64>']
    if (parts.length !== 5) return false
    const params = parts[2]
    const saltB64 = parts[3]
    const hashB64 = parts[4]

    let N = 16384, r = 8, p = 1
    for (const kv of params.split(',')) {
      const [k, v] = kv.split('=')
      if (k === 'N') N = parseInt(v, 10)
      else if (k === 'r') r = parseInt(v, 10)
      else if (k === 'p') p = parseInt(v, 10)
    }
    if (!Number.isFinite(N) || !Number.isFinite(r) || !Number.isFinite(p)) return false

    try {
      const salt = Buffer.from(saltB64, 'base64')
      const expected = Buffer.from(hashB64, 'base64')
      const actual = scryptSync(candidate, salt, expected.length, { N, r, p })
      if (actual.length !== expected.length) return false
      return timingSafeEqual(actual, expected)
    } catch {
      return false
    }
  }

  // Plaintext comparison (backward compat)
  const a = Buffer.from(candidate)
  const b = Buffer.from(stored)
  if (a.length !== b.length) return false
  return timingSafeEqual(a, b)
}

/**
 * Split a comma-separated user spec, preserving commas inside `$scrypt$...$` parameters.
 */
export function splitUserEntries(spec: string): string[] {
  const entries: string[] = []
  let current = ''
  let inScryptParams = false

  for (let i = 0; i < spec.length; i++) {
    const ch = spec[i]
    if (spec.startsWith('$scrypt$', i)) {
      inScryptParams = true
      current += '$scrypt$'
      i += 7 // skip '$scrypt$' prefix
      continue
    }
    if (inScryptParams && ch === '$') {
      inScryptParams = false
    }
    if (ch === ',' && !inScryptParams) {
      if (current.trim()) entries.push(current.trim())
      current = ''
    } else {
      current += ch
    }
  }
  if (current.trim()) entries.push(current.trim())
  return entries
}

export class BasicAuth {
  private readonly users = new Map<string, UserEntry>()
  readonly realm: string

  constructor(spec: string, realm: string) {
    this.realm = realm
    for (const raw of splitUserEntries(spec)) {
      const entry = raw.trim()
      if (!entry) continue
      const { user, pass, groups } = parseEntry(entry)
      if (!user) continue
      this.users.set(user, { pass, groups })
    }
  }

  get userCount(): number {
    return this.users.size
  }

  /** The value for a `WWW-Authenticate` challenge header. */
  challenge(): string {
    return `Basic realm="${this.realm}", charset="UTF-8"`
  }

  /**
   * Generate a new scrypt hash string for the given plaintext password.
   * Uses N=16384, r=8, p=1, and a random 16-byte salt.
   *
   * Returns a string in the format:
   *   `$scrypt$N=16384,r=8,p=1$<base64-salt>$<base64-hash>`
   */
  static hashPassword(pass: string): string {
    const N = 16384, r = 8, p = 1
    const salt = randomBytes(16)
    const hash = scryptSync(pass, salt, 32, { N, r, p })
    return `$scrypt$N=${N},r=${r},p=${p}$${salt.toString('base64')}$${hash.toString('base64')}`
  }

  /**
   * Resolve an Authorization header to a Principal:
   *   - missing / non-Basic header  => ANONYMOUS (let ACL decide)
   *   - valid Basic credentials     => authenticated Principal
   *   - malformed / wrong password  => null (caller must challenge with 401)
   */
  authenticate(header: string | undefined): Principal | null {
    if (!header) return ANONYMOUS
    const m = /^Basic\s+(.+)$/i.exec(header.trim())
    if (!m) return ANONYMOUS
    let decoded: string
    try {
      decoded = Buffer.from(m[1], 'base64').toString('utf8')
    } catch {
      return null
    }
    const idx = decoded.indexOf(':')
    if (idx < 0) return null
    const user = decoded.slice(0, idx)
    const pass = decoded.slice(idx + 1)
    const entry = this.users.get(user)
    if (!entry || !verifyPassword(pass, entry.pass)) return null
    return { principal: user, groups: entry.groups, anonymous: false }
  }
}
