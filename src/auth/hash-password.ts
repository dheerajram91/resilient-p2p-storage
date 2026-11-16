/**
 * CLI helper: hash a plaintext password with scrypt (node:crypto).
 *
 * Usage:
 *   node dist/auth/hash-password.js <password>
 *
 * Prints the hashed string to stdout, e.g.:
 *   $scrypt$N=16384,r=8,p=1$<base64-salt>$<base64-hash>
 *
 * This can then be used as the password field in WEBDAV_USERS / MCP_USERS:
 *   alice:$scrypt$N=16384,r=8,p=1$...:staff
 */

import { BasicAuth } from './basic.js'

const pass = process.argv[2]
if (!pass) {
  process.stderr.write('Usage: node dist/auth/hash-password.js <password>\n')
  process.exit(1)
}

process.stdout.write(BasicAuth.hashPassword(pass) + '\n')
