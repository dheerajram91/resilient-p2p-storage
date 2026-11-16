import test from 'node:test'
import assert from 'node:assert/strict'
import { BasicAuth, verifyPassword } from '../../dist/auth/basic.js'

test('BasicAuth and verifyPassword with scrypt and plaintext', () => {
  const password = 'super-secret-password-123'
  const hash1 = BasicAuth.hashPassword(password)
  const hash2 = BasicAuth.hashPassword(password)

  // Format check
  assert.ok(hash1.startsWith('$scrypt$N=16384,r=8,p=1$'))
  // Distinct salts
  assert.notEqual(hash1, hash2)

  // Verification against scrypt hash
  assert.equal(verifyPassword(password, hash1), true)
  assert.equal(verifyPassword(password, hash2), true)
  assert.equal(verifyPassword('wrong-password', hash1), false)
  assert.equal(verifyPassword('', hash1), false)

  // Plaintext backward compatibility
  assert.equal(verifyPassword('myplainpass', 'myplainpass'), true)
  assert.equal(verifyPassword('myplainpass', 'wrongpass'), false)
  assert.equal(verifyPassword('short', 'much-longer-stored-password'), false)

  // BasicAuth user table with mixed hashed and plaintext users
  const spec = `alice:${hash1}:admins|engineering,bob:bobpass:readers`
  const auth = new BasicAuth(spec, 'test-realm')
  assert.equal(auth.userCount, 2)
  assert.equal(auth.challenge(), 'Basic realm="test-realm", charset="UTF-8"')

  const b64 = (s) => Buffer.from(s).toString('base64')

  // Alice (scrypt) with valid credentials
  const aliceGood = auth.authenticate(`Basic ${b64(`alice:${password}`)}`)
  assert.ok(aliceGood)
  assert.equal(aliceGood.principal, 'alice')
  assert.deepEqual(aliceGood.groups, ['admins', 'engineering'])
  assert.equal(aliceGood.anonymous, false)

  // Alice with wrong credentials -> null
  assert.equal(auth.authenticate(`Basic ${b64('alice:badpass')}`), null)

  // Bob (plaintext) with valid credentials
  const bobGood = auth.authenticate(`Basic ${b64('bob:bobpass')}`)
  assert.ok(bobGood)
  assert.equal(bobGood.principal, 'bob')
  assert.deepEqual(bobGood.groups, ['readers'])
  assert.equal(bobGood.anonymous, false)

  // Bob with wrong credentials -> null
  assert.equal(auth.authenticate(`Basic ${b64('bob:wrongpass')}`), null)

  // Unknown user -> null
  assert.equal(auth.authenticate(`Basic ${b64('charlie:pass')}`), null)

  // Unauthenticated / missing header -> anonymous
  const anon = auth.authenticate(undefined)
  assert.ok(anon)
  assert.equal(anon.anonymous, true)
  assert.equal(anon.principal, 'anonymous')

  // Malformed Base64 -> null
  assert.equal(auth.authenticate('Basic not-valid-base64@@@!'), null)
})
