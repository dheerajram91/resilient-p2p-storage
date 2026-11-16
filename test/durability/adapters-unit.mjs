import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { MetadataStore } from '../../dist/metadata/store.js'
import { WebdavApi } from '../../dist/api/webdav.js'
import { AdminApi } from '../../dist/api/admin.js'
import { BasicAuth } from '../../dist/auth/basic.js'

test('HTTP adapters report unavailable durable writes, including empty-folder COPY', async () => {
  const store = new MetadataStore(':memory:')
  const unavailable = () => Promise.resolve({ error: 'No metadata majority', code: 'EUNAVAILABLE' })
  const replication = { mkpath: unavailable, deleteEntry: unavailable, moveEntry: unavailable }
  const content = { writeFile: unavailable }
  const webdav = new WebdavApi({
    port: 0, realm: 'durability', allowAnon: true,
    store, content, replication, auth: new BasicAuth('', 'durability')
  })
  const admin = new AdminApi({ port: 0, store, content, replication })
  store.apply({ op: 'folder.create', id: 1, parentId: 0, name: 'source', aclId: 0, ctime: 0, mtime: 0 })
  webdav.start()
  admin.start()
  try {
    await Promise.all([once(webdav.server, 'listening'), once(admin.server, 'listening')])
    const base = `http://127.0.0.1:${webdav.server.address().port}`
    for (const [method, path, extra] of [
      ['PUT', '/file.txt', { body: 'not acknowledged' }],
      ['MKCOL', '/folder', {}],
      ['DELETE', '/source', {}],
      ['COPY', '/source', { headers: { Destination: `${base}/copy` } }]
    ]) {
      const response = await fetch(`${base}${path}`, { method, ...extra })
      assert.equal(response.status, 503, `${method} must not report success`)
      assert.equal((await response.json()).code, 'EUNAVAILABLE')
    }
    const response = await fetch(`http://127.0.0.1:${admin.server.address().port}/admin/mkcol`, {
      method: 'POST', body: JSON.stringify({ path: '/folder' })
    })
    assert.equal(response.status, 503)
    assert.equal(store.resolvePath('/copy').kind, 'none')
    assert.equal(store.resolvePath('/file.txt').kind, 'none')
  } finally {
    const closed = Promise.all([once(webdav.server, 'close'), once(admin.server, 'close')])
    webdav.stop()
    admin.stop()
    await closed
    store.close()
  }
})
