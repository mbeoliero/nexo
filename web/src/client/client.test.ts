// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ImClient } from './client'
import { directId, type Conversation, type Message } from './types'

class Socket {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSED = 3
  static instances: Socket[] = []
  readyState = Socket.CONNECTING
  onopen: (() => void) | null = null
  onclose: (() => void) | null = null
  onmessage: ((event: { data: string }) => void) | null = null
  onerror: (() => void) | null = null
  send = vi.fn()

  constructor(readonly url: URL) {
    Socket.instances.push(this)
  }
  open() {
    this.readyState = Socket.OPEN
    this.onopen?.()
  }
  frame(req_id: number, data: unknown = {}) {
    this.onmessage?.({ data: JSON.stringify({ req_id, data }) })
  }
  // Browser close events arrive after close(), not inside it.
  close = vi.fn(() => {
    this.readyState = Socket.CLOSED
  })
  closed() {
    this.readyState = Socket.CLOSED
    this.onclose?.()
  }
}

const user = 'u___1'
const peer = 'u___2'
const id = directId(user, peer)
const key = (account: string) => `nexo:sync:${account}`
const message = (seq: number): Message => ({
  conversation_id: id,
  seq,
  server_msg_id: `server-${seq}`,
  client_msg_id: `client-${seq}`,
  session_type: 1,
  sender_id: peer,
  recv_id: user,
  content_type: 1,
  content: JSON.stringify({ text: `message ${seq}` }),
  send_time: Date.now(),
})
const conversation = (max_seq: number): Conversation => ({
  conversation_id: id,
  min_seq: 1,
  max_seq,
  read_seq: 0,
  type: 1,
  peer_user_id: peer,
  unread: max_seq,
  updated_at: Date.now(),
})
const response = (data: unknown) => new Response(JSON.stringify({ code: 0, message: '', data }))
const deferred = <T>() => {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

type Handler = (url: URL, init: RequestInit) => unknown | Promise<unknown>
let routes: Map<string, Handler>
let requests: { url: URL; init: RequestInit }[]
let storage: Map<string, string>
let clients: ImClient[]
let browser: EventTarget
let page: EventTarget
const calls = (path: string) =>
  requests.filter((request) => request.url.pathname === `/api/v1${path}`)
const body = (request: { init: RequestInit }) => JSON.parse(String(request.init.body))
const flush = () => vi.advanceTimersByTimeAsync(0)

function makeClient(account = user, unauthorized = vi.fn()) {
  const client = new ImClient(
    { user_id: account, token: `token-${account}`, expires_at: Date.now() + 3_600_000 },
    unauthorized,
  )
  clients.push(client)
  return client
}

function serveConversation(maxSeq: number) {
  routes.set('/conversation/list', () => ({
    conversations: [conversation(maxSeq)],
    has_more: false,
    next_cursor: '',
  }))
  routes.set('/message/max_seqs', () => ({
    items: [conversation(maxSeq)],
    has_more: false,
    next_cursor: '',
  }))
  routes.set('/message/pull', (url) => ({
    messages: Array.from(
      {
        length:
          Number(url.searchParams.get('end_seq')) - Number(url.searchParams.get('begin_seq')) + 1,
      },
      (_, index) => message(Number(url.searchParams.get('begin_seq')) + index),
    ),
    has_more: false,
  }))
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-06-01T00:00:00Z'))
  vi.spyOn(Math, 'random').mockReturnValue(0)
  Socket.instances = []
  clients = []
  requests = []
  storage = new Map()
  browser = new EventTarget()
  page = new EventTarget()
  vi.stubGlobal(
    'window',
    Object.assign(browser, { location: { href: 'https://im.example.test/' } }),
  )
  vi.stubGlobal('document', Object.assign(page, { visibilityState: 'visible' }))
  vi.stubGlobal('navigator', { onLine: true })
  vi.stubGlobal('localStorage', {
    getItem: (name: string) => storage.get(name) ?? null,
    setItem: (name: string, value: string) => {
      storage.set(name, value)
    },
    removeItem: (name: string) => {
      storage.delete(name)
    },
    clear: () => storage.clear(),
  })
  vi.stubGlobal('WebSocket', Socket)
  routes = new Map<string, Handler>([
    ['/conversation/list', () => ({ conversations: [], has_more: false, next_cursor: '' })],
    ['/message/max_seqs', () => ({ items: [], has_more: false, next_cursor: '' })],
    ['/user/me', () => ({ user_id: user })],
    ['/user/info', () => ({ users: [{ user_id: peer, nickname: 'Peer', avatar: '' }] })],
    ['/conversation/read', (_url, init) => ({ read_seq: JSON.parse(String(init.body)).read_seq })],
  ])
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string, init: RequestInit) => {
      const url = new URL(input, 'https://im.example.test')
      requests.push({ url, init })
      const route = routes.get(url.pathname.replace('/api/v1', ''))
      if (!route) throw new Error(`Unexpected request: ${url.pathname}`)
      return response(await route(url, init))
    }),
  )
})

afterEach(() => {
  for (const client of clients) client.stop()
  vi.clearAllTimers()
  vi.useRealTimers()
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

describe('ImClient lifecycle and delivery', () => {
  it('2002 ends the session permanently even when onclose and resume events follow', async () => {
    const unauthorized = vi.fn()
    const client = makeClient(user, unauthorized)
    client.start()
    const socket = Socket.instances[0]
    socket.open()
    await flush()
    socket.frame(2002, { reason: 'new_login' })
    socket.closed()
    browser.dispatchEvent(new Event('online'))
    browser.dispatchEvent(new Event('focus'))
    page.dispatchEvent(new Event('visibilitychange'))
    await vi.advanceTimersByTimeAsync(60_000)
    expect(unauthorized).toHaveBeenCalledExactlyOnceWith(expect.stringContaining('另一个 Web'))
    expect(socket.close).toHaveBeenCalledOnce()
    expect(Socket.instances).toHaveLength(1)
    expect(calls('/user/me')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('backs off ordinary disconnects, caps the delay, and resets it after opening', async () => {
    const unauthorized = vi.fn()
    const client = makeClient(user, unauthorized)
    client.start()
    Socket.instances[0].open()
    await flush()
    for (const delay of [1000, 2000, 4000, 8000, 16_000, 30_000, 30_000]) {
      const count = Socket.instances.length
      Socket.instances.at(-1)!.closed()
      expect(client.getSnapshot().status).toBe('reconnecting')
      await vi.advanceTimersByTimeAsync(delay - 1)
      expect(Socket.instances).toHaveLength(count)
      await vi.advanceTimersByTimeAsync(1)
      expect(Socket.instances).toHaveLength(count + 1)
    }
    Socket.instances.at(-1)!.open()
    await flush()
    expect(client.getSnapshot().status).toBe('online')
    const count = Socket.instances.length
    Socket.instances.at(-1)!.closed()
    await vi.advanceTimersByTimeAsync(1000)
    expect(Socket.instances).toHaveLength(count + 1)
    expect(unauthorized).not.toHaveBeenCalled()
  })

  it('coalesces resync frames into one HTTP sync and never sends a JSON heartbeat', async () => {
    const client = makeClient()
    client.start()
    const socket = Socket.instances[0]
    socket.open()
    await flush()
    requests.length = 0
    socket.frame(2001, { conversation_id: id, seq: 9 })
    socket.frame(2004)
    await vi.advanceTimersByTimeAsync(119)
    expect(calls('/conversation/list')).toHaveLength(0)
    await vi.advanceTimersByTimeAsync(1)
    expect(calls('/conversation/list')).toHaveLength(1)
    expect(calls('/message/max_seqs')).toHaveLength(1)
    expect(socket.send).not.toHaveBeenCalled()
  })

  // syncRange clamps the cursor into the bounds its round already fetched, so a push that lands
  // mid-round is undone by the very round it raced. The push has to ask for another one.
  it('re-runs a sync round a push landed inside, instead of leaving the cursor clamped back', async () => {
    serveConversation(1)
    const client = makeClient()
    client.start()
    const socket = Socket.instances[0]
    socket.open()
    await flush()
    await vi.advanceTimersByTimeAsync(1000)
    requests.length = 0
    // Bounds fixed at max_seq 1, held open until after the push announces seq 2.
    const bounds = deferred<void>()
    routes.set('/message/max_seqs', async () => {
      await bounds.promise
      return { items: [conversation(1)], has_more: false, next_cursor: '' }
    })
    const round = client.refresh()
    await flush()
    socket.frame(2001, message(2))
    await flush()
    serveConversation(2)
    bounds.resolve()
    await round
    await vi.advanceTimersByTimeAsync(1000)
    expect(calls('/message/max_seqs')).toHaveLength(2)
    expect(JSON.parse(storage.get(key(user))!)).toEqual({ [id]: 2 })
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1, 2])
  })

  it('applies an in-order 2001 push without any HTTP request', async () => {
    const group: Conversation = {
      ...conversation(1),
      conversation_id: 'sg_1',
      type: 2,
      peer_user_id: undefined,
      group_id: 'g1',
    }
    routes.set('/conversation/list', () => ({
      conversations: [conversation(1), group],
      has_more: false,
      next_cursor: '',
    }))
    routes.set('/message/max_seqs', () => ({
      items: [conversation(1), group],
      has_more: false,
      next_cursor: '',
    }))
    routes.set('/message/pull', (url) => ({
      messages: [
        {
          ...message(Number(url.searchParams.get('begin_seq'))),
          conversation_id: String(url.searchParams.get('conversation_id')),
        },
      ],
      has_more: false,
    }))
    const client = makeClient()
    client.start()
    const socket = Socket.instances[0]
    socket.open()
    await flush()
    requests.length = 0
    const before = client.getSnapshot().conversations
    socket.frame(2001, message(2))
    await vi.advanceTimersByTimeAsync(1000)
    expect(requests).toEqual([])
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1, 2])
    const [direct, untouched] = client.getSnapshot().conversations
    expect(direct).toMatchObject({
      max_seq: 2,
      unread: 2,
      last_message: expect.objectContaining({ seq: 2 }),
    })
    expect(direct).not.toBe(before[0])
    // Only the pushed row is rebuilt, so ConversationList's memo sees the rest unchanged.
    expect(untouched).toBe(before[1])
    // The applied message advances the persisted baseline exactly as a pulled one would.
    client.stop()
    expect(JSON.parse(storage.get(key(user))!)[id]).toBe(2)
  })

  it('pulls the gap once when 2001 pushes jump ahead of the baseline', async () => {
    serveConversation(1)
    const client = makeClient()
    client.start()
    const socket = Socket.instances[0]
    socket.open()
    await flush()
    requests.length = 0
    serveConversation(4)
    socket.frame(2001, message(3))
    socket.frame(2001, message(4))
    // Out-of-order pushes wait for the pull instead of landing on top of the gap.
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1])
    await vi.advanceTimersByTimeAsync(120)
    expect(calls('/conversation/list')).toHaveLength(1)
    expect(calls('/message/max_seqs')).toHaveLength(1)
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1, 2, 3, 4])
  })

  it('drops a 2001 push at or below the local baseline', async () => {
    serveConversation(2)
    const client = makeClient()
    client.start()
    const socket = Socket.instances[0]
    socket.open()
    await flush()
    requests.length = 0
    const snapshot = client.getSnapshot()
    socket.frame(2001, message(1))
    socket.frame(2001, message(2))
    await vi.advanceTimersByTimeAsync(1000)
    expect(requests).toEqual([])
    expect(client.getSnapshot()).toBe(snapshot)
  })

  it.each([
    ['a partial payload', { conversation_id: id, seq: 2 }],
    ['a fractional seq', { ...message(2), seq: 2.5 }],
    ['a non-string content', { ...message(2), content: 42 }],
    ['an untracked conversation', { ...message(2), conversation_id: 'si_unknown' }],
  ])('falls back to a full sync for %s in a 2001 push', async (_label, data) => {
    serveConversation(1)
    const client = makeClient()
    client.start()
    const socket = Socket.instances[0]
    socket.open()
    await flush()
    requests.length = 0
    serveConversation(2)
    socket.frame(2001, data)
    await vi.advanceTimersByTimeAsync(120)
    expect(calls('/conversation/list')).toHaveLength(1)
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1, 2])
    expect(client.getSnapshot().messages['si_unknown']).toBeUndefined()
  })

  it('recovers a silently lost last push while the connection remains healthy', async () => {
    serveConversation(1)
    const client = makeClient()
    client.start()
    Socket.instances[0].open()
    await flush()
    requests.length = 0
    serveConversation(2)
    await vi.advanceTimersByTimeAsync(36_000)
    expect(client.getSnapshot().status).toBe('online')
    expect(calls('/message/max_seqs').length).toBeGreaterThan(0)
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1, 2])
    expect(Socket.instances[0].send).not.toHaveBeenCalled()
  })

  it('ends the session when selecting history returns HTTP 401', async () => {
    serveConversation(1)
    const unauthorized = vi.fn()
    const client = makeClient(user, unauthorized)
    client.start()
    Socket.instances[0].open()
    await flush()
    vi.mocked(fetch).mockResolvedValueOnce(new Response(
      JSON.stringify({ code: 10102, message: 'token expired', data: {} }),
      { status: 401 },
    ))
    await expect(client.select(id)).rejects.toMatchObject({ status: 401, code: 10102 })
    expect(unauthorized).toHaveBeenCalledOnce()
    expect(Socket.instances[0].close).toHaveBeenCalledOnce()
  })

  it('batches completed-page cursor persistence for a sync round', async () => {
    serveConversation(3)
    routes.set('/message/pull', (url) => {
      const begin = Number(url.searchParams.get('begin_seq'))
      return { messages: [message(begin)], has_more: begin < 3 }
    })
    const save = vi.spyOn(localStorage, 'setItem')
    const client = makeClient()
    client.start()
    await flush()
    expect(JSON.parse(storage.get(key(user))!)[id]).toBe(3)
    expect(save).toHaveBeenCalledOnce()
  })

  it('pauses automatic sync while hidden or offline and reconciles immediately on return', async () => {
    serveConversation(1)
    const client = makeClient()
    client.start()
    Socket.instances[0].open()
    await flush()
    requests.length = 0
    Object.assign(page, { visibilityState: 'hidden' })
    page.dispatchEvent(new Event('visibilitychange'))
    serveConversation(2)
    Socket.instances[0].frame(2001)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(calls('/message/max_seqs')).toHaveLength(0)
    Object.assign(page, { visibilityState: 'visible' })
    page.dispatchEvent(new Event('visibilitychange'))
    await flush()
    expect(client.getSnapshot().messages[id].at(-1)?.seq).toBe(2)

    requests.length = 0
    Object.assign(navigator, { onLine: false })
    browser.dispatchEvent(new Event('offline'))
    serveConversation(3)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(calls('/message/max_seqs')).toHaveLength(0)
    Object.assign(navigator, { onLine: true })
    browser.dispatchEvent(new Event('online'))
    await flush()
    expect(client.getSnapshot().messages[id].at(-1)?.seq).toBe(3)
    client.stop()
    requests.length = 0
    await vi.advanceTimersByTimeAsync(60_000)
    expect(requests).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('backs off repeated synchronization failures without allowing pushes to bypass the delay', async () => {
    let attempts = 0
    routes.set('/message/max_seqs', () => {
      attempts++
      throw new TypeError('temporary outage')
    })
    const client = makeClient()
    client.start()
    Socket.instances[0].open()
    await flush()
    expect(attempts).toBe(1)
    for (const delay of [3000, 6000, 12_000, 24_000, 30_000, 30_000]) {
      const before = attempts
      Socket.instances[0].frame(2001)
      await vi.advanceTimersByTimeAsync(delay - 1)
      expect(attempts).toBe(before)
      await vi.advanceTimersByTimeAsync(1)
      expect(attempts).toBe(before + 1)
    }
    serveConversation(1)
    await vi.advanceTimersByTimeAsync(30_000)
    expect(client.getSnapshot().messages[id].at(-1)?.seq).toBe(1)
    requests.length = 0
    await vi.advanceTimersByTimeAsync(30_000)
    expect(calls('/message/max_seqs')).toHaveLength(1)
  })

  it('jitters the normal reconciliation interval', async () => {
    vi.mocked(Math.random).mockReturnValue(0.5)
    const client = makeClient()
    client.start()
    Socket.instances[0].open()
    await flush()
    requests.length = 0
    await vi.advanceTimersByTimeAsync(32_999)
    expect(calls('/message/max_seqs')).toHaveLength(0)
    await vi.advanceTimersByTimeAsync(1)
    expect(calls('/message/max_seqs')).toHaveLength(1)
  })

  it('retries even when random jitter produces fractional milliseconds', async () => {
    vi.mocked(Math.random).mockReturnValue(0.5001)
    serveConversation(1)
    let attempts = 0
    routes.set('/message/max_seqs', () => {
      if (++attempts === 1) throw new TypeError('temporary outage')
      return { items: [conversation(1)], has_more: false, next_cursor: '' }
    })
    const client = makeClient()
    client.start()
    await flush()
    await vi.advanceTimersByTimeAsync(3600)
    expect(attempts).toBe(2)
    expect(client.getSnapshot().messages[id].at(-1)?.seq).toBe(1)
  })

  it.each(['profile', 'open direct', 'read', 'logout'])(
    'ends the session after HTTP 401 from %s',
    async (operation) => {
      serveConversation(3)
      const unauthorized = vi.fn()
      const client = makeClient(user, unauthorized)
      client.start()
      await flush()
      vi.mocked(fetch).mockResolvedValueOnce(new Response(
        JSON.stringify({ code: 10102, message: 'token expired', data: {} }),
        { status: 401 },
      ))
      const actions = {
        profile: () => client.me(),
        'open direct': () => client.openDirect(peer),
        read: () => client.markRead(id, 3),
        logout: () => client.logout(),
      }
      await expect(actions[operation as keyof typeof actions]()).rejects.toMatchObject({ status: 401 })
      expect(unauthorized).toHaveBeenCalledOnce()
    },
  )

  it('ignores a late HTTP 401 after the client restarts', async () => {
    serveConversation(1)
    const unauthorized = vi.fn()
    const client = makeClient(user, unauthorized)
    client.start()
    await flush()
    const result = deferred<unknown>()
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: false, status: 401, json: () => result.promise,
    } as Response)
    const selecting = client.select(id).catch((error) => error)
    await flush()
    expect(client.getSnapshot().loadingHistory).toBe(true)
    client.stop()
    expect(client.getSnapshot().loadingHistory).toBe(false)
    client.start()
    await flush()
    const fresh = deferred<unknown>()
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true, status: 200, json: () => fresh.promise,
    } as Response)
    const selectingFresh = client.select(id)
    await flush()
    const current = client.getSnapshot()
    expect(current.loadingHistory).toBe(true)
    result.resolve({ code: 10102, message: 'old token expired', data: {} })
    expect(await selecting).toMatchObject({ name: 'AbortError' })
    expect(unauthorized).not.toHaveBeenCalled()
    expect(client.getSnapshot()).toBe(current)
    fresh.resolve({ code: 0, data: { messages: [message(1)], has_more: false } })
    await selectingFresh
    expect(client.getSnapshot().loadingHistory).toBe(false)
  })

  it('keeps completed-page progress when a subsequent page fails', async () => {
    serveConversation(3)
    routes.set('/message/pull', (url) => {
      if (url.searchParams.get('begin_seq') === '1')
        return { messages: [message(1)], has_more: true }
      throw new TypeError('second page failed')
    })
    const client = makeClient()
    client.start()
    await flush()
    expect(JSON.parse(storage.get(key(user))!)[id]).toBe(1)
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1])
  })

  it.each(['stop', 'pagehide'])('flushes completed pages on %s while another page is pending', async (event) => {
    serveConversation(3)
    const later = deferred<unknown>()
    routes.set('/message/pull', (url) => {
      if (url.searchParams.get('begin_seq') === '1')
        return { messages: [message(1)], has_more: true }
      return later.promise
    })
    const client = makeClient()
    client.start()
    await flush()
    if (event === 'stop') client.stop()
    else browser.dispatchEvent(new Event('pagehide'))
    expect(JSON.parse(storage.get(key(user))!)[id]).toBe(1)
    client.stop()
    later.resolve({ messages: [message(2), message(3)], has_more: false })
    await flush()
    expect(JSON.parse(storage.get(key(user))!)[id]).toBe(1)
  })

  it('retries a lost HTTP ACK using the original client_msg_id and content', async () => {
    const client = makeClient()
    client.start()
    await flush()
    await client.openDirect(peer)
    let sends = 0
    routes.set('/message/send', () => {
      if (++sends === 1) throw new TypeError('response lost after server committed')
      return { conversation_id: id, server_msg_id: 'committed', seq: 1, send_time: Date.now() }
    })
    await client.send('  original text  ')
    const pending = client.getSnapshot().pending[0]
    expect(pending.status).toBe('failed')
    expect(pending.error).toBeTruthy()
    await client.retry(pending.client_msg_id)
    const sent = calls('/message/send').map(body)
    expect(sent).toHaveLength(2)
    expect(sent[1]).toEqual(sent[0])
    expect(sent[1]).toMatchObject({
      client_msg_id: pending.client_msg_id,
      content: '{"text":"original text"}',
      sender_read: true,
    })
    expect(client.getSnapshot().pending).toEqual([])
    expect(client.getSnapshot().messages[id]).toEqual([
      expect.objectContaining({ server_msg_id: 'committed', seq: 1 }),
    ])
  })

  it('does not let an ACK cross a sync gap when marking read', async () => {
    serveConversation(1)
    const client = makeClient()
    client.start()
    await flush()
    await client.select(id)
    routes.set('/message/send', () => ({
      conversation_id: id,
      server_msg_id: 'ack-3',
      seq: 3,
      send_time: Date.now(),
    }))
    await client.send('third')
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1, 3])
    await client.markRead(id, 3)
    expect(calls('/conversation/read').map(body)).toEqual([{ conversation_id: id, read_seq: 1 }])
    expect(JSON.parse(storage.get(key(user))!)[id]).toBe(1)
    serveConversation(3)
    await vi.advanceTimersByTimeAsync(120)
    await client.markRead(id, 3)
    expect(calls('/conversation/read').map(body)).toEqual([
      { conversation_id: id, read_seq: 1 },
      { conversation_id: id, read_seq: 3 },
    ])
    expect(client.getSnapshot().conversations[0].read_seq).toBe(3)
  })

  it('loads and persists sync cursors only under the current account key', async () => {
    storage.set(key(user), JSON.stringify({ [id]: 2 }))
    serveConversation(3)
    const first = makeClient()
    first.start()
    await flush()
    expect(calls('/message/pull')[0].url.searchParams.get('begin_seq')).toBe('3')
    first.stop()
    requests.length = 0
    const second = makeClient(peer)
    second.start()
    await flush()
    expect(calls('/message/pull')[0].url.searchParams.get('begin_seq')).toBe('1')
    expect(JSON.parse(storage.get(key(user))!)).toEqual({ [id]: 3 })
    expect(JSON.parse(storage.get(key(peer))!)).toEqual({ [id]: 3 })
  })

  it('ignores an old send response after stop, including after the same instance restarts', async () => {
    const ack = deferred<unknown>()
    routes.set('/message/send', () => ack.promise)
    const client = makeClient()
    client.start()
    await flush()
    await client.openDirect(peer)
    const sending = client.send('old request')
    const pending = client.getSnapshot().pending[0]
    const signal = calls('/message/send')[0].init.signal!
    client.stop()
    expect(signal.aborted).toBe(true)
    expect(client.getSnapshot().pending[0]).toMatchObject({
      ...pending,
      status: 'failed',
      error: expect.stringContaining('未确认'),
    })
    client.start()
    await flush()
    const retriedAck = deferred<unknown>()
    routes.set('/message/send', () => retriedAck.promise)
    const retrying = client.retry(pending.client_msg_id)
    expect(calls('/message/send')).toHaveLength(2)
    expect(body(calls('/message/send')[1])).toEqual(body(calls('/message/send')[0]))
    const snapshot = client.getSnapshot()
    expect(snapshot.pending[0].status).toBe('sending')
    ack.resolve({ conversation_id: id, server_msg_id: 'stale', seq: 99, send_time: Date.now() })
    await sending
    expect(client.getSnapshot()).toBe(snapshot)
    expect(client.getSnapshot().messages[id]).toBeUndefined()
    retriedAck.resolve({ conversation_id: id, server_msg_id: 'confirmed', seq: 1, send_time: Date.now() })
    await retrying
    expect(client.getSnapshot().pending).toEqual([])
    expect(client.getSnapshot().messages[id]).toEqual([
      expect.objectContaining({ server_msg_id: 'confirmed', seq: 1, content: '{"text":"old request"}' }),
    ])
  })

  it('does not apply an old openDirect response to a restarted instance', async () => {
    const result = deferred<unknown>()
    const client = makeClient()
    client.start()
    await flush()
    // Fetch has completed; delay JSON parsing to exercise the post-await lifecycle guard.
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => result.promise,
    } as Response)
    const opening = client.openDirect(peer).catch((error) => error)
    await flush()
    client.stop()
    client.start()
    await flush()
    result.resolve({
      code: 0,
      data: { users: [{ user_id: peer, nickname: 'Stale profile', avatar: '' }] },
    })
    await opening
    expect(client.getSnapshot().active).toBeNull()
    expect(client.getSnapshot().profiles[peer]).toBeUndefined()
  })

  it('retries transient HTTP sync failures without waiting for another push', async () => {
    serveConversation(1)
    let attempts = 0
    routes.set('/message/max_seqs', () => {
      if (++attempts === 1) throw new TypeError('temporary network failure')
      return { items: [conversation(1)], has_more: false, next_cursor: '' }
    })
    const client = makeClient()
    client.start()
    await flush()
    expect(client.getSnapshot().error).not.toBe('')
    await vi.advanceTimersByTimeAsync(2999)
    expect(attempts).toBe(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(attempts).toBe(2)
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1])
    expect(client.getSnapshot().error).toBe('')
  })

  it('loads the recent window even when resume has already cached one new message', async () => {
    serveConversation(3)
    storage.set(key(user), JSON.stringify({ [id]: 2 }))
    const client = makeClient()
    client.start()
    await flush()
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([3])
    await client.select(id)
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1, 2, 3])
  })

  it('loads recent history across a server page limit smaller than the requested window', async () => {
    serveConversation(3)
    storage.set(key(user), JSON.stringify({ [id]: 3 }))
    routes.set('/message/pull', (url) => {
      const begin = Number(url.searchParams.get('begin_seq'))
      return { messages: [message(begin)], has_more: begin < 3 }
    })
    const client = makeClient()
    client.start()
    await flush()
    await client.select(id)
    expect(client.getSnapshot().messages[id].map((item) => item.seq)).toEqual([1, 2, 3])
    expect(
      calls('/message/pull').map((request) => request.url.searchParams.get('begin_seq')),
    ).toEqual(['1', '2', '3'])
  })
})
