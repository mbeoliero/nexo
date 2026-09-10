import { Api, errorMessage } from './api'
import { mergeMessages, syncRange } from './sync'
import {
  directId,
  parseMessage,
  type Ack,
  type Bounds,
  type Conversation,
  type Message,
  type Page,
  type PendingMessage,
  type Profile,
  type Session,
  type Snapshot,
  type Target,
} from './types'

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null

export class ImClient {
  private readonly api: Api
  private snapshot: Snapshot = {
    status: 'connecting',
    syncing: false,
    conversations: [],
    profiles: {},
    messages: {},
    pending: [],
    active: null,
    loadingHistory: false,
    error: '',
  }
  private readonly listeners = new Set<() => void>()
  private controller = new AbortController()
  private socket?: WebSocket
  private reconnectTimer?: ReturnType<typeof setTimeout>
  private syncTimer?: ReturnType<typeof setTimeout>
  private reconcileTimer?: ReturnType<typeof setTimeout>
  private connectTimer?: ReturnType<typeof setTimeout>
  private expiryTimer?: ReturnType<typeof setTimeout>
  private refreshPromise?: Promise<void>
  private dirty = false
  private running = false
  private attempt = 0
  private syncFailures = 0
  private retrySyncAt = 0
  private historyRequest = 0
  private readonly expandedHistory = new Set<string>()
  private readonly readRequests = new Map<string, number>()
  private readonly cursors: Record<string, number> = {}
  private cursorsDirty = false

  constructor(
    readonly session: Session,
    private readonly unauthorized: (reason: string) => void,
  ) {
    this.api = new Api(session.token, () => {
      if (this.running) this.endSession('登录已失效，请重新登录')
    })
    try {
      const saved: unknown = JSON.parse(localStorage.getItem(this.storageKey) || '{}')
      if (saved && typeof saved === 'object') {
        for (const [id, value] of Object.entries(saved)) {
          if (Number.isSafeInteger(value) && value >= 0) this.cursors[id] = value
        }
      }
    } catch {
      /* Storage may be disabled; the next session safely pulls again. */
    }
  }

  private get storageKey() {
    return `nexo:sync:${this.session.user_id}`
  }
  getSnapshot = () => this.snapshot
  subscribe = (listener: () => void) => {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  private update(patch: Partial<Snapshot>) {
    this.snapshot = { ...this.snapshot, ...patch }
    for (const listener of this.listeners) listener()
  }

  private report = (error: unknown) => {
    if (!this.running || (error instanceof Error && error.name === 'AbortError')) return
    this.update({ error: errorMessage(error) })
  }

  clearError = () => this.update({ error: '' })

  me(): Promise<Profile> {
    return this.api.request('/user/me', { signal: this.controller.signal })
  }

  async logout() {
    const signal = this.controller.signal
    await this.api.request('/auth/logout', {
      body: {},
      empty: true,
      signal,
    })
    signal.throwIfAborted()
    this.endSession('已退出登录')
  }

  start() {
    if (this.running) return
    this.running = true
    this.controller = new AbortController()
    window.addEventListener('online', this.resume)
    window.addEventListener('offline', this.offline)
    window.addEventListener('focus', this.resume)
    window.addEventListener('pagehide', this.persistCursors)
    document.addEventListener('visibilitychange', this.visible)
    const remaining = this.session.expires_at - Date.now()
    if (remaining <= 0) {
      this.endSession('登录已过期，请重新登录')
      return
    }
    this.scheduleExpiry()
    this.connect()
    this.backgroundSync()
  }

  stop() {
    this.running = false
    this.controller.abort()
    this.persistCursors()
    clearTimeout(this.reconnectTimer)
    clearTimeout(this.syncTimer)
    clearTimeout(this.reconcileTimer)
    clearTimeout(this.connectTimer)
    clearTimeout(this.expiryTimer)
    this.syncTimer = undefined
    this.reconcileTimer = undefined
    this.syncFailures = 0
    this.retrySyncAt = 0
    this.readRequests.clear()
    this.refreshPromise = undefined
    this.historyRequest++
    this.socket?.close()
    this.socket = undefined
    window.removeEventListener('online', this.resume)
    window.removeEventListener('offline', this.offline)
    window.removeEventListener('focus', this.resume)
    window.removeEventListener('pagehide', this.persistCursors)
    document.removeEventListener('visibilitychange', this.visible)
    this.update({
      syncing: false,
      loadingHistory: false,
      pending: this.snapshot.pending.map((message) =>
        message.status === 'sending'
          ? { ...message, status: 'failed', error: '发送结果未确认，请重试' }
          : message,
      ),
    })
  }

  private scheduleExpiry() {
    const remaining = this.session.expires_at - Date.now()
    if (remaining <= 0) {
      this.endSession('登录已过期，请重新登录')
      return
    }
    this.expiryTimer = setTimeout(() => this.scheduleExpiry(), Math.min(remaining, 2_147_483_647))
  }

  private persistCursors = () => {
    if (!this.cursorsDirty) return
    try {
      localStorage.setItem(this.storageKey, JSON.stringify(this.cursors))
      this.cursorsDirty = false
    } catch {
      /* Keep completed progress in memory and retry persistence at the next flush. */
    }
  }

  private backgroundSync = () => {
    if (!this.canAutoSync() || Date.now() < this.retrySyncAt) return
    const signal = this.controller.signal
    void this.refresh()
      .then(() => {
        if (!signal.aborted) this.update({ error: '' })
      })
      .catch((error) => {
        if (signal.aborted) return
        this.report(error)
      })
  }

  private canAutoSync() {
    return this.running && navigator.onLine && document.visibilityState === 'visible'
  }

  private pauseSyncTimers() {
    clearTimeout(this.syncTimer)
    clearTimeout(this.reconcileTimer)
    this.syncTimer = undefined
    this.reconcileTimer = undefined
  }

  private scheduleReconciliation() {
    if (!this.canAutoSync()) return
    const signal = this.controller.signal
    const delay = this.retrySyncAt
      ? Math.max(0, this.retrySyncAt - Date.now())
      : 30_000 * (1 + Math.random() * 0.2)
    this.reconcileTimer = setTimeout(() => {
      this.reconcileTimer = undefined
      if (!signal.aborted) {
        this.retrySyncAt = 0
        this.backgroundSync()
      }
    }, delay)
  }

  private endSession(reason: string) {
    this.stop()
    this.unauthorized(reason)
  }

  private visible = () => {
    if (document.visibilityState === 'visible') this.resume()
    else this.pauseSyncTimers()
  }
  private offline = () => {
    this.pauseSyncTimers()
    clearTimeout(this.reconnectTimer)
    this.update({ status: 'offline' })
    this.socket?.close()
  }
  private resume = () => {
    if (!this.running || !navigator.onLine) return
    this.retrySyncAt = 0
    if (!this.socket || this.socket.readyState === WebSocket.CLOSED) {
      clearTimeout(this.reconnectTimer)
      this.connect()
    }
    this.backgroundSync()
  }

  private connect() {
    if (!this.running) return
    if (!navigator.onLine) {
      this.update({ status: 'offline' })
      return
    }
    const url = new URL('/ws', window.location.href)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    url.search = new URLSearchParams({ token: this.session.token, platform_id: '5' }).toString()
    this.update({ status: this.attempt ? 'reconnecting' : 'connecting' })
    const socket = new WebSocket(url)
    this.socket = socket
    this.connectTimer = setTimeout(() => {
      if (socket.readyState === WebSocket.CONNECTING) socket.close()
    }, 15_000)
    socket.onopen = () => {
      if (this.socket !== socket || !this.running) return
      clearTimeout(this.connectTimer)
      this.attempt = 0
      this.update({ status: 'online', error: '' })
      this.backgroundSync()
    }
    socket.onmessage = (event) => {
      if (this.socket !== socket || !this.running) return
      try {
        // JSON.parse hands back any, which would let every field below through unchecked; the frame
        // comes off the wire, so each one is narrowed instead.
        const frame: unknown = JSON.parse(String(event.data))
        if (!isRecord(frame)) return
        const data = isRecord(frame.data) ? frame.data : undefined
        if (frame.req_id === 2002) {
          this.endSession(
            data?.reason === 'new_login'
              ? '账号已在另一个 Web 客户端登录，请重新登录'
              : '连接已被服务端终止，请重新登录',
          )
        } else if (
          frame.req_id === 2003 &&
          typeof data?.conversation_id === 'string' &&
          typeof data.read_seq === 'number' &&
          Number.isSafeInteger(data.read_seq) &&
          data.read_seq >= 0
        ) {
          this.applyRead(data.conversation_id, data.read_seq)
        } else if (frame.req_id === 2001) {
          // A push carries the whole message, so an in-order one needs no HTTP round trip at all.
          if (!this.applyPush(frame.data)) this.scheduleSync()
        } else if (frame.req_id === 2004) {
          this.scheduleSync()
        } else if (typeof frame.code === 'number' && frame.code !== 0) {
          this.update({ error: `连接请求失败（${frame.code}）` })
        }
      } catch {
        this.update({ error: '收到无效消息帧，请重新同步' })
        this.scheduleSync()
      }
    }
    socket.onclose = () => {
      if (this.socket !== socket || !this.running) return
      clearTimeout(this.connectTimer)
      this.socket = undefined
      this.update({ status: navigator.onLine ? 'reconnecting' : 'offline' })
      if (!navigator.onLine) return
      const delay = Math.min(30_000, 1000 * 2 ** Math.min(this.attempt++, 5)) + Math.random() * 500
      this.reconnectTimer = setTimeout(() => this.connect(), delay)
      // Browsers hide handshake errors; only an HTTP 401 proves the token is invalid.
      const signal = this.controller.signal
      void this.api.request('/user/me', { signal }).catch((error) => {
        if (!signal.aborted) this.report(error)
      })
    }
    socket.onerror = () => {
      /* onclose owns retries; no custom JSON heartbeat is needed. */
    }
  }

  private scheduleSync(delay = 120) {
    if (!this.canAutoSync() || this.syncTimer || Date.now() < this.retrySyncAt) return
    this.syncTimer = setTimeout(() => {
      this.syncTimer = undefined
      if (this.running) this.backgroundSync()
    }, delay)
  }

  async refresh(): Promise<void> {
    if (!this.running) return
    if (this.refreshPromise) {
      this.dirty = true
      return this.refreshPromise
    }
    clearTimeout(this.reconcileTimer)
    this.reconcileTimer = undefined
    const signal = this.controller.signal
    this.update({ syncing: true })
    const task = (async () => {
      do {
        this.dirty = false
        await this.syncOnce(signal)
        signal.throwIfAborted()
      } while (this.dirty)
    })()
    this.refreshPromise = task
    try {
      await task
      if (!signal.aborted) {
        this.syncFailures = 0
        this.retrySyncAt = 0
      }
    } catch (error) {
      if (!signal.aborted) {
        const delay = Math.min(30_000, 3000 * 2 ** Math.min(this.syncFailures++, 4))
        this.retrySyncAt = Date.now() + Math.ceil(delay * (1 + Math.random() * 0.2))
      }
      throw error
    } finally {
      if (this.refreshPromise === task) {
        this.persistCursors()
        this.refreshPromise = undefined
        this.update({ syncing: false })
        this.scheduleReconciliation()
      }
    }
  }

  private async syncOnce(signal: AbortSignal) {
    const conversations = new Map<string, Conversation>()
    let cursor = ''
    do {
      const page = await this.api.request<{
        conversations: Conversation[]
        next_cursor: string
        has_more: boolean
      }>('/conversation/list', { query: { cursor, limit: 100, with_last_message: 'true' }, signal })
      signal.throwIfAborted()
      for (const conversation of page.conversations)
        conversations.set(conversation.conversation_id, conversation)
      if (!page.has_more) break
      if (!page.next_cursor || page.next_cursor === cursor) throw new Error('会话分页游标无效')
      cursor = page.next_cursor
    } while (true)
    const bounds = new Map<string, Bounds>()
    cursor = ''
    do {
      const page = await this.api.request<Page<Bounds>>('/message/max_seqs', {
        query: { cursor, limit: 200 },
        signal,
      })
      signal.throwIfAborted()
      for (const item of page.items) bounds.set(item.conversation_id, item)
      if (!page.has_more) break
      if (!page.next_cursor || page.next_cursor === cursor) throw new Error('同步分页游标无效')
      cursor = page.next_cursor
    } while (true)

    const messages = { ...this.snapshot.messages }
    for (const id of Object.keys(messages)) {
      const range = bounds.get(id)
      if (range)
        messages[id] = messages[id].filter(
          (message) => message.seq >= range.min_seq && message.seq <= range.max_seq,
        )
      else delete messages[id]
    }
    const previous = new Map(this.snapshot.conversations.map((item) => [item.conversation_id, item]))
    const list = [...conversations.values()].map((conversation) => {
      const range = bounds.get(conversation.conversation_id)
      const read = Math.max(
        range?.read_seq ?? 0,
        conversation.read_seq,
        previous.get(conversation.conversation_id)?.read_seq ?? 0,
      )
      const visible = range ?? conversation
      const last = conversation.last_message
      return {
        ...conversation,
        ...range,
        last_message:
          last && last.seq >= visible.min_seq && last.seq <= visible.max_seq ? last : undefined,
        read_seq: read,
        unread: Math.max(0, visible.max_seq - read),
      }
    })
    this.update({ conversations: list, messages })
    for (const range of bounds.values()) {
      await syncRange(
        this.api,
        range,
        this.cursors[range.conversation_id] ?? 0,
        (batch, local) => {
          signal.throwIfAborted()
          this.merge(range.conversation_id, batch)
          if (this.cursors[range.conversation_id] !== local) {
            this.cursors[range.conversation_id] = local
            this.cursorsDirty = true
          }
        },
        signal,
      )
    }
    const missing = [
      ...new Set(
        list.flatMap((conversation) =>
          conversation.peer_user_id ? [conversation.peer_user_id] : [],
        ),
      ),
    ].filter((id) => !this.snapshot.profiles[id])
    for (let index = 0; index < missing.length; index += 100) {
      const result = await this.api.request<{ users: Profile[] }>('/user/info', {
        query: { user_ids: missing.slice(index, index + 100).join(',') },
        signal,
      })
      signal.throwIfAborted()
      this.update({
        profiles: {
          ...this.snapshot.profiles,
          ...Object.fromEntries(result.users.map((user) => [user.user_id, user])),
        },
      })
    }
    const active = this.snapshot.active
    if (
      active &&
      bounds.has(active.conversation_id) &&
      !this.snapshot.messages[active.conversation_id]?.length
    ) {
      const range = bounds.get(active.conversation_id)!
      await this.fetchHistory(range, range.max_seq, signal)
    }
  }

  private merge(id: string, batch: Message[]) {
    if (!batch.length) return
    let messages = mergeMessages(this.snapshot.messages[id] ?? [], batch)
    if (!this.expandedHistory.has(id)) messages = messages.slice(-100)
    const confirmed = new Set(
      batch
        .filter((message) => message.sender_id === this.session.user_id)
        .map((message) => message.client_msg_id),
    )
    this.update({
      messages: { ...this.snapshot.messages, [id]: messages },
      pending: this.snapshot.pending.filter(
        (message) => message.target.conversation_id !== id || !confirmed.has(message.client_msg_id),
      ),
    })
  }

  // Client synchronization (docs/integration.md): on 2001 apply `seq == local_max+1`, pull the gap
  // first when the push jumps ahead, and drop `seq <= local_max`. Returns false when the frame
  // cannot be applied against a known baseline; the caller then falls back to a full sync, which is
  // also the only path allowed to touch a conversation this client does not track yet.
  private applyPush(data: unknown): boolean {
    const message = parseMessage(data)
    if (!message) return false
    const id = message.conversation_id
    const conversation = this.snapshot.conversations.find((item) => item.conversation_id === id)
    if (!conversation) return false
    const local = this.cursors[id] ?? 0
    // A republished send or a message this round already pulled.
    if (message.seq <= local) return true
    // Below the visible range the baseline is stale (a rejoin raises it), so let syncOnce redo it.
    if (message.seq !== local + 1 || message.seq < conversation.min_seq) return false
    this.cursors[id] = message.seq
    this.cursorsDirty = true
    // A round that fetched its bounds before this push carries a max_seq below it, and its syncRange
    // clamps the cursor back down to that. Ask for another round: fresh bounds include the push, so
    // the cursor ends where the push left it instead of one seq behind.
    if (this.refreshPromise) this.dirty = true
    const max = Math.max(conversation.max_seq, message.seq)
    this.update({
      // Only the pushed conversation is rebuilt: every other row keeps its identity for the memo.
      conversations: this.snapshot.conversations.map((item) =>
        item.conversation_id === id
          ? {
              ...item,
              max_seq: max,
              last_message: message,
              unread: Math.max(0, max - item.read_seq),
              updated_at: Math.max(item.updated_at, message.send_time),
            }
          : item,
      ),
    })
    this.merge(id, [message])
    return true
  }

  private target(conversation: Conversation): Target {
    return {
      conversation_id: conversation.conversation_id,
      session_type: conversation.type,
      recv_id: conversation.peer_user_id,
      group_id: conversation.group_id,
      title:
        conversation.type === 2
          ? `群聊 ${conversation.group_id}`
          : this.snapshot.profiles[conversation.peer_user_id ?? '']?.nickname ||
            conversation.peer_user_id ||
            '会话',
    }
  }

  async select(id: string) {
    const conversation = this.snapshot.conversations.find((item) => item.conversation_id === id)
    if (!conversation) return
    this.update({ active: this.target(conversation), error: '' })
    const request = ++this.historyRequest
    this.update({ loadingHistory: true })
    try {
      await this.fetchHistory(conversation, conversation.max_seq, this.controller.signal)
    } finally {
      if (request === this.historyRequest) this.update({ loadingHistory: false })
    }
  }

  async openDirect(userId: string) {
    const id = userId.trim()
    if (!id) throw new Error('请输入对方完整的用户 ID')
    if (id === this.session.user_id) throw new Error('请输入另一个用户的 ID')
    const signal = this.controller.signal
    const result = await this.api.request<{ users: Profile[] }>('/user/info', {
      query: { user_ids: id },
      signal,
    })
    signal.throwIfAborted()
    const user = result.users.find((user) => user.user_id === id)
    if (!user) throw new Error('用户不存在，请检查完整的用户 ID')
    this.update({ profiles: { ...this.snapshot.profiles, [id]: user } })
    const conversationId = directId(this.session.user_id, id)
    if (
      this.snapshot.conversations.some(
        (conversation) => conversation.conversation_id === conversationId,
      )
    )
      await this.select(conversationId)
    else {
      this.historyRequest++
      this.update({
        active: {
          conversation_id: conversationId,
          session_type: 1,
          recv_id: id,
          title: user.nickname || id,
        },
        loadingHistory: false,
        error: '',
      })
    }
  }

  back() {
    this.historyRequest++
    this.update({ active: null, loadingHistory: false })
  }

  private async fetchHistory(range: Bounds, end: number, signal: AbortSignal) {
    if (end < range.min_seq || end < 1) return
    let begin = Math.max(range.min_seq, end - 99)
    while (begin <= end) {
      const result = await this.api.request<{ messages: Message[]; has_more: boolean }>(
        '/message/pull',
        {
          query: {
            conversation_id: range.conversation_id,
            begin_seq: begin,
            end_seq: end,
            limit: 100,
          },
          signal,
        },
      )
      signal.throwIfAborted()
      // A concurrent rejoin may have narrowed the visible range while the request was in flight.
      const current =
        this.snapshot.conversations.find(
          (conversation) => conversation.conversation_id === range.conversation_id,
        ) ?? range
      this.merge(
        range.conversation_id,
        result.messages.filter(
          (message) =>
            message.conversation_id === range.conversation_id &&
            message.seq >= current.min_seq &&
            message.seq <= current.max_seq,
        ),
      )
      if (!result.has_more) break
      const last = result.messages.at(-1)?.seq
      if (last === undefined || last < begin) throw new Error('历史消息分页没有前进')
      begin = last + 1
    }
  }

  async loadOlder() {
    const id = this.snapshot.active?.conversation_id
    const range = this.snapshot.conversations.find(
      (conversation) => conversation.conversation_id === id,
    )
    if (!id || !range || this.snapshot.loadingHistory) return
    const end = (this.snapshot.messages[id]?.[0]?.seq ?? range.max_seq + 1) - 1
    if (end < range.min_seq) return
    this.expandedHistory.add(id)
    const request = ++this.historyRequest
    this.update({ loadingHistory: true })
    try {
      await this.fetchHistory(range, end, this.controller.signal)
    } finally {
      if (request === this.historyRequest) this.update({ loadingHistory: false })
    }
  }

  async send(text: string) {
    const target = this.snapshot.active
    if (!target) throw new Error('请先选择一个会话')
    const value = text.trim()
    if (!value) throw new Error('消息不能为空')
    if (new TextEncoder().encode(JSON.stringify({ text: value })).length > 8192)
      throw new Error('消息过长：编码后不能超过 8192 字节')
    const pending: PendingMessage = {
      client_msg_id: crypto.randomUUID(),
      target,
      text: value,
      send_time: Date.now(),
      status: 'sending',
    }
    this.update({ pending: [...this.snapshot.pending, pending] })
    await this.transmit(pending)
  }

  async retry(id: string) {
    const pending = this.snapshot.pending.find((message) => message.client_msg_id === id)
    if (!pending || pending.status === 'sending') return
    this.update({
      pending: this.snapshot.pending.map((message) =>
        message === pending ? { ...message, status: 'sending', error: undefined } : message,
      ),
    })
    await this.transmit(pending)
  }

  private async transmit(pending: PendingMessage) {
    const signal = this.controller.signal
    try {
      const { target } = pending
      const ack = await this.api.request<Ack>('/message/send', {
        body: {
          client_msg_id: pending.client_msg_id,
          session_type: target.session_type,
          recv_id: target.recv_id,
          group_id: target.group_id,
          content_type: 1,
          content: JSON.stringify({ text: pending.text }),
          sender_read: true,
        },
        signal,
      })
      signal.throwIfAborted()
      this.merge(ack.conversation_id, [
        {
          ...ack,
          client_msg_id: pending.client_msg_id,
          session_type: target.session_type,
          sender_id: this.session.user_id,
          recv_id: target.recv_id,
          group_id: target.group_id,
          content_type: 1,
          content: JSON.stringify({ text: pending.text }),
        },
      ])
      this.scheduleSync()
    } catch (error) {
      if (signal.aborted) return
      this.update({
        pending: this.snapshot.pending.map((message) =>
          message.client_msg_id === pending.client_msg_id
            ? { ...message, status: 'failed', error: errorMessage(error) }
            : message,
        ),
      })
    }
  }

  private applyRead(id: string, seq: number) {
    this.update({
      conversations: this.snapshot.conversations.map((conversation) => {
        if (conversation.conversation_id !== id) return conversation
        const read = Math.max(conversation.read_seq, seq)
        return { ...conversation, read_seq: read, unread: Math.max(0, conversation.max_seq - read) }
      }),
    })
  }

  async markRead(id: string, seq: number) {
    const conversation = this.snapshot.conversations.find((item) => item.conversation_id === id)
    const read = Math.min(seq, this.cursors[id] ?? 0)
    if (
      !conversation ||
      read < 1 ||
      read <= Math.max(conversation.read_seq, this.readRequests.get(id) ?? 0)
    )
      return
    this.readRequests.set(id, read)
    const signal = this.controller.signal
    try {
      const result = await this.api.request<{ read_seq: number }>('/conversation/read', {
        body: { conversation_id: id, read_seq: read },
        signal,
      })
      signal.throwIfAborted()
      this.applyRead(id, result.read_seq)
    } finally {
      if (this.controller.signal === signal && this.readRequests.get(id) === read)
        this.readRequests.delete(id)
    }
  }
}
