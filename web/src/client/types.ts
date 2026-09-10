export interface Profile {
  user_id: string
  nickname: string
  avatar: string
}

export interface Session {
  user_id: string
  token: string
  expires_at: number
}

export interface Message {
  server_msg_id: string
  client_msg_id: string
  conversation_id: string
  seq: number
  session_type: 1 | 2
  sender_id: string
  recv_id?: string
  group_id?: string
  content_type: number
  content: string
  send_time: number
}

export interface Bounds {
  conversation_id: string
  min_seq: number
  max_seq: number
  read_seq: number
}

export interface Conversation extends Bounds {
  type: 1 | 2
  peer_user_id?: string
  group_id?: string
  unread: number
  updated_at: number
  last_message?: Message
}

export interface Target {
  conversation_id: string
  session_type: 1 | 2
  recv_id?: string
  group_id?: string
  title: string
}

export interface PendingMessage {
  client_msg_id: string
  target: Target
  text: string
  send_time: number
  status: 'sending' | 'failed'
  error?: string
}

export interface Snapshot {
  status: 'connecting' | 'online' | 'reconnecting' | 'offline'
  syncing: boolean
  conversations: Conversation[]
  profiles: Record<string, Profile>
  messages: Record<string, Message[]>
  pending: PendingMessage[]
  active: Target | null
  loadingHistory: boolean
  error: string
}

export interface Page<T> {
  items: T[]
  next_cursor: string
  has_more: boolean
}

export interface Ack {
  conversation_id: string
  server_msg_id: string
  seq: number
  send_time: number
}

export function messageText(message: Pick<Message, 'content' | 'content_type'>): string {
  if (message.content_type !== 1)
    return (
      (
        { 2: '[图片]', 3: '[视频]', 4: '[语音]', 5: '[文件]', 100: '[自定义消息]' } as Record<
          number,
          string
        >
      )[message.content_type] ?? '[暂不支持的消息]'
    )
  try {
    const content: unknown = JSON.parse(message.content)
    if (typeof content === 'string') return content
    if (
      content &&
      typeof content === 'object' &&
      'text' in content &&
      typeof content.text === 'string'
    )
      return content.text
  } catch {
    /* Old or third-party content may not match the typed text format. */
  }
  return '[无法解析的文本消息]'
}

export function directId(a: string, b: string): string {
  return `si_${[a, b].sort().join(':')}`
}

// A 2001 push carries a full message (docs/integration.md "WebSocket"), but an unrecognized shape
// must never reach local state: the caller falls back to a full sync instead.
export function parseMessage(value: unknown): Message | undefined {
  if (!value || typeof value !== 'object') return undefined
  const {
    server_msg_id,
    client_msg_id,
    conversation_id,
    seq,
    session_type,
    sender_id,
    recv_id,
    group_id,
    content_type,
    content,
    send_time,
  } = value as Record<string, unknown>
  if (
    typeof server_msg_id !== 'string' ||
    typeof client_msg_id !== 'string' ||
    typeof conversation_id !== 'string' ||
    !conversation_id ||
    typeof seq !== 'number' ||
    !Number.isSafeInteger(seq) ||
    seq < 1 ||
    (session_type !== 1 && session_type !== 2) ||
    typeof sender_id !== 'string' ||
    (recv_id !== undefined && typeof recv_id !== 'string') ||
    (group_id !== undefined && typeof group_id !== 'string') ||
    typeof content_type !== 'number' ||
    !Number.isSafeInteger(content_type) ||
    typeof content !== 'string' ||
    typeof send_time !== 'number' ||
    !Number.isSafeInteger(send_time)
  )
    return undefined
  return {
    server_msg_id,
    client_msg_id,
    conversation_id,
    seq,
    session_type,
    sender_id,
    recv_id,
    group_id,
    content_type,
    content,
    send_time,
  }
}
