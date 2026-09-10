import type { Api } from './api'
import type { Bounds, Message } from './types'

export function mergeMessages(existing: Message[], incoming: Message[]): Message[] {
  const bySeq = new Map(existing.map((message) => [message.seq, message]))
  for (const message of incoming) bySeq.set(message.seq, message)
  return [...bySeq.values()].sort((a, b) => a.seq - b.seq)
}

export async function syncRange(
  api: Api,
  bounds: Bounds,
  cursor: number,
  commit: (messages: Message[], cursor: number) => void,
  signal: AbortSignal,
): Promise<void> {
  let local = Math.min(Math.max(cursor, bounds.min_seq - 1), bounds.max_seq)
  commit([], local)
  while (local < bounds.max_seq) {
    const page = await api.request<{ messages: Message[]; has_more: boolean }>('/message/pull', {
      query: {
        conversation_id: bounds.conversation_id,
        begin_seq: local + 1,
        end_seq: bounds.max_seq,
        limit: 100,
      },
      signal,
    })
    signal.throwIfAborted()
    if (!page.messages.length) throw new Error('消息可见范围已变化，请重新同步')
    let next = local
    for (const message of page.messages) {
      if (
        message.conversation_id !== bounds.conversation_id ||
        message.seq !== next + 1 ||
        message.seq > bounds.max_seq
      ) {
        throw new Error('消息序号不连续，未推进同步进度，请重新同步')
      }
      next = message.seq
    }
    // A last_message, push or send ACK must never skip a gap in this cursor.
    commit(page.messages, next)
    local = next
  }
}
