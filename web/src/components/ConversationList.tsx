import { memo } from 'react'
import { ChatBubbleIcon } from '@radix-ui/react-icons'
import { messageText, type Conversation, type Profile } from '../client/types'
import { PersonAvatar } from './PersonAvatar'

const time = new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit' })

export const ConversationList = memo(function ConversationList({
  conversations,
  profiles,
  activeId,
  search,
  syncing,
  onSelect,
}: {
  conversations: readonly Conversation[]
  profiles: Readonly<Record<string, Profile>>
  activeId: string | undefined
  search: string
  syncing: boolean
  onSelect: (id: string) => void
}) {
  const title = (item: Conversation) =>
    item.type === 2
      ? `群聊 ${item.group_id}`
      : profiles[item.peer_user_id ?? '']?.nickname || item.peer_user_id || '会话'
  const query = search.toLowerCase()
  const filtered = conversations.filter((item) =>
    `${title(item)} ${item.peer_user_id ?? item.group_id ?? ''}`.toLowerCase().includes(query),
  )

  return (
    <nav aria-label="会话列表" className="min-h-0 flex-1 space-y-1 overflow-y-auto px-2 pb-3">
      {filtered.map((item) => (
        <button
          key={item.conversation_id}
          onClick={() => onSelect(item.conversation_id)}
          aria-current={item.conversation_id === activeId ? 'true' : undefined}
          className={`flex w-full items-center gap-3 rounded-xl px-3 py-4 text-left transition ${item.conversation_id === activeId ? 'bg-[#edf2ff]' : 'hover:bg-slate-100'}`}
        >
          <PersonAvatar
            name={title(item)}
            url={profiles[item.peer_user_id ?? '']?.avatar}
            group={item.type === 2}
            large
          />
          <div className="min-w-0 flex-1">
            <div className="flex items-center justify-between gap-2">
              <span className="truncate text-sm font-semibold">{title(item)}</span>
              <time className="shrink-0 text-[10px] text-muted">{time.format(item.updated_at)}</time>
            </div>
            <div className="mt-1.5 flex items-center justify-between gap-2">
              <p className="truncate text-xs text-muted">
                {item.last_message ? messageText(item.last_message) : '暂无消息'}
              </p>
              {item.unread > 0 && (
                <span
                  aria-label={`${item.unread} 条未读`}
                  className="flex min-w-4.5 shrink-0 items-center justify-center rounded-full bg-accent px-1 text-[10px] leading-4.5 font-medium text-white"
                >
                  {item.unread > 99 ? '99+' : item.unread}
                </span>
              )}
            </div>
          </div>
        </button>
      ))}
      {!filtered.length && (
        <div className="px-5 py-16 text-center text-sm leading-7 text-muted">
          <ChatBubbleIcon className="mx-auto mb-3 size-7 text-slate-300" />
          {search
            ? '没有匹配的会话'
            : syncing
              ? '正在加载会话…'
              : '还没有对话，点击 + 开始聊天'}
        </div>
      )}
    </nav>
  )
})
