import { memo } from 'react'
import { ChatBubbleIcon } from '@radix-ui/react-icons'
import { messageText, type Message, type PendingMessage, type Profile } from '../client/types'
import { PersonAvatar } from './PersonAvatar'

const time = new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit' })

export const MessageList = memo(function MessageList({
  messages,
  pending,
  me,
  profiles,
  group,
  loadingHistory,
  onRetry,
}: {
  messages: readonly Message[]
  pending: readonly PendingMessage[]
  me: Profile
  profiles: Readonly<Record<string, Profile>>
  group: boolean
  loadingHistory: boolean
  onRetry: (id: string) => void
}) {
  return (
    <>
      {!messages.length && !pending.length && (
        <div className="flex h-full min-h-40 flex-col items-center justify-center text-center">
          <span className="mb-4 flex size-14 items-center justify-center rounded-2xl border border-line bg-white text-slate-300">
            <ChatBubbleIcon className="size-6" />
          </span>
          <p className="text-sm text-muted">
            {loadingHistory ? '正在加载消息…' : '从一句你好开始'}
          </p>
          <p className="mt-2 text-xs text-slate-400">
            {loadingHistory ? '请稍候' : '发送第一条消息，建立你们的对话'}
          </p>
        </div>
      )}
      <div className="space-y-5">
        {messages.map((message) => {
          const mine = message.sender_id === me.user_id
          const sender = mine ? me : profiles[message.sender_id]
          return (
            <div
              key={message.server_msg_id}
              className={`flex items-end gap-2.5 ${mine ? 'flex-row-reverse' : ''}`}
            >
              <PersonAvatar
                name={sender?.nickname || message.sender_id}
                url={sender?.avatar}
              />
              <div
                className={`flex max-w-[80%] flex-col ${mine ? 'items-end' : 'items-start'}`}
              >
                {!mine && group && (
                  <span className="mb-1 px-1 text-[10px] text-muted">
                    {sender?.nickname || message.sender_id}
                  </span>
                )}
                <div
                  className={`rounded-2xl px-4 py-3 text-sm leading-6 whitespace-pre-wrap wrap-anywhere ${mine ? 'rounded-br-md bg-accent text-white' : 'rounded-bl-md border border-line bg-white text-ink'}`}
                >
                  {messageText(message)}
                </div>
                <time className="mt-1.5 px-1 text-[10px] text-muted">
                  {time.format(message.send_time)}
                  {mine && ' · 已发送'}
                </time>
              </div>
            </div>
          )
        })}
        {pending.map((message) => (
          <div
            key={message.client_msg_id}
            className="flex flex-row-reverse items-end gap-2.5"
          >
            <PersonAvatar name={me.nickname} url={me.avatar} />
            <div className="flex max-w-[80%] flex-col items-end">
              <div className="rounded-2xl rounded-br-md bg-accent px-4 py-3 text-sm leading-6 whitespace-pre-wrap wrap-anywhere text-white">
                {message.text}
              </div>
              {message.status === 'failed' ? (
                <button
                  className="mt-1.5 max-w-full rounded px-1 text-right text-xs text-red-600 hover:underline"
                  onClick={() => onRetry(message.client_msg_id)}
                  title={message.error}
                >
                  发送失败 · 点击重试
                </button>
              ) : (
                <span role="status" className="mt-1.5 text-[10px] text-muted">
                  发送中…
                </span>
              )}
            </div>
          </div>
        ))}
      </div>
    </>
  )
})
