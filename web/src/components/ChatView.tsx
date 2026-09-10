import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
} from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import * as DropdownMenu from '@radix-ui/react-dropdown-menu'
import {
  ArrowLeftIcon,
  ArrowUpIcon,
  ChatBubbleIcon,
  CheckIcon,
  ChevronDownIcon,
  CopyIcon,
  Cross2Icon,
  DotsHorizontalIcon,
  ExitIcon,
  MagnifyingGlassIcon,
  PaperPlaneIcon,
  PlusIcon,
  ReloadIcon,
} from '@radix-ui/react-icons'
import type { ImClient } from '../client/client'
import { errorMessage } from '../client/api'
import type { Message, Profile, Snapshot } from '../client/types'
import { ConversationList } from './ConversationList'
import { MessageList } from './MessageList'
import { PersonAvatar } from './PersonAvatar'

const emptyMessages: readonly Message[] = []
const statusText = {
  connecting: '连接中',
  online: '已连接',
  reconnecting: '正在重连',
  offline: '离线',
}

export function ChatView({
  client,
  snapshot,
  me,
  onLogout,
}: {
  client: ImClient
  snapshot: Snapshot
  me: Profile
  onLogout: () => Promise<void>
}) {
  const [search, setSearch] = useState('')
  const [drafts, setDrafts] = useState<Record<string, string>>({})
  const [dialogOpen, setDialogOpen] = useState(false)
  const [dialogBusy, setDialogBusy] = useState(false)
  const [dialogError, setDialogError] = useState('')
  const [error, setError] = useState('')
  const [copied, setCopied] = useState(false)
  const [loggingOut, setLoggingOut] = useState(false)
  const [atBottom, setAtBottom] = useState(true)
  const viewport = useRef<HTMLDivElement>(null)
  const sticky = useRef(true)
  const previousId = useRef<string | undefined>(undefined)
  const restoreScroll = useRef<{ id: string; height: number; top: number } | null>(null)
  const active = snapshot.active
  const id = active?.conversation_id
  const messages = id ? (snapshot.messages[id] ?? emptyMessages) : emptyMessages
  const pending = useMemo(
    () => snapshot.pending.filter((message) => message.target.conversation_id === id),
    [snapshot.pending, id],
  )
  const conversation = snapshot.conversations.find((item) => item.conversation_id === id)
  const draft = id ? (drafts[id] ?? '') : ''
  const lastSeq = messages.at(-1)?.seq ?? 0
  const hasOlder = Boolean(conversation && messages[0] && messages[0].seq > conversation.min_seq)
  const activeName =
    active?.session_type === 1
      ? snapshot.profiles[active.recv_id ?? '']?.nickname || active.title
      : active?.title
  const report = useCallback((reason: unknown) => {
    if (reason instanceof Error && reason.name === 'AbortError') return
    setError(errorMessage(reason))
  }, [])
  const selectConversation = useCallback(
    (conversationId: string) => {
      setError('')
      void client.select(conversationId).catch(report)
    },
    [client, report],
  )
  const retryMessage = useCallback(
    (clientMsgId: string) => {
      void client.retry(clientMsgId).catch(report)
    },
    [client, report],
  )

  function scrolled() {
    const node = viewport.current
    if (!node) return
    const nearBottom = node.scrollHeight - node.scrollTop - node.clientHeight < 32
    sticky.current = nearBottom
    setAtBottom(nearBottom)
  }

  useLayoutEffect(() => {
    const node = viewport.current
    if (!node) return
    const savedScroll = restoreScroll.current
    if (previousId.current !== id) {
      previousId.current = id
      restoreScroll.current = null
      sticky.current = true
      node.scrollTop = node.scrollHeight
    } else if (savedScroll && savedScroll.id === id) {
      if (!snapshot.loadingHistory) {
        node.scrollTop = savedScroll.top + node.scrollHeight - savedScroll.height
        restoreScroll.current = null
      }
    } else if (sticky.current) node.scrollTop = node.scrollHeight
    scrolled()
  }, [id, messages, pending.length, snapshot.loadingHistory])

  useEffect(() => {
    function readVisible() {
      if (
        id &&
        atBottom &&
        lastSeq > 0 &&
        !snapshot.loadingHistory &&
        !snapshot.syncing &&
        document.visibilityState === 'visible'
      ) {
        void client.markRead(id, lastSeq).catch(report)
      }
    }
    readVisible()
    window.addEventListener('focus', readVisible)
    document.addEventListener('visibilitychange', readVisible)
    return () => {
      window.removeEventListener('focus', readVisible)
      document.removeEventListener('visibilitychange', readVisible)
    }
  }, [client, id, atBottom, lastSeq, snapshot.loadingHistory, snapshot.syncing, report])

  const unsent = snapshot.pending.length > 0 || Object.values(drafts).some((value) => value.trim())
  useEffect(() => {
    if (!unsent) return
    const warn = (event: BeforeUnloadEvent) => {
      event.preventDefault()
      event.returnValue = ''
    }
    window.addEventListener('beforeunload', warn)
    return () => window.removeEventListener('beforeunload', warn)
  }, [unsent])

  async function copyId() {
    try {
      await navigator.clipboard.writeText(me.user_id)
      setCopied(true)
    } catch {
      setError('复制失败，请手动复制下方的用户 ID')
    }
  }

  async function createChat(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const value = String(new FormData(event.currentTarget).get('user_id') ?? '')
    setDialogBusy(true)
    setDialogError('')
    try {
      await client.openDirect(value)
      setDialogOpen(false)
    } catch (reason) {
      setDialogError(errorMessage(reason))
    } finally {
      setDialogBusy(false)
    }
  }

  function send(event?: FormEvent) {
    event?.preventDefault()
    if (!id || !draft.trim()) return
    if (new TextEncoder().encode(JSON.stringify({ text: draft.trim() })).length > 8192) {
      setError('消息过长：编码后不能超过 8192 字节')
      return
    }
    setDrafts((current) => ({ ...current, [id]: '' }))
    sticky.current = true
    setAtBottom(true)
    void client.send(draft).catch((reason) => {
      setDrafts((current) => ({ ...current, [id]: current[id] || draft }))
      report(reason)
    })
  }

  async function loadOlder() {
    const node = viewport.current
    if (!node || !id) return
    restoreScroll.current = { id, height: node.scrollHeight, top: node.scrollTop }
    try {
      await client.loadOlder()
    } catch (reason) {
      restoreScroll.current = null
      report(reason)
    }
  }

  async function logout() {
    if (unsent && !window.confirm('还有未发送的内容，退出后不会保留。确认退出？')) return
    setLoggingOut(true)
    try {
      await onLogout()
    } catch (reason) {
      report(reason)
      setLoggingOut(false)
    }
  }

  return (
    <Dialog.Root
      open={dialogOpen}
      onOpenChange={(open) => {
        setDialogOpen(open)
        if (open) setDialogError('')
      }}
    >
      <main className="mx-auto flex h-dvh max-w-[1600px] flex-col p-0 md:p-6 lg:p-10">
        <div className="mb-5 hidden shrink-0 items-center justify-between px-1 md:flex">
          <div className="flex items-center gap-2.5">
            <span className="flex size-8 items-center justify-center rounded-xl bg-accent text-white">
              <ChatBubbleIcon className="size-4" />
            </span>
            <span className="text-lg font-bold tracking-tight">Nexo</span>
            <span className="ml-1 rounded-md border border-line bg-white px-2 py-0.5 text-[10px] font-semibold tracking-widest text-muted">
              WEB DEMO
            </span>
          </div>
          <span className="text-xs text-muted">简单连接，认真对话。</span>
        </div>
        <div className="flex min-h-0 flex-1 overflow-hidden border-line bg-white md:rounded-2xl md:border md:shadow-xl md:shadow-slate-200/30">
          <aside
            className={`${active ? 'hidden md:flex' : 'flex'} w-full shrink-0 flex-col border-r border-line bg-[#fcfdff] md:w-72 lg:w-80`}
          >
            <div className="p-5 pb-4">
              <div className="mb-5 flex items-center justify-between">
                <div className="flex items-center gap-2.5">
                  <h1 className="text-xl font-semibold">消息</h1>
                  <span className="rounded-lg bg-slate-100 px-2 py-0.5 text-xs text-muted">
                    {snapshot.conversations.length}
                  </span>
                </div>
                <Dialog.Trigger asChild>
                  <button
                    className="icon-button bg-accent/8 text-accent hover:bg-accent/15"
                    aria-label="发起聊天"
                  >
                    <PlusIcon className="size-5" />
                  </button>
                </Dialog.Trigger>
              </div>
              <label className="flex items-center gap-2 rounded-xl border border-line bg-white px-3 text-muted focus-within:border-accent">
                <MagnifyingGlassIcon className="size-4 shrink-0" />
                <input
                  className="min-w-0 flex-1 bg-transparent py-2.5 text-sm text-ink outline-none"
                  aria-label="搜索会话"
                  placeholder="搜索当前会话"
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                />
              </label>
            </div>
            <ConversationList
              conversations={snapshot.conversations}
              profiles={snapshot.profiles}
              activeId={id}
              search={search}
              syncing={snapshot.syncing}
              onSelect={selectConversation}
            />
            <div className="border-t border-line p-4">
              <div
                role="status"
                className="mb-4 flex items-center justify-between text-xs text-muted"
              >
                <span className="flex items-center gap-2">
                  <span
                    className={`size-1.5 rounded-full ${snapshot.status === 'online' ? 'bg-emerald-500' : snapshot.status === 'offline' ? 'bg-slate-400' : 'bg-amber-500'}`}
                  />
                  {statusText[snapshot.status]}
                  {snapshot.syncing && ' · 同步中'}
                </span>
                <button
                  className="rounded-lg p-1 hover:bg-slate-100 disabled:opacity-40"
                  aria-label="重新同步"
                  disabled={snapshot.syncing}
                  onClick={() => {
                    client.clearError()
                    void client.refresh().catch(report)
                  }}
                >
                  <ReloadIcon />
                </button>
              </div>
              <div className="flex items-center gap-2.5">
                <PersonAvatar name={me.nickname} url={me.avatar} />
                <div className="min-w-0 flex-1">
                  <p className="truncate text-sm font-semibold">{me.nickname}</p>
                  <p className="mt-0.5 truncate text-[10px] text-muted" title={me.user_id}>
                    {me.user_id}
                  </p>
                </div>
                <DropdownMenu.Root>
                  <DropdownMenu.Trigger asChild>
                    <button aria-label="账号菜单" className="icon-button">
                      <DotsHorizontalIcon />
                    </button>
                  </DropdownMenu.Trigger>
                  <DropdownMenu.Portal>
                    <DropdownMenu.Content
                      sideOffset={8}
                      align="end"
                      className="z-40 min-w-44 rounded-xl border border-line bg-white p-1.5 text-sm shadow-lg"
                    >
                      <DropdownMenu.Item
                        className="flex cursor-pointer items-center gap-2 rounded-lg px-3 py-2 outline-none data-highlighted:bg-slate-100"
                        onSelect={() => void copyId()}
                      >
                        {copied ? <CheckIcon /> : <CopyIcon />}
                        {copied ? '已复制用户 ID' : '复制用户 ID'}
                      </DropdownMenu.Item>
                      <DropdownMenu.Separator className="my-1 h-px bg-line" />
                      <DropdownMenu.Item
                        disabled={loggingOut}
                        className="flex cursor-pointer items-center gap-2 rounded-lg px-3 py-2 text-red-600 outline-none data-highlighted:bg-red-50 data-disabled:opacity-50"
                        onSelect={() => void logout()}
                      >
                        <ExitIcon />
                        {loggingOut ? '正在退出…' : '退出登录'}
                      </DropdownMenu.Item>
                    </DropdownMenu.Content>
                  </DropdownMenu.Portal>
                </DropdownMenu.Root>
              </div>
            </div>
          </aside>
          <section
            aria-label="聊天区域"
            className={`${active ? 'flex' : 'hidden md:flex'} relative min-w-0 flex-1 flex-col`}
          >
            {(error || snapshot.error) && (
              <div
                role="alert"
                className="flex shrink-0 items-center gap-3 border-b border-amber-100 bg-amber-50 px-5 py-3 text-xs leading-5 text-amber-900"
              >
                <span className="flex-1">{error || snapshot.error}</span>
                <button
                  aria-label="关闭错误提示"
                  className="rounded p-1 hover:bg-amber-100"
                  onClick={() => {
                    setError('')
                    client.clearError()
                  }}
                >
                  <Cross2Icon />
                </button>
              </div>
            )}
            {active ? (
              <>
                <header className="flex h-20 shrink-0 items-center gap-3 border-b border-line px-4 sm:px-7">
                  <button
                    className="icon-button md:hidden"
                    aria-label="返回会话列表"
                    onClick={() => client.back()}
                  >
                    <ArrowLeftIcon className="size-5" />
                  </button>
                  <PersonAvatar
                    name={activeName || '?'}
                    url={snapshot.profiles[active.recv_id ?? '']?.avatar}
                    group={active.session_type === 2}
                  />
                  <div className="min-w-0 flex-1">
                    <h2 className="truncate text-sm font-semibold">{activeName}</h2>
                    <p
                      className="mt-1 truncate text-[11px] text-muted"
                      title={active.recv_id || active.group_id}
                    >
                      {active.session_type === 2 ? '群聊' : '单聊'} ·{' '}
                      {active.recv_id || active.group_id}
                    </p>
                  </div>
                  <span className="hidden rounded-full bg-slate-50 px-3 py-1 text-[10px] text-muted sm:inline">
                    按序同步
                  </span>
                </header>
                <div
                  ref={viewport}
                  onScroll={scrolled}
                  aria-label="聊天记录"
                  className="min-h-0 flex-1 overflow-y-auto bg-[#fafbfe] px-4 py-5 sm:px-7"
                >
                  {hasOlder && (
                    <div className="mb-6 text-center">
                      <button
                        disabled={snapshot.loadingHistory}
                        onClick={() => void loadOlder()}
                        className="inline-flex items-center gap-1.5 rounded-full border border-line bg-white px-4 py-2 text-xs text-muted hover:text-accent disabled:opacity-50"
                      >
                        <ArrowUpIcon />
                        {snapshot.loadingHistory ? '加载中…' : '加载更早消息'}
                      </button>
                    </div>
                  )}
                  <MessageList
                    messages={messages}
                    pending={pending}
                    me={me}
                    profiles={snapshot.profiles}
                    group={active.session_type === 2}
                    loadingHistory={snapshot.loadingHistory}
                    onRetry={retryMessage}
                  />
                </div>
                {!atBottom && (
                  <button
                    className="absolute right-5 bottom-40 flex items-center gap-1 rounded-full border border-line bg-white px-3 py-2 text-xs text-accent shadow-md"
                    onClick={() => {
                      if (viewport.current)
                        viewport.current.scrollTop = viewport.current.scrollHeight
                      scrolled()
                    }}
                  >
                    回到最新
                    <ChevronDownIcon />
                  </button>
                )}
                <form
                  onSubmit={send}
                  className="shrink-0 border-t border-line bg-white px-4 pt-4 pb-3 sm:px-7"
                >
                  <textarea
                    aria-label="消息内容"
                    placeholder="输入消息…"
                    rows={2}
                    value={draft}
                    onChange={(event) => {
                      if (id) setDrafts((current) => ({ ...current, [id]: event.target.value }))
                    }}
                    onKeyDown={(event) => {
                      if (
                        event.key === 'Enter' &&
                        !event.shiftKey &&
                        !event.nativeEvent.isComposing &&
                        event.keyCode !== 229
                      ) {
                        event.preventDefault()
                        send()
                      }
                    }}
                    className="w-full resize-none rounded-lg bg-transparent px-1 py-1 text-sm leading-6 outline-none placeholder:text-muted focus-visible:ring-2 focus-visible:ring-accent"
                  />
                  <div className="mt-2 flex items-center justify-between gap-2">
                    <span className="text-[10px] text-muted">Enter 发送 · Shift + Enter 换行</span>
                    <button
                      type="submit"
                      className="primary-button rounded-lg px-4 py-2"
                      disabled={!draft.trim()}
                    >
                      <span>发送</span>
                      <PaperPlaneIcon />
                    </button>
                  </div>
                </form>
              </>
            ) : (
              <div className="flex flex-1 flex-col items-center justify-center px-8 text-center">
                <div className="mb-7 flex size-24 items-center justify-center rounded-3xl border border-blue-100 bg-blue-50/60 text-accent">
                  <ChatBubbleIcon className="size-10" />
                </div>
                <h2 className="text-xl font-semibold tracking-tight">你的下一段对话，从这里开始</h2>
                <p className="mt-3 max-w-xs text-sm leading-7 text-muted">
                  选择左侧会话，或者通过用户 ID
                  <br />
                  和朋友打个招呼。
                </p>
                <button className="primary-button mt-7" onClick={() => setDialogOpen(true)}>
                  <PlusIcon />
                  发起新对话
                </button>
                <p className="mt-16 text-xs text-slate-400">Nexo · 轻量、可靠的即时通讯</p>
              </div>
            )}
          </section>
        </div>
        {!active && (error || snapshot.error) && (
          <div
            role="alert"
            className="border-t border-amber-100 bg-amber-50 px-4 py-3 text-xs text-amber-900 md:hidden"
          >
            {error || snapshot.error}
            <button
              className="ml-3 underline"
              onClick={() => {
                setError('')
                client.clearError()
              }}
            >
              关闭
            </button>
          </div>
        )}
      </main>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-40 bg-slate-950/30 backdrop-blur-xs" />
        <Dialog.Content className="fixed top-1/2 left-1/2 z-50 w-[calc(100%-2.5rem)] max-w-md -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-line bg-white p-7 shadow-2xl">
          <Dialog.Title className="text-xl font-semibold">发起聊天</Dialog.Title>
          <Dialog.Description className="mt-2 text-sm leading-6 text-muted">
            输入对方完整的用户 ID。第一条消息发送成功后，会话会自动保存。
          </Dialog.Description>
          <Dialog.Close asChild>
            <button className="icon-button absolute top-4 right-4" aria-label="关闭对话框">
              <Cross2Icon className="size-5" />
            </button>
          </Dialog.Close>
          <form onSubmit={createChat} className="mt-6 space-y-4">
            <label className="block space-y-2 text-sm font-medium">
              <span>用户 ID</span>
              <input
                className="field"
                name="user_id"
                placeholder="nx__… / u___… / ag__…"
                autoComplete="off"
                required
                disabled={dialogBusy}
              />
            </label>
            {dialogError && (
              <p role="alert" className="text-sm text-red-600">
                {dialogError}
              </p>
            )}
            <button disabled={dialogBusy} className="primary-button w-full">
              {dialogBusy ? '查找用户…' : '开始聊天'}
            </button>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
