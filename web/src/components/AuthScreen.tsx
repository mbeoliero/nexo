import { useState, type FormEvent } from 'react'
import { ArrowRightIcon, ChatBubbleIcon, CheckIcon } from '@radix-ui/react-icons'

export function AuthScreen({
  onSubmit,
  notice,
}: {
  onSubmit: (username: string, password: string, nickname?: string) => Promise<void>
  notice: string
}) {
  const [register, setRegister] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const form = new FormData(event.currentTarget)
    setBusy(true)
    setError('')
    try {
      await onSubmit(
        String(form.get('username')).trim(),
        String(form.get('password')),
        register ? String(form.get('nickname')).trim() : undefined,
      )
    } catch (error) {
      setError(error instanceof Error ? error.message : '登录失败，请重试')
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="flex min-h-dvh items-center justify-center p-5 sm:p-10">
      <div className="grid w-full max-w-5xl overflow-hidden rounded-3xl border border-line bg-white shadow-xl shadow-slate-200/40 md:grid-cols-2">
        <section className="relative hidden flex-col justify-between overflow-hidden bg-[#edf2ff] p-12 md:flex">
          <div className="absolute -right-24 -top-24 size-80 rounded-full bg-white/50" />
          <div className="relative flex items-center gap-3 text-2xl font-bold tracking-tight">
            <span className="flex size-10 items-center justify-center rounded-xl bg-accent text-white">
              <ChatBubbleIcon className="size-6" />
            </span>
            Nexo
            <span className="rounded-md border border-accent/20 px-2 py-0.5 text-[10px] font-medium tracking-widest text-accent">
              WEB
            </span>
          </div>
          <div className="relative my-16">
            <p className="mb-4 text-xs font-semibold tracking-[0.25em] text-accent">
              STAY CONNECTED
            </p>
            <h1 className="text-4xl leading-snug font-semibold tracking-tight">
              让每一次对话，
              <br />
              简单发生。
            </h1>
            <p className="mt-5 max-w-xs text-sm leading-7 text-muted">
              轻量的即时通讯客户端。连接彼此，也让错过的消息重新回到眼前。
            </p>
            <div className="mt-9 space-y-3">
              {['实时收发，专注对话', '断线重连，自动补齐', '简洁界面，清晰状态'].map((text) => (
                <div key={text} className="flex items-center gap-2.5 text-sm text-slate-600">
                  <span className="flex size-5 items-center justify-center rounded-full bg-accent/10 text-accent">
                    <CheckIcon />
                  </span>
                  {text}
                </div>
              ))}
            </div>
          </div>
          <p className="relative text-xs text-muted">Nexo IM · 开源 Web Demo</p>
        </section>
        <section className="px-7 py-12 sm:px-12 md:py-16">
          <div className="mb-10 flex items-center gap-2 font-bold md:hidden">
            <ChatBubbleIcon className="size-6 text-accent" />
            Nexo
          </div>
          <h2 className="text-2xl font-semibold tracking-tight">
            {register ? '创建你的账号' : '欢迎回来'}
          </h2>
          <p className="mt-2 text-sm text-muted">
            {register ? '注册后即可开始第一段对话。' : '登录 Nexo，继续你的对话。'}
          </p>
          {notice && (
            <p role="status" className="mt-5 rounded-xl bg-amber-50 p-3 text-sm text-amber-800">
              {notice}
            </p>
          )}
          <form onSubmit={submit} className="mt-8 space-y-5">
            <label className="block space-y-2 text-sm font-medium">
              <span>用户名</span>
              <input
                className="field"
                name="username"
                autoComplete="username"
                placeholder="输入用户名"
                required
                maxLength={64}
                disabled={busy}
              />
            </label>
            {register && (
              <label className="block space-y-2 text-sm font-medium">
                <span>昵称</span>
                <input
                  className="field"
                  name="nickname"
                  autoComplete="nickname"
                  placeholder="其他人将看到这个名字"
                  required
                  maxLength={64}
                  disabled={busy}
                />
              </label>
            )}
            <label className="block space-y-2 text-sm font-medium">
              <span>密码</span>
              <input
                className="field"
                name="password"
                type="password"
                autoComplete={register ? 'new-password' : 'current-password'}
                placeholder={register ? '至少 8 位字符' : '输入密码'}
                minLength={register ? 8 : 1}
                required
                disabled={busy}
              />
            </label>
            {error && (
              <p role="alert" className="rounded-xl bg-red-50 p-3 text-sm text-red-700">
                {error}
              </p>
            )}
            <button className="primary-button mt-2 w-full" disabled={busy}>
              {busy ? '请稍候…' : register ? '注册并登录' : '登录'}
              {!busy && <ArrowRightIcon />}
            </button>
          </form>
          <p className="mt-7 text-center text-sm text-muted">
            {register ? '已经有账号？' : '还没有账号？'}
            <button
              className="ml-1 font-medium text-accent hover:underline disabled:opacity-50"
              disabled={busy}
              onClick={() => {
                setRegister(!register)
                setError('')
              }}
            >
              {register ? '立即登录' : '创建账号'}
            </button>
          </p>
          <p className="mt-10 text-center text-xs leading-6 text-muted">
            使用 Nexo 原生账号登录
            <br />
            服务端需开启 native 认证
          </p>
        </section>
      </div>
    </main>
  )
}
