import { useEffect, useState, useSyncExternalStore } from 'react'
import { Api, errorMessage } from './client/api'
import { ImClient } from './client/client'
import type { Profile, Session } from './client/types'
import { AuthScreen } from './components/AuthScreen'
import { ChatView } from './components/ChatView'

const sessionKey = 'nexo:web:session'

// The stored session is whatever the last version of this app wrote, so every field is checked
// rather than trusted: JSON.parse types it as any, which would make the guard below a no-op.
function isSession(value: unknown): value is Session {
  return (
    typeof value === 'object' &&
    value !== null &&
    'token' in value &&
    typeof value.token === 'string' &&
    value.token !== '' &&
    'user_id' in value &&
    typeof value.user_id === 'string' &&
    'expires_at' in value &&
    typeof value.expires_at === 'number' &&
    Number.isFinite(value.expires_at)
  )
}

function restoreSession(): Session | null {
  try {
    const value: unknown = JSON.parse(sessionStorage.getItem(sessionKey) || 'null')
    if (isSession(value) && value.expires_at > Date.now()) return value
    sessionStorage.removeItem(sessionKey)
  } catch {
    /* Private browsers can disable session storage. */
  }
  return null
}

export function App() {
  const [session, setSession] = useState<Session | null>(restoreSession)
  const [notice, setNotice] = useState('')

  function setAuthentication(next: Session | null, reason = '') {
    try {
      if (next) sessionStorage.setItem(sessionKey, JSON.stringify(next))
      else sessionStorage.removeItem(sessionKey)
    } catch {
      /* Keep the session in memory when storage is unavailable. */
    }
    setNotice(reason)
    setSession(next)
  }

  async function authenticate(username: string, password: string, nickname?: string) {
    const api = new Api()
    try {
      if (nickname !== undefined)
        await api.request('/auth/register', { body: { username, password, nickname } })
      const next = await api.request<Session>('/auth/login', {
        body: { username, password, platform_id: 5 },
      })
      if (
        !next.token ||
        !next.user_id ||
        !Number.isFinite(next.expires_at) ||
        next.expires_at <= Date.now()
      )
        throw new Error('登录响应缺少有效凭据')
      setAuthentication(next)
    } catch (error) {
      throw new Error(errorMessage(error))
    }
  }

  return session ? (
    <SessionView
      key={session.token}
      session={session}
      onEnd={(reason) => setAuthentication(null, reason)}
    />
  ) : (
    <AuthScreen onSubmit={authenticate} notice={notice} />
  )
}

function SessionView({ session, onEnd }: { session: Session; onEnd: (reason: string) => void }) {
  // The token-keyed component owns one client lifetime; UI state never leaks across accounts.
  const [client] = useState(() => new ImClient(session, onEnd))
  const snapshot = useSyncExternalStore(client.subscribe, client.getSnapshot)
  const [profile, setProfile] = useState<Profile>({
    user_id: session.user_id,
    nickname: session.user_id,
    avatar: '',
  })
  useEffect(() => {
    client.start()
    void client
      .me()
      .then(setProfile)
      .catch(() => {
        /* The client handles 401; a transient profile failure keeps the user ID visible. */
      })
    return () => {
      client.stop()
    }
  }, [client])

  async function logout() {
    try {
      await client.logout()
    } catch (error) {
      // Failed revocation must not be presented as a successful server-side logout.
      throw new Error(`退出失败：${errorMessage(error)}。请重试。`)
    }
  }

  return <ChatView client={client} snapshot={snapshot} me={profile} onLogout={logout} />
}
