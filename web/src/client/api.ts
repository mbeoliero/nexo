export class ApiError extends Error {
  constructor(
    message: string,
    public readonly code: number,
    public readonly status: number,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

export class Api {
  constructor(
    private readonly token = '',
    private readonly unauthorized?: () => void,
  ) {}

  async request<T>(
    path: string,
    options: {
      body?: unknown
      query?: Record<string, string | number>
      signal?: AbortSignal
      empty?: boolean
    } = {},
  ): Promise<T> {
    const query = new URLSearchParams()
    for (const [key, value] of Object.entries(options.query ?? {})) query.set(key, String(value))
    const signal = options.signal
      ? AbortSignal.any([options.signal, AbortSignal.timeout(15_000)])
      : AbortSignal.timeout(15_000)
    const fail: (error: ApiError) => never = (error) => {
      signal.throwIfAborted()
      if (this.token && error.status === 401) this.unauthorized?.()
      throw error
    }
    const response = await fetch(`/api/v1${path}${query.size ? `?${query}` : ''}`, {
      method: options.body === undefined ? 'GET' : 'POST',
      headers: {
        ...(options.body === undefined ? {} : { 'Content-Type': 'application/json' }),
        ...(this.token ? { Authorization: `Bearer ${this.token}`, 'X-Platform-Id': '5' } : {}),
      },
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
      signal,
      cache: 'no-store',
    })
    signal.throwIfAborted()
    let envelope: { code?: unknown; message?: unknown; data?: T } | null
    try {
      envelope = await response.json()
    } catch {
      fail(
        new ApiError(
          `服务响应不是有效 JSON（HTTP ${response.status}），请检查后端和代理配置`,
          20001,
          response.status,
        ),
      )
    }
    signal.throwIfAborted()
    if (!envelope || typeof envelope !== 'object' || !Number.isInteger(envelope.code)) {
      fail(new ApiError('服务响应格式无效', 20001, response.status))
    }
    if (!response.ok || envelope.code !== 0) {
      fail(
        new ApiError(
          typeof envelope.message === 'string'
            ? envelope.message
            : `请求失败（HTTP ${response.status}）`,
          Number(envelope.code) || 20001,
          response.status,
        ),
      )
    }
    if (!options.empty && envelope.data == null)
      fail(new ApiError('服务响应缺少 data', 20001, response.status))
    return envelope.data as T
  }
}

export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) return `${error.message}（${error.code}）`
  if (error instanceof Error && error.name === 'TimeoutError') return '请求超时，请稍后重试'
  if (error instanceof TypeError) return '无法连接服务，请检查网络或后端地址'
  return error instanceof Error ? error.message : '操作失败，请重试'
}
