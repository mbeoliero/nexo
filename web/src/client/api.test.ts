import { afterEach, describe, expect, it, vi } from 'vitest'
import { Api, ApiError } from './api'
import { directId, messageText } from './types'

afterEach(() => vi.unstubAllGlobals())

describe('HTTP envelope', () => {
  it.each([
    null,
    [],
    {},
    { code: null },
    { code: '0' },
    { code: 0.5 },
    { code: 0 },
    { code: 0, data: null },
  ])('rejects malformed success %j', async (body) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(body))))
    await expect(new Api().request('/user/me')).rejects.toBeInstanceOf(ApiError)
  })

  it('checks business codes even when HTTP status is 200', async () => {
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValue(
          new Response(JSON.stringify({ code: 10105, message: 'login failed', data: {} })),
        ),
    )
    await expect(new Api().request('/auth/login', { body: {} })).rejects.toMatchObject({
      code: 10105,
      status: 200,
    })
  })

  it('encodes query values and keeps credentials in the authorization header', async () => {
    const request = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify({ code: 0, data: { users: [] } })))
    vi.stubGlobal('fetch', request)
    await new Api('private-token').request('/user/info', { query: { user_ids: 'u:a&b' } })
    expect(request.mock.calls[0][0]).toBe('/api/v1/user/info?user_ids=u%3Aa%26b')
    expect(request.mock.calls[0][1].headers.Authorization).toBe('Bearer private-token')
    expect(request.mock.calls[0][1].headers['X-Platform-Id']).toBe('5')
  })

  it('accepts empty data only for explicitly void operations', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{"code":0}')))
    await expect(
      new Api().request('/auth/logout', { body: {}, empty: true }),
    ).resolves.toBeUndefined()
  })

  it.each([
    { status: 401, body: '{"code":10102,"data":{}}', endsSession: true },
    { status: 401, body: 'proxy authentication refusal', endsSession: true },
    { status: 503, body: '{"code":20101,"message":"unavailable","data":{}}', endsSession: false },
  ])('handles authentication by status $status while preserving dependency failures', async ({ status, body, endsSession }) => {
    const unauthorized = vi.fn()
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(body, { status })))
    await expect(new Api('token', unauthorized).request('/user/me')).rejects.toMatchObject({ status })
    expect(unauthorized).toHaveBeenCalledTimes(endsSession ? 1 : 0)
  })
})

it('formats content defensively without assuming typed text', () => {
  expect(messageText({ content_type: 1, content: '{"text":"<script>"}' })).toBe('<script>')
  expect(messageText({ content_type: 1, content: 'null' })).toBe('[无法解析的文本消息]')
  expect(messageText({ content_type: 1, content: 'not json' })).toBe('[无法解析的文本消息]')
  expect(messageText({ content_type: 2, content: '{}' })).toBe('[图片]')
  expect(directId('nx__b', 'nx__a')).toBe('si_nx__a:nx__b')
})
