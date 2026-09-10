import { describe, expect, it, vi } from 'vitest'
import { Api } from './api'
import { mergeMessages, syncRange } from './sync'
import type { Message } from './types'

const message = (seq: number): Message => ({
  seq,
  conversation_id: 'c',
  server_msg_id: `s${seq}`,
  client_msg_id: `m${seq}`,
  sender_id: 'u',
  session_type: 1,
  content_type: 1,
  content: '{"text":"hello"}',
  send_time: seq,
})
const bounds = { conversation_id: 'c', min_seq: 1, max_seq: 3, read_seq: 0 }

describe('continuous synchronization', () => {
  it('fills a gap in pages and commits only received contiguous messages', async () => {
    const api = new Api()
    const request = vi
      .spyOn(api, 'request')
      .mockResolvedValueOnce({ messages: [message(1), message(2)], has_more: true })
      .mockResolvedValueOnce({ messages: [message(3)], has_more: false })
    const commit = vi.fn()
    await syncRange(api, bounds, 0, commit, new AbortController().signal)
    expect(commit.mock.calls.map((call) => call[1])).toEqual([0, 2, 3])
    expect(request.mock.calls.map((call) => call[1]?.query?.begin_seq)).toEqual([1, 3])
  })

  it('does not advance over a missing message or an empty page', async () => {
    for (const messages of [[], [message(3)]]) {
      const api = new Api()
      vi.spyOn(api, 'request').mockResolvedValue({ messages, has_more: false })
      const commit = vi.fn()
      await expect(
        syncRange(api, bounds, 1, commit, new AbortController().signal),
      ).rejects.toThrow()
      expect(commit.mock.calls.map((call) => call[1])).toEqual([1])
    }
  })

  it('retries from the last committed page after a network failure', async () => {
    const api = new Api()
    vi.spyOn(api, 'request')
      .mockResolvedValueOnce({ messages: [message(1)], has_more: true })
      .mockRejectedValueOnce(new TypeError('offline'))
    const commit = vi.fn()
    await expect(syncRange(api, bounds, 0, commit, new AbortController().signal)).rejects.toThrow(
      'offline',
    )
    expect(commit.mock.calls.map((call) => call[1])).toEqual([0, 1])
  })

  it('raises the lower baseline on rejoin and clamps a cursor after server reset', async () => {
    const api = new Api()
    const request = vi
      .spyOn(api, 'request')
      .mockResolvedValue({ messages: [message(10)], has_more: false })
    const commit = vi.fn()
    await syncRange(
      api,
      { ...bounds, min_seq: 10, max_seq: 10 },
      3,
      commit,
      new AbortController().signal,
    )
    expect(commit.mock.calls.map((call) => call[1])).toEqual([9, 10])
    expect(request.mock.calls[0][1]?.query?.begin_seq).toBe(10)
    request.mockClear()
    commit.mockClear()
    await syncRange(
      api,
      { ...bounds, min_seq: 4, max_seq: 3 },
      20,
      commit,
      new AbortController().signal,
    )
    expect(commit).toHaveBeenCalledWith([], 3)
    expect(request).not.toHaveBeenCalled()
  })

  it('aborted sessions cannot commit late HTTP responses', async () => {
    const api = new Api()
    const controller = new AbortController()
    vi.spyOn(api, 'request').mockImplementation(async () => {
      controller.abort()
      return { messages: [message(1)], has_more: false }
    })
    const commit = vi.fn()
    await expect(syncRange(api, bounds, 0, commit, controller.signal)).rejects.toThrow()
    expect(commit).toHaveBeenCalledTimes(1)
  })

  it('deduplicates and orders push/pull copies by sequence', () => {
    expect(
      mergeMessages([message(2), message(3)], [message(1), message(2)]).map((item) => item.seq),
    ).toEqual([1, 2, 3])
  })
})
