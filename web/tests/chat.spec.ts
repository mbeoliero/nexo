import { expect, test, type Page, type WebSocketRoute } from '@playwright/test'
import { directId, type Message } from '../src/client/types'

const alice = { user_id: 'nx__019a0000-0000-7000-8000-000000000001', nickname: 'Alice', avatar: '' }
const bob = { user_id: 'nx__019a0000-0000-7000-8000-000000000002', nickname: 'Bob', avatar: '' }
const conversationId = directId(alice.user_id, bob.user_id)

async function mockServer(page: Page, count = 1) {
  const messages: Message[] = Array.from({ length: count }, (_, index) => ({
    conversation_id: conversationId,
    seq: index + 1,
    server_msg_id: `m${index + 1}`,
    client_msg_id: `c${index + 1}`,
    session_type: 1,
    sender_id: bob.user_id,
    recv_id: alice.user_id,
    content_type: 1,
    content: JSON.stringify({ text: count === 1 ? '你好，Alice' : `历史消息 ${index + 1}` }),
    send_time: Date.now() + index,
  }))
  let readSeq = 0
  let loseAck = false
  let socket: WebSocketRoute | undefined
  let connections = 0
  const sends: Record<string, unknown>[] = []
  const reads: number[] = []
  const bounds = () => ({
    conversation_id: conversationId,
    min_seq: 1,
    max_seq: messages.length,
    read_seq: readSeq,
  })
  await page.routeWebSocket(/\/ws\?/, (ws) => {
    socket = ws
    connections++
  })
  await page.route('**/api/v1/**', async (route) => {
    const url = new URL(route.request().url())
    const path = url.pathname.replace('/api/v1', '')
    const body = route.request().method() === 'POST' ? route.request().postDataJSON() : {}
    let data: unknown
    if (path === '/auth/login')
      data = { user_id: alice.user_id, token: 'demo-token', expires_at: Date.now() + 3_600_000 }
    else if (path === '/auth/register' || path === '/user/me') data = alice
    else if (path === '/user/info') data = { users: [bob] }
    else if (path === '/auth/logout') data = {}
    else if (path === '/conversation/list')
      data = {
        conversations: messages.length
          ? [
              {
                ...bounds(),
                type: 1,
                peer_user_id: bob.user_id,
                unread: Math.max(0, messages.length - readSeq),
                updated_at: messages.at(-1)!.send_time,
                last_message: messages.at(-1),
              },
            ]
          : [],
        has_more: false,
        next_cursor: '',
      }
    else if (path === '/message/max_seqs')
      data = { items: messages.length ? [bounds()] : [], has_more: false, next_cursor: '' }
    else if (path === '/message/pull') {
      const selected = messages.filter(
        (message) =>
          message.seq >= Number(url.searchParams.get('begin_seq')) &&
          message.seq <= Number(url.searchParams.get('end_seq')),
      )
      data = { messages: selected.slice(0, 100), has_more: selected.length > 100 }
    } else if (path === '/conversation/read') {
      reads.push(body.read_seq)
      readSeq = Math.max(readSeq, body.read_seq)
      data = { read_seq: readSeq }
    } else if (path === '/message/send') {
      sends.push(body)
      let message = messages.find(
        (item) => item.client_msg_id === body.client_msg_id && item.sender_id === alice.user_id,
      )
      if (!message) {
        message = {
          ...body,
          conversation_id: conversationId,
          server_msg_id: `m${messages.length + 1}`,
          seq: messages.length + 1,
          sender_id: alice.user_id,
          send_time: Date.now(),
        } as Message
        messages.push(message)
      }
      if (loseAck) {
        loseAck = false
        await route.abort('failed')
        return
      }
      data = {
        conversation_id: conversationId,
        server_msg_id: message.server_msg_id,
        seq: message.seq,
        send_time: message.send_time,
      }
    } else {
      await route.fulfill({
        status: 404,
        json: { code: 10001, message: `Unexpected test route: ${path}`, data: {} },
      })
      return
    }
    await route.fulfill({ json: { code: 0, message: '', data } })
  })
  return {
    messages,
    sends,
    reads,
    loseNextAck() {
      loseAck = true
    },
    get connections() {
      return connections
    },
    push(req_id: number, data: unknown) {
      socket!.send(JSON.stringify({ req_id, data }))
    },
    disconnect() {
      socket!.close({ code: 1001, reason: 'test reconnect' })
    },
    add(text: string) {
      const message: Message = {
        ...messages[0],
        conversation_id: conversationId,
        client_msg_id: `push${messages.length + 1}`,
        server_msg_id: `m${messages.length + 1}`,
        seq: messages.length + 1,
        sender_id: bob.user_id,
        session_type: 1,
        content_type: 1,
        content: JSON.stringify({ text }),
        send_time: Date.now(),
      }
      messages.push(message)
      return message
    },
  }
}

async function login(page: Page) {
  await page.goto('/')
  await page.getByLabel('用户名', { exact: true }).fill('alice')
  await page.getByLabel('密码', { exact: true }).fill('password123')
  await page.getByRole('button', { name: '登录', exact: true }).click()
  await expect(page.getByRole('status').filter({ hasText: '已连接' })).toBeVisible()
}

async function openBob(page: Page) {
  await page
    .getByRole('navigation', { name: '会话列表' })
    .getByRole('button')
    .filter({ hasText: 'Bob' })
    .click()
  await expect(page.getByRole('heading', { name: 'Bob', exact: true })).toBeVisible()
}

test('login, safe text rendering, send, and reload history', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(error.message))
  const server = await mockServer(page)
  await login(page)
  await openBob(page)
  await expect(page.getByLabel('聊天记录').getByText('你好，Alice', { exact: true })).toBeVisible()
  await page.getByLabel('消息内容').fill('<script>alert("not HTML")</script>')
  await page.getByLabel('消息内容').press('Enter')
  await expect(
    page.getByLabel('聊天记录').getByText('<script>alert("not HTML")</script>', { exact: true }),
  ).toBeVisible()
  await expect.poll(() => server.sends.length).toBe(1)
  await page.reload()
  await openBob(page)
  await expect(page.getByLabel('聊天记录').getByText('你好，Alice', { exact: true })).toBeVisible()
  await expect(
    page.getByLabel('聊天记录').getByText('<script>alert("not HTML")</script>', { exact: true }),
  ).toBeVisible()
  expect(errors).toEqual([])
  await page.screenshot({ path: test.info().outputPath('desktop.png'), fullPage: true })
})

test('new-chat dialog and idempotent retry after a lost ACK', async ({ page }) => {
  const server = await mockServer(page, 0)
  await login(page)
  await page.getByRole('button', { name: '发起聊天', exact: true }).click()
  await page.getByLabel('用户 ID', { exact: true }).fill(bob.user_id)
  await page.getByRole('button', { name: '开始聊天' }).click()
  server.loseNextAck()
  await page.getByLabel('消息内容').fill('第一条消息')
  await page.getByRole('button', { name: '发送', exact: true }).click()
  await page.getByRole('button', { name: '发送失败 · 点击重试' }).click()
  await expect(page.getByLabel('聊天记录').getByText('第一条消息', { exact: true })).toHaveCount(1)
  await expect.poll(() => server.sends.length).toBe(2)
  expect(server.sends[1]).toEqual(server.sends[0])
  expect(server.messages).toHaveLength(1)
})

test('reconciles the last missed push while the socket stays connected', async ({ page }) => {
  await page.clock.install()
  const server = await mockServer(page)
  await login(page)
  await openBob(page)
  await expect(page.getByLabel('聊天记录').getByText('你好，Alice', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: '重新同步' })).toBeEnabled()
  const connections = server.connections
  server.add('没有推送的最后一条')

  await page.clock.fastForward(37_000)

  await expect(
    page.getByLabel('聊天记录').getByText('没有推送的最后一条', { exact: true }),
  ).toBeVisible()
  expect(server.connections).toBe(connections)
})

test('a history 401 clears the session and returns to login', async ({ page }) => {
  await mockServer(page)
  await login(page)
  await expect(page.getByRole('button', { name: '重新同步' })).toBeEnabled()
  await page.route('**/api/v1/message/pull?*', (route) =>
    route.fulfill({ status: 401, json: { code: 10102, message: 'Token invalid' } }),
  )
  await page
    .getByRole('navigation', { name: '会话列表' })
    .getByRole('button')
    .filter({ hasText: 'Bob' })
    .click()

  await expect(page.getByRole('button', { name: '登录', exact: true })).toBeVisible()
  await expect(page.getByText('登录已失效，请重新登录', { exact: true })).toBeVisible()
  expect(await page.evaluate(() => sessionStorage.getItem('nexo:web:session'))).toBeNull()
})

test('fills a push gap, resyncs after reconnect, and never reconnects after kick', async ({
  page,
}) => {
  const server = await mockServer(page)
  await login(page)
  await openBob(page)
  server.add('缺失的第二条')
  const third = server.add('第三条')
  server.push(2001, third)
  await expect(page.getByLabel('聊天记录').getByText('缺失的第二条', { exact: true })).toBeVisible()
  await expect(page.getByLabel('聊天记录').getByText('第三条', { exact: true })).toHaveCount(1)
  const beforeReconnect = server.connections
  server.disconnect()
  server.add('断线期间的第四条')
  await expect.poll(() => server.connections).toBe(beforeReconnect + 1)
  await expect(
    page.getByLabel('聊天记录').getByText('断线期间的第四条', { exact: true }),
  ).toBeVisible()
  const beforeKick = server.connections
  server.push(2002, { reason: 'new_login' })
  await expect(page.getByRole('heading', { name: '欢迎回来' })).toBeVisible()
  await expect(page.getByRole('status')).toContainText('另一个 Web 客户端')
  await page.waitForTimeout(1800)
  expect(server.connections).toBe(beforeKick)
  expect(await page.evaluate(() => sessionStorage.getItem('nexo:web:session'))).toBeNull()
})

test('mobile history pagination preserves scroll and does not mark unseen messages read', async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 })
  const server = await mockServer(page, 205)
  await login(page)
  await openBob(page)
  await expect(page.getByLabel('聊天记录').getByText('历史消息 205', { exact: true })).toBeVisible()
  await expect.poll(() => server.reads.at(-1)).toBe(205)
  const history = page.getByLabel('聊天记录')
  await history.evaluate((node) => {
    node.scrollTop = 0
  })
  await page.getByRole('button', { name: '加载更早消息' }).click()
  await expect(history.getByText('历史消息 6', { exact: true })).toHaveCount(1)
  await expect.poll(() => history.evaluate((node) => node.scrollTop)).toBeGreaterThan(0)
  server.push(2001, server.add('暂未阅读的新消息'))
  await expect(history.getByText('暂未阅读的新消息', { exact: true })).toHaveCount(1)
  await page.waitForTimeout(250)
  expect(server.reads.at(-1)).toBe(205)
  await page.getByRole('button', { name: '回到最新' }).click()
  await expect.poll(() => server.reads.at(-1)).toBe(206)
  await page.screenshot({ path: test.info().outputPath('mobile.png'), fullPage: true })
  await page.getByRole('button', { name: '返回会话列表' }).click()
  await expect(page.getByRole('heading', { name: '消息', exact: true })).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
})
