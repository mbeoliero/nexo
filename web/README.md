# Nexo Web

独立的 Web IM Demo：React + TypeScript + Vite + Radix Primitives + Tailwind CSS。后端启动见[根 README](../README.md#quick-start)，HTTP/WS 契约见[接入协议](../docs/integration.md)。

## 本地启动

需要 Node.js 22.12+（推荐 Node.js 24）、npm，以及已经启动的 Nexo 后端。

```sh
cd web
npm ci
npm run dev
```

打开 [本地开发页面](http://127.0.0.1:5173)。Vite 将 `/api` 和 `/ws` 同源代理到 `http://127.0.0.1:8080`，无需修改后端 CORS。

登录页使用 native 用户名密码认证，需要后端的 `auth.providers` 包含 `native`；Web 平台固定为 `5`。后端配置了 `ws.allowed_origins` 时，须允许实际浏览器 origin（本地默认 `http://127.0.0.1:5173`）。

代理到其他开发后端或 compose 的负载均衡端口：

```sh
NEXO_WEB_PROXY_TARGET=http://127.0.0.1:18080 npm run dev
```

`NEXO_WEB_PROXY_TARGET` 只供 Vite 开发服务器使用，不打进浏览器产物。不要将平台 HMAC secret、native 签名密钥或数据库凭据放进前端或 `VITE_*` 环境变量。

## 两个账号验收

1. 在第一个标签页注册 Alice，复制自己的完整用户 ID。
2. 在独立浏览器上下文（如无痕窗口）注册 Bob；点击发起聊天，输入 Alice 的用户 ID。
3. 发送文本，确认双方收到；刷新后进入会话，确认最近历史仍可加载。
4. 让 Alice 断网，Bob 连续发送两条消息，再恢复 Alice 网络；确认消息按序补齐且不重复。
5. 用另一个浏览器上下文重新登录 Alice；旧连接应回到登录页，不自动重连抢占新连接。

新单聊先是本地草稿，第一条消息发送成功才由后端创建会话。不提供用户目录或按用户名搜索；需要完整 `user_id`。同一用户的不同 Web 登录会互踢，这是后端已有规则。

## 范围与行为

- 原生注册、登录、退出，完整用户 ID 展示/复制。
- 会话列表、本地会话搜索、未读数、文本单聊，以及已有群会话的文本收发。
- 最近 100 条消息与按需加载更早历史；内容以纯文本渲染。非文本消息显示类型占位，不执行内容中的 HTML。
- WS 接收通知，HTTP 发送/拉取/已读；断线指数退避重连。2001 推送携带完整正文：`seq == 本地游标+1` 直接落地并推进游标，不发任何请求；`seq` 更大时先拉缺口，`seq` 不大于游标时丢弃，帧无法识别或会话未跟踪则退回整体同步。连接建立、前台恢复与 Resync 同样触发整体同步。浏览器自动响应协议 Ping，不发送自定义 JSON 心跳。
- 同步请求合并并串行执行，按 `has_more` 翻页；只在连续消息成功合并后推进游标，不能从单个 ACK 或 last_message 跳过缺口。网络失败后台重试；被踢或 HTTP 401 则停止重连并返回登录。

页面在前台且浏览器在线时，每轮同步成功后间隔 30–36 秒再次对账，即使 WS 一直健康也会补查静默丢失的最后一条消息。失败按 3、6、12、24、30 秒退避，并附加最多 20% 随机抖动；退避期间的 push 合并等待重试，成功后恢复正常间隔。页面隐藏或浏览器离线时暂停自动同步计时，已发出的请求可以完成；恢复前台或在线时立即对账。手动刷新也可立即重试。停止客户端或更换会话会取消旧请求和计时器。

所有携带当前凭据的 HTTP 请求（包括个人资料、历史、已读和退出）在收到 401 时统一结束会话；503 等依赖故障保留凭据。旧会话已取消的响应不能使新会话退出。

消息发送失败后，点击失败消息重试，复用原 `client_msg_id` 和原内容；“已发送”只表示服务端已提交，不是送达或对方已读。停止客户端会清除加载状态，并把内存中尚未确认的发送标为可重试，重启同一客户端后仍复用原消息 ID 和正文；取消请求不证明服务端提交失败。已读仅在会话可见、滚动到底部并完成加载时上报；后端的已读同步只面向本人其他端。

### 存储与限制

凭据存于当前标签页的 `sessionStorage`，退出/失效时清除；不保存密码。连续同步游标存于按用户隔离的 `localStorage`。两者都不可用时退化为内存状态，重新进入后重新拉取。

每轮同步结束时批量保存已完成页的连续游标；后续页失败、客户端停止或页面离开时仍保存已完成的进度，不把未获取的正文算作已同步。直接落地的 2001 推送同样推进游标，随下一次落盘一起保存。

消息缓存与失败待发送消息只在内存中，**刷新/关闭页面会丢失未成功发送的内容**。刷新后从后端恢复历史；同步游标不代表正文已经持久化。默认每个会话保留最近 100 条，主动加载更早历史后内存列表随加载增长。

当前不做建群/成员管理、媒体上传、通知推送、外部平台登录、对方已读回执、独立 npm SDK 或移动端适配。现有群会话使用群 ID 展示；群可见范围变更在下次同步时清理旧缓存。

## 检查

```sh
npm test                          # envelope、连续 seq、连接生命周期与幂等重试
npm run build                     # TypeScript 检查 + 生产构建
npx playwright install chromium  # 首次运行浏览器测试
npm run test:e2e                   # HTTP/WS mock 驱动的真实浏览器交互
```

浏览器测试使用模拟协议服务，不连接真实数据库；真实后端联调使用上面的双账号验收步骤。CI 独立运行前端检查，不改变后端测试流程。

## 部署

`npm run build` 生成 `web/dist/` 静态文件。部署在站点根路径；同一 origin 的 `/api/v1/*` 与 `/ws` 必须反向代理到 Nexo。`npm run preview` 只预览静态产物，不提供开发代理。

例如在已有 TLS nginx 站点中配置（替换静态目录和后端地址）：

```nginx
root /srv/nexo-web/dist;

location / {
    try_files $uri $uri/ /index.html;
}
location /api/ {
    proxy_pass http://127.0.0.1:8080;
}
location = /ws {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 90s;
    # 浏览器 WS 握手必须通过 query 传 token，不记录该 URL。
    access_log off;
}
```

使用 HTTPS（localhost 开发除外）；客户端自动使用 WSS。任何上游代理也应避免记录 WS query token。生产配置应限制 WS origin。

## 文件

[`src/client/`](src/client/) 是不依赖 React 的协议与同步实现；[`src/components/`](src/components/) 负责界面，Radix 提供交互基础，Tailwind 提供样式。
