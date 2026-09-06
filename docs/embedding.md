# 嵌入到宿主进程（`server/`）

设计：`docs/design.md` §15.1。宿主必须是 Hertz；非 Hertz 宿主用 `server.ListenAndServe` 在自己的 goroutine 里起独立端口。

完整、可编译的示例是 `server/example_test.go` 里的 `Example_embedding`（pkg.go.dev 上直接可读，
`go doc github.com/mbeoliero/nexo/server` 也能看到）。它没有 `// Output:` 注释，因此只编译不执行：
不需要数据库，但 API 一旦改动就会构建失败，文档不会和代码脱节。

顺序是 `DefaultConfig`（或 `LoadConfig`）→ `nexo.New(ctx, cfg, WithGormDb, WithRoutePrefix)` →
`s.Mount(h.Engine)` + `s.Start(ctx)` → 宿主 `h.Spin()`。退出时用独立、有超时的 context，
并发执行 `s.Drain` 和宿主 `h.Shutdown`；等待两者结束后再 `s.Close()`，确保在途 HTTP 请求仍能使用依赖。
`s.Shutdown` 只负责 Nexo 的排空和依赖关闭，不会停止宿主 HTTP；需要协调宿主退出时使用 `Drain` / `Close`。

```go
sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
defer cancel()
drained := make(chan error, 1)
go func() { drained <- s.Drain(sctx) }()
httpShutdownErr := h.Shutdown(sctx)
drainErr := <-drained
s.Close()
if err := errors.Join(httpShutdownErr, drainErr); err != nil {
    log.Printf("shutdown: %v", err)
}
```

以上代码使用标准库 `context`、`errors`、`log`、`time`，保留 HTTP shutdown 和 Nexo drain 两方错误。
Hertz 必须用 `standard.NewTransporter`，WS 升级要 Hijack。宿主还可以注入离线推送：
`nexo.WithOfflinePusher(myPusher)`，`myPusher` 实现下面的 `nexo.Pusher`。

离线推送：`Notification` 只带事实（`SenderId`、`GroupId`、`ContentType`、原始 `Content`），文案由 push 侧决定。名字用宿主自己的用户系统查；正文用 `n.Preview()` 拿默认文案，或用 `msgbody.Parse` 自己解析（自定义消息 `Preview()` 返回空串）。

```go
import "github.com/mbeoliero/nexo/msgbody"

func (p myPusher) Push(ctx context.Context, userIds []string, n nexo.Notification) error {
    title := p.users.Name(ctx, n.SenderId)          // 宿主的用户表
    body := n.Preview()
    if n.ContentType == msgbody.Custom {
        b, _ := msgbody.Parse(n.ContentType, n.Content)
        body = renderCustom(b.Custom)                 // 自定义消息的文案由宿主定义
    }
    return p.apns.Send(ctx, userIds, title, body, map[string]string{"conversation_id": n.ConversationId, "seq": strconv.FormatInt(n.Seq, 10)})
}
```

规则：

- 一个进程一个 `New`。`node_id` 和 online_conns 归属按进程算。
- `WithGormDb` 要求 `db.access=gorm`，`WithPgxPool` 要求 `db.access=sqlc`。注入连接只覆盖 Store；`bus=postgres` 和 `cache=pg` 各开自己的连接，仍需 `db.dsn`。
- `WithGormDb` 传入的 `*gorm.DB` 必须以 `gorm.Config{TranslateError: true}` 打开：Store 靠 `gorm.ErrDuplicatedKey` 识别重复键（注册重名、幂等插入）；否则这些会变成 20002 系统错误。注入 MySQL 时，宿主还必须保证池中所有连接禁用 `CLIENT_FOUND_ROWS`（DSN 的 `clientFoundRows`），否则无变化的幂等插入会被当成新消息。Nexo 配置中的 DSN 不能代表注入池的真实参数；无法保证时不要注入，改用 Nexo 自建连接。
- 注入的 GORM DB 必须是非事务连接；不支持从宿主活动事务中调用业务来获得嵌套／共同提交语义。Store.WithTx callback 只使用传入 tx，不得再次 WithTx；这是禁止调用的约束，不承诺不同后端误用时返回相同错误，也未新增运行时嵌套检测。
- 进程内 `User().Logout(ctx, identity)` 须传入非空 `identity.TokenId`，只原子注销匹配的请求 token，不删除后来登录的 token；不能只用用户和平台字段代替已验证身份。
- 带前缀时 internal HMAC 签的是完整路径（`/im/api/v1/internal/...`）；`log.redact_paths` 会自动加前缀。
- `DefaultConfig` 不读环境变量；`LoadConfig` 读文件 + `NEXO_*`。
- 每个 service 方法的入参/返回类型都能用 `nexo.Xxx` 命名（`server/types.go`，由 `types_test.go` 守住）。
- `s.Err()` 在 Bus 订阅失败时收到一个错误；正常停止收到 nil。
