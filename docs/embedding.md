# 嵌入到宿主进程（`server/`）

[设计边界](design.md#151-server--嵌入入口)。宿主必须是 Hertz；非 Hertz 宿主用 `server.ListenAndServe` 在自己的 goroutine 里起独立端口。

完整示例见 [`server/example_test.go`](../server/example_test.go) 的 `Example_embedding`，也可通过 pkg.go.dev 或 `go doc github.com/mbeoliero/nexo/server` 阅读。示例没有 `// Output:`，只编译、不连接数据库执行；它检查导出 API 的可用性，不能替代宿主联调。

## 启动与退出

1. `DefaultConfig` 或 `LoadConfig` 取得配置，调用 `nexo.New`；注入选项与数据库要求见下节。先检查初始化错误，再使用返回值。
2. 宿主 Hertz 使用 `standard.NewTransporter`，WS 升级依赖 Hijack；调用 `s.Mount(h.Engine)`、`s.Start(ctx)` 后由宿主 `h.Spin()` 开始服务。
3. 退出时创建独立、有超时的 context，并发执行 `s.Drain` 与宿主 `h.Shutdown`，等待两者结束再 `s.Close()`。使用 `errors.Join` 保留两方错误，确保在途 HTTP 请求结束前依赖仍可用；具体写法见编译示例。

`s.Shutdown` 只排空 Nexo 并关闭其依赖，不停止宿主 HTTP。宿主协同退出使用 `Drain` / `Close`，排空与清理共用退出期限，详见 [设计 §10](design.md#10-多机部署要点与故障矩阵)。

在线状态订阅随 `Start` 启动刷新，随连接注销释放订阅并纳入同一退出期限；宿主不需要额外定时器或订阅生命周期调用。`New` 按配置装配 OnlineStore，并同时供 user service 查询与 gateway 登记使用，由宿主通过配置选择实现。WS 协议与部署兼容条件见 [在线状态订阅](integration.md#online-status-subscriptions)；特别是本版没有新 Bus 事件的独立启用开关，需要协调同一 Bus 下的服务升级。

依赖与发送限流、群成员缓存 TTL 在 `New` 中一次装配，启动后固定；不提供运行时 `SetOnlineStore`、`SetOfflinePush`、`SetMemberCacheTtl` 或 `SetSendRateLimit`。原先调用这些方法的宿主须改用传给 `New` 的配置及 `WithOfflinePusher` 选项；运行中变更须重建实例。`WithAuthenticator`、`WithGormDb`、`WithPgxPool` 等公共构造选项保持不变。

`s.Stats()` 同时返回网关状态、消息重发布尝试和当前 Bus 的已知失败计数，口径见 [设计 §6.1](design.md#61-bus--节点间事件广播)。计数按实例累计、进程重启归零。Bus 计数是 `BusDegradedPublishes` 与 `BusDegradedPublishesAvailable` 一对：`Available` 为 false 表示所选驱动（`bus.driver: postgres`）根本没有接收者信号，此时计数无意义，必须按“不可观测”展示，不能当作 0 去画图；为 true 且计数为 0 只表示没有检测到已知失败，不代表所有消息已送达。这些计数不能用作真实消息漏推率。宿主可读取现有统计返回值；服务没有额外的 `/metrics` 或 exporter。本版新增 `BusDegradedPublishes` 与 `BusDegradedPublishesAvailable`，不删除既有字段。

## 自定义离线推送

通过 `nexo.WithOfflinePusher(myPusher)` 注入 [`nexo.Pusher`](../server/types.go)。Notification 只携带事实，发送者／群名称从宿主用户系统取得；`n.Preview()` 提供默认文案，也可用 [`msgbody.Parse`](../msgbody/) 解析原始 content。自定义消息默认预览为空，宿主须自行渲染并处理解析失败。触发时机与接收者筛选见 [设计 §6.6](design.md#66-pusher--离线推送接口上层实现-apnsfcm)，接口声明以源码为准。

## 规则

- 一个进程一个 `New`。`node_id` 和 online_conns 归属按进程算。
- `WithGormDb` 要求 `db.access=gorm`，`WithPgxPool` 要求 `db.access=sqlc`，不匹配时 `New` 返回错误；`db.driver` 必须匹配宿主数据库（`postgres` 或 `mysql`）。注入连接只覆盖 Store，Nexo 不关闭宿主的连接；`bus=postgres` 和 `cache=pg` 各开自己的连接，仍需 `db.dsn`，缺少时校验失败。调用 `Mount` 前必须检查 `New` 的错误，初始化失败不会返回可用的 server。
- `WithGormDb` 传入的 `*gorm.DB` 必须以 `gorm.Config{TranslateError: true}` 打开：Store 靠 `gorm.ErrDuplicatedKey` 识别重复键（注册重名、幂等插入）；否则这些会变成 20002 系统错误。注入 MySQL 时，宿主还必须保证池中所有连接禁用 `CLIENT_FOUND_ROWS`（DSN 的 `clientFoundRows`），否则无变化的幂等插入会被当成新消息。Nexo 配置中的 DSN 不能代表注入池的真实参数；无法保证时不要注入，改用 Nexo 自建连接。
- 注入的 GORM DB 必须是非事务连接；不支持从宿主活动事务中调用业务来获得嵌套／共同提交语义。Store.WithTx callback 只使用传入 tx，不得再次 WithTx；这是禁止调用的约束，不承诺不同后端误用时返回相同错误，也未新增运行时嵌套检测。
- 进程内调用不经过 HTTP Bearer / HTTP 限流，消息限流是否豁免由宿主通过 `SendInput.Unlimited` 决定；事件仍经 Bus 推到所有节点。发送结果未知时，宿主按 [接入协议](integration.md#websocket) 用原 `client_msg_id` 重试：幂等命中返回原 ACK，并按发送额度尝试重发布，不重复离线推送。`User().Logout(ctx, identity)` 须传入非空 `identity.TokenId`，只原子注销匹配的请求 token，不删除后来登录的 token；不能只用用户和平台字段代替已验证身份。
- 带前缀时 internal HMAC 签的是完整路径（`/im/api/v1/internal/...`）；`log.redact_paths` 会自动加前缀。
- `DefaultConfig` 不读环境变量；`LoadConfig` 读文件 + `NEXO_*`。`WithAuthenticator` 替换整条鉴权链；嵌入时全局 `kit/log` 配置由宿主决定。
- 每个 service 方法的入参/返回类型都能用 `nexo.Xxx` 命名（`server/types.go`，由 `types_test.go` 守住）。
- `s.Err()` 在 Bus 订阅失败时收到一个错误；正常停止收到 nil。
