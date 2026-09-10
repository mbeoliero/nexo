# Nexo IM 系统设计（单体二进制 · 多机部署）

> 本文定义当前架构与行为约束。对外协议见 [integration.md](integration.md)，宿主操作见 [embedding.md](embedding.md)。在线状态订阅见 §7.4，幂等重发布与 Bus 失败可见化见 §8.3／§6.1；延迟重试、定向 Resync 与服务端主导消息同步仍是 §16 的未实现草案，不能据此移除现有客户端消息对账兜底。

## 0. 一页总览

| 项 | 结论 |
| --- | --- |
| 形态 | 一个二进制 `nexo`，N 个同构节点，前置 LB；共享 MySQL 或 PostgreSQL；Redis 可选。与外部社交平台 app 同机/同集群部署 |
| 功能 | 用户资料、群（建/加/退/踢/成员）、单聊群聊、会话列表（游标分页 + 未读 + last_message）、已读、WS 实时推送、按 seq 同步、在线状态查询与订阅、离线推送接口、平台后端 internal 通道 |
| 不做（一期） | 好友/黑名单、撤回、已读回执、文件存储、消息压缩、open-im SDK 协议兼容、APNs/FCM 具体对接（只给接口） |
| 同步增量 | 在线状态订阅见 §7.4，幂等重发布见 §8.3；后台补偿与服务端主导消息同步仍为 §16 的未实现草案 |
| 鉴权 | `Authenticator` 链，provider 顺序由 `auth.providers` **启动时配置**：`external_jwt`（平台 HS256，claims `user_id int64 + role`）、`native`（自建 JWT + `TokenStore` 每平台单 token）。用户 id 固定格式 `u___{id}` / `ag__{id}` / 自建 `nx__{uuid}` |
| 服务间调用 | `/api/v1/internal/**`：HMAC-SHA256 签名，覆盖 nonce、方法、原始路径／查询串、用户／平台头和正文摘要；`X-User-Id` 代用户操作。完整协议见 §6.4，旧签名不兼容 |
| 数据访问 | 仓储接口 `Store` 双实现：MySQL 只能用 GORM 泛型；PostgreSQL 可选 GORM 泛型或 sqlc + pgx/v5 |
| seq 分配 | DB 行锁（`conversations.max_seq` FOR UPDATE），落库同事务，多机天然一致 |
| 跨节点推送 | `Bus` 接口：Redis Pub/Sub 版 + PostgreSQL LISTEN/NOTIFY 版 + 单机 local 版；重连后向本地连接推 `Resync` |
| 多端登录 | 同平台互踢：WS 连接级 kick 广播（两种 token 都有）+ 自建 token 走 `TokenStore` 覆盖旧 token（HTTP 也立即失效）；外部 token 的吊销归平台 |
| 离线推送 | `Pusher` 接口（内置 Noop / Webhook），发送节点对离线目标调用一次；离线判定靠 `OnlineStore` |
| 投递语义 | 实时推送 at-most-once；消息已落库；客户端按 `(conversation_id, seq)` 去重 + 拉取补齐 |
| 共享状态 | `Bus`、`Cache`（承载 `TokenStore`）、`OnlineStore` 各有 Redis 与 PG 实现；无 Redis 时 Cache 使用 PG 普通表 |

## 1. 部署形态

```
            ┌────────────┐
  clients ──┤  LB/nginx  │  (HTTP + WS upgrade, 无需会话粘性)
            └─────┬──────┘
        ┌─────────┼─────────┐
        ▼         ▼         ▼
    ┌───────┐ ┌───────┐ ┌───────┐
    │ nexo1 │ │ nexo2 │ │ nexo3 │   每个节点 = HTTP API + WS 网关 + 业务逻辑（完全同构）
    └───┬───┘ └───┬───┘ └───┬───┘
        │         │         │            ▲ 平台后端 ──HMAC──► /api/v1/internal/**
        ├─────────┼─────────┤──────► MySQL / PostgreSQL   (唯一事实来源：消息、seq、在线连接)
        │         │         │
        ├─────────┼─────────┤──────► Bus: Redis Pub/Sub 或 PG LISTEN/NOTIFY  (节点间事件广播)
        │         │         │
        └─────────┴─────────┴──────► Pusher (Webhook → 上层自己的 APNs/FCM 服务)   [可选]
```

支持的部署组合（一期）：

| DB | 数据访问 | Redis | Bus | Cache | OnlineStore | 一期 |
| --- | --- | --- | --- | --- | --- | --- |
| PostgreSQL | `gorm` 或 `sqlc` | 无 | `postgres` | `pg` | `db` | ✅ 最小依赖：只要一个 PG |
| PostgreSQL | `gorm` 或 `sqlc` | 有 | `redis` | `redis` 或 `pg` | `redis` 或 `db` | ✅ |
| MySQL | `gorm` | 有 | `redis` | `redis` | `redis` 或 `db` | ✅ |
| MySQL | `gorm` | 无 | — | — | — | ❌ MySQL 没有 LISTEN/NOTIFY，二期可加 DB 轮询总线 + MySQL cache 表 |
| 单机开发 | 任意 | 任意 | `local` | `local` | `db` | ✅ 进程内 channel / map |

节点身份：`node_id` 来自配置/环境变量，默认 `hostname`。用于日志、事件来源标记、`online_conns` 归属和启动清理。不参与路由。过长的 `node_id` 会挤占 `presence_changed` 的事件封装预算并使该事件整批发布失败，见 §6.1 的 payload 大小规则。

## 2. 技术栈与关键决策

| 决策点 | 选择 | 原因 / 备注 |
| --- | --- | --- |
| HTTP | Hertz | 沿用偏好；日志用 `mbeoliero/kit/log` 的 `WithHertz()` |
| WebSocket | `hertz-contrib/websocket`（gorilla 分支），使用 standard transporter | WS 升级依赖 Hijack；与 HTTP 同端口，LB 只配一个 upstream |
| 仓储接口 | `store.Store`：全部仓储方法 + `WithTx(ctx, func(Store) error)`；不支持嵌套事务。service 不直接依赖它，只依赖自己声明的窄接口（§3） | service 层定义事务边界；callback 只能使用传入的 tx，不得再次调用 WithTx（包括捕获外层 Store 后调用），不提供嵌套保存点或跨后端一致的嵌套结果 |
| 实现 1：`gormstore` | GORM v1.31 泛型 API（`gorm.G[T]`），MySQL + PG | 一套代码跑双库。方言差异 3 处用 clause 抹平：upsert（`clause.OnConflict`）、行锁（`clause.Locking`）、唯一键冲突识别（`TranslateError: true` → `gorm.ErrDuplicatedKey`） |
| 实现 2：`pgstore` | sqlc 生成代码 + `pgx/v5` pool，仅 PG | SQL 手写、零反射；唯一键冲突用 `pgconn.PgError.Code == "23505"`；事务 `pool.Begin` + `Queries.WithTx` |
| 选择规则 | `db.driver=mysql` → 强制 `gorm`；`db.driver=postgres` → `db.access=gorm\|sqlc` | 启动时校验非法组合 |
| Schema 事实来源 | SQL 迁移文件：`migrations/postgres/*.sql`（同时是 sqlc 的 schema 输入）、`migrations/mysql/*.sql` | 放弃 AutoMigrate。GORM 模型必须与 SQL 一致，靠集成测试矩阵保证 |
| 迁移执行 | `goose`（`pressly/goose/v3`，embed.FS，同时给 sqlc 当 schema 输入），`nexo migrate` 子命令 | 部署前单独执行一次，节点启动不跑 DDL。一期空库起，无存量迁移 |
| PG 直连 | `pgx/v5` | `pgstore` 的驱动；两种 access 下 LISTEN 都用独立 `pgx.Conn`；NOTIFY 走 `pg_notify()` |
| Redis | go-redis v9（可选）| Bus / Cache / OnlineStore 的 Redis 实现；`kit/connector` 初始化 |
| Cache | `internal/cache`：`redis` / `pg`（普通表 + cleaner）/ `local` | KV 状态统一接口，当前用于 TokenStore 与 internal HMAC nonce 去重 |
| 鉴权 | `auth.Authenticator` 接口 + 链，provider 顺序由 `auth.providers` 启动配置；`external_jwt` / `native` 均 HS256（golang-jwt v5）| 外部 token claims `{user_id int64, role, exp}`，platform 由请求参数给出；自建 claims `{sub, pid, jti, exp}` + `TokenStore` 每平台单 token |
| 用户 id | `internal/identity`：平台用户／agent 与 native 三类，前缀固定 4 字节 | ID 含 `_`，conversation_id **必须用 `:` 分隔**；格式见 [User ids](integration.md#user-ids) |
| 服务间鉴权 | HMAC-SHA256、时钟窗、服务白名单、Cache nonce 去重（§6.4） | 旧签名未覆盖用户／平台／query，调用方必须按现行协议升级 |
| 其他 ID | group_id：服务端短 ID；server_msg_id / conn_id / token_id：UUIDv7 | 无 snowflake → 无需节点号协调 |
| 序列化 | JSON（WS 与 HTTP 一致）| 握手预留 `encoding`/`compression` 参数，后续加 gzip 不改协议 |
| 错误 | `errcode` 区分业务与系统错误，`%w` 保留原因链；[错误映射](integration.md#http-envelope-and-error-codes) 由接入协议维护 | 未知错误统一映射 `20001`，不能伪装成 `code=0` 成功 |
| 配置 | yaml + 环境变量覆盖（viper）| 每个配置项必须有引用；启动时打印生效配置（secret 脱敏）|

双实现的代价：每个仓储方法写两遍；改表要同步 pg SQL、mysql SQL、GORM model 三处。
建议实现顺序：先写 sqlc 的 `.sql` 当规格，再照着写 GORM 实现，最后跑同一套集成测试。

## 3. 目录结构

按职责定位代码；完整文件列表以仓库为准，生成输入与输出见 [Code generation](../CONTRIBUTING.md#code-generation)。

| 边界 | 入口与职责 |
| --- | --- |
| 进程与公共入口 | [`cmd/nexo/`](../cmd/nexo/) 拥有独立进程；[`server/`](../server/) 提供嵌入 facade；[`sdk/`](../sdk/) 为独立 HTTP 客户端（§15） |
| 组装 | [`internal/app/`](../internal/app/) 连接依赖并管理生命周期，不处理信号、监听、handler 或业务规则 |
| 接入 | [`internal/api/`](../internal/api/) 注册路由、鉴权和限流；其中 `webx` 提供无业务依赖的 HTTP helper；[`internal/gateway/`](../internal/gateway/) 管理 WS 连接、帧和本地扇出 |
| 业务 | [`internal/service/`](../internal/service/) 拥有校验及事务；`conv` 放会话 ID、游标与可见范围，`dto` 放服务间共用响应类型 |
| 数据 | [`internal/store/`](../internal/store/) 定义实体和仓储，`gormstore` / `pgstore` 实现；`store/migrate` 应用 [`migrations/`](../migrations/) |
| 鉴权与共享状态 | [`auth`](../internal/auth/)、[`tokenstore`](../internal/tokenstore/)、[`cache`](../internal/cache/)、[`bus`](../internal/bus/)、[`onlinestore`](../internal/onlinestore/) 的契约见 §6 |
| 对外扩展 | [`internal/offlinepush/`](../internal/offlinepush/) 拥有离线推送；公开的 [`msgbody/`](../msgbody/) 只依赖标准库；[`errcode/`](../errcode/) 提供稳定错误类型与码值 |
| 配置与部署 | [`internal/config/`](../internal/config/) 校验组合；[`deploy/`](../deploy/) 和 [`scripts/`](../scripts/) 提供部署与验收入口 |

`api` 和 `gateway` 的业务调用进入 `service`，服务之间不相互导入。网关另订阅 Bus、登记 OnlineStore，并通过 Authenticator 验证连接；这些接入职责不放进 service。鉴权依赖方向为 `auth` → `tokenstore` → `cache`，native 签发／吊销由 `service/user` 调用。跨层叶子如 `identity`、`ratelimit` 留在 `internal/` 顶层，service-only helper 留在 service 内，避免无职责的 `common` / `utils` 包。

`store.Store` 是后端的完整实现契约；consumer 只声明实际调用的方法，或复用恰好匹配的 Store 子接口。`group` 和 `message` 的事务回调拿到本包 `Tx`，不暴露 `WithTx`；包内唯一的 `Adapt(store.Store)` 在组装时桥接，赋值本身提供编译期兼容检查。业务不得捕获外层 Store 绕回事务外，也不支持嵌套事务或保存点。

`storetest` / `bustest` / `cachetest` / `onlinetest` 各用一套用例验证同一接口的全部实现；其中内存 fake 不由生产代码导入。

## 4. 数据模型

完整列、类型和索引以 [PostgreSQL 迁移](../migrations/postgres/) / [MySQL 迁移](../migrations/mysql/) 为准；本节只记录跨表关系与不能从 DDL 推出的约束。GORM 模型须与两套迁移一致。

- 时间列使用 PG `timestamptz DEFAULT now()` / MySQL `datetime(3) DEFAULT CURRENT_TIMESTAMP(3)`；service 每事务取一个毫秒精度的 `time.Time` 显式写入，禁用 GORM 自动时间戳。API 与游标用 Unix 毫秒；发送时的时钟回拨保护见 §5.11。
- JSON 存为 `text` 字符串，不依赖 `jsonb` 函数，保持双库行为一致。表名 `chat_groups` 避开 MySQL 保留字 `groups`。
- 所有 `conversation_id` 列用 `varchar(256)`：最长单聊 ID 为 `si_` + 64 + `:` + 64 = 133 字节，128 无法容纳。联合主键不另加自增代理键。
- 用户与群的 `extra` 上限为 65535 UTF-8 字节，允许空串；service 在写库前统一校验，不截断。部分更新的 `nil` 表示不修改，空串表示清空；[接入协议](integration.md#profile-fields) 定义 wire 表示。

### users

`id` 是身份主键，格式见 [User ids](integration.md#user-ids)。平台用户由 internal upsert 写入；native 账号保存 username 和 bcrypt password_hash，平台账号这两项为空。username 只有普通索引：注册时存在性检查仅防误操作，并发重复注册是可接受的，不承诺唯一。

### online_conns（OnlineStore 的 DB 实现；Redis 实现时不用此表）

以 `conn_id` 为主键，关联 user、platform 和 node；`node_id` 用于启动清理和续期，读取时排除过期心跳。登记、续期、删除的并发约束及 Redis 对应实现见 §6.5。

### cache（Cache 的 PG 实现；Redis / local 实现时不用此表）

仅有 PG 迁移；以带 `nexo:` 前缀的 key 为主键，`expires_at=NULL` 表示不过期。过期判断使用 DB `now()`，普通表保留崩溃恢复后的 token；原子操作与 cleaner 见 §6.3。

### chat_groups

`id` 标识群，`owner_id` 指向群主；status 区分正常与已解散。解散状态与成员资格须在发送事务持有会话行锁后检查（§8.4），不能只依赖事务前预检。

### group_members

联合主键 `(group_id, user_id)` 表示当前成员资格；role 为 1 成员、2 管理员、3 群主，管理员仅由宿主预置，当前没有授予／撤销入口。自行加入时 inviter 为空；`user_id` 索引用于查询加入的群。退群／被踢删除成员行，历史可见边界留在 `user_conversations`（§8.5）。

### conversations（会话 seq 行 = 锁对象）

`conversation_id` 是主键；会话类型和群关联不随发消息改变。`max_seq` 是已分配最大 seq，其行锁统一串行化发送、入群、退群／踢人和解散。

### user_conversations（每个用户视角的会话）

联合主键 `(owner_id, conversation_id)` 同时表示读取权限与个人会话状态：

- `min_seq` 是可见下界：单聊为 1，入群为当时 `conversations.max_seq+1`；建群初始成员例外，见 §8.5。
- `max_seq=0` 表示无上界，退群时冻结到当时会话最大 seq；**若群尚无消息，必须删除该行**，否则 0 会误授予未来消息的可见性。重入群重算下界并解除上界。
- `read_seq` 只增不减；`recv_msg_opt=1` 免打扰只排除离线推送；`is_pinned` 只存储、不参与服务端排序。
- `(owner_id, updated_at DESC, conversation_id)` 索引支持游标分页，`conversation_id` 索引支持群消息批量更新。可见范围、未读与排序时间的计算见 §5，列表查询见 §8.8。

### messages

联合主键 `(conversation_id, seq)` 支持顺序拉取和 last_message 点查；`server_msg_id` 为唯一 UUIDv7。幂等唯一键限定为 `(conversation_id, sender_id, client_msg_id)`，跨会话共享会把发给别人的消息当作幂等结果返回。ID 校验、冲突回滚与 seq 不留洞的规则见 §5.6。

消息携带发送身份、会话及收件人／群关联，content 按 [消息协议](integration.md#websocket) 存储合法 JSON 字符串；类型化解析不等于发送参数验证（§6.6）。

## 5. 核心约定

1. **conversation_id**：单聊 `si_` + 两个 user_id 字典序小者 + `:` + 大者；群聊 `sg_` + group_id。分隔符**只能是 `:`**（user_id 含 `_`）。生成后不可变。
2. **seq**：每个会话独立、从 1 起、连续、单调。由 `conversations.max_seq` 在事务内 `FOR UPDATE` 后 +1 分配。
3. **可见区间**：用户 U 在会话 C 能看到的 seq 范围 =
   `[ max(begin, uc.min_seq), min(end, conv.max_seq, uc.max_seq>0 ? uc.max_seq : ∞) ]`。
   拉取、推送、last_message 都强制这个边界。入群不可看历史、退群不可看新消息都靠它。拉取和 MarkRead 通过 Store 的单条 JOIN 同时读取 `user_conversations` 和 `conversations.max_seq`，按同一语句快照计算可见边界。拉取再读取该范围的消息；不能混用退群前的权限与退群后的会话上界。普通 READ COMMITTED 事务中的两次 SELECT 不提供这个保证。
4. **未读数** = `visible_max_seq - read_seq`，其中 `visible_max_seq = uc.max_seq>0 ? min(uc.max_seq, conv.max_seq) : conv.max_seq`。
5. **read_seq 单调**：`UPDATE ... SET read_seq = GREATEST(read_seq, ?)`（MySQL/PG 都有 GREATEST）。MarkRead 的写入目标、返回值和广播使用同一快照算出的目标；目标不大于已有 read_seq 时保持原有无更新／不广播语义，不回退历史已读值。
6. **幂等**：唯一索引 `(conversation_id, sender_id, client_msg_id)`。`client_msg_id` 长度为 1–64 字节，末尾不得是 Unicode 空白（按 Go `unicode.IsSpace`，包括空格、制表符、换行及全角空格）；service 在幂等查询前校验，违规返回 `10001`，HTTP / internal / WS / 嵌入调用一致。只拒绝，不裁剪或归一化；前导和内部空白不受此规则限制。保留 MySQL 当前排序规则，避免 PAD SPACE 将合法的新 ID 与尾空白变体视为同一消息。本规则不迁移历史数据；如已有尾空白 ID，须在上线前另行处理，否则合法短 ID 仍可能命中旧记录。先查快路径；事务内在持有行锁后 `INSERT ... ON CONFLICT DO NOTHING`（gormstore 用 `clause.OnConflict{DoNothing: true}`，MySQL 实际生成自赋值的 `ON DUPLICATE KEY UPDATE`；所有 MySQL 连接必须禁用 `CLIENT_FOUND_ROWS`），**影响行数 0 → 回滚**（`max_seq` 尚未更新，seq 不浪费）→ 回查已存在消息 → 返回同一 ACK。并发双发必须得到相同 ACK，不能返错。
7. **推送 at-most-once**：每次 Publish 不自动重试；同一消息可因客户端再次 Send 而重新发布一次，见 §8.3。没有后台重试队列或投递成功保证，客户端按会话和 seq 去重。客户端三条规则兜底：
   - 收到 push 的 `seq > 本地 max_seq + 1` → 拉取缺口；
   - 连接建立/前台恢复/收到 `Resync` 帧 → `GetMaxSeqs` 对比本地，拉取差异；
   - 收到 app push → 唤起后同上。
8. **权限**：所有按 conversation_id 的读操作，先确认 `user_conversations(owner=当前用户, conversation_id)` 存在，否则 403。
9. **被踢不重连**：客户端收到 `2002 KickOnline` 后不得自动重连，须重新走登录（平台 token 场景由平台决定）。否则新旧两端会互相踢成乒乓。
10. **收件人必须存在**：`users` 无该 id → `10201`。平台用户先由平台后端 `/internal/user/upsert` 写入。
11. **消息排序时间不回退**：发送持有会话行锁后确定本次持久化时间，不小于该会话已有 `updated_at`；并发请求交错、节点时钟偏差或回拨不能让新消息把会话排序时间写回过去。消息的 `send_time`／`created_at` 与会话 `updated_at` 共用该毫秒值；触达用户会话时以该时间更新，但保留已有更晚的排序时间（例如其他节点入群时写入的时间），不能降低个人视角的排序键。时间只保证不减，允许相等，严格顺序仍以 seq 为准；幂等重试返回原 ACK，不刷新时间。限流按请求到达时的服务时钟判定，不用被钳制的会话时间补充令牌。

## 6. 共享状态与横切抽象

### 6.1 Bus —— 节点间事件广播

接口与事件类型见 [`internal/bus/bus.go`](../internal/bus/bus.go)：`Event{Type, NodeId, Payload}`，Payload 为 `jsontext.Value`；`Bus.Publish` 发布事件，`Subscribe` 阻塞订阅并在连接成功时回调 `onConnected`。

| 实现 | 机制 | 说明 |
| --- | --- | --- |
| `redis` | 单 channel `nexo:events` PUBLISH / SUBSCRIBE | 断线后 go-redis 自动重连；期间事件丢失 |
| `postgres` | `SELECT pg_notify('nexo_events', $1)`；独立 `pgx.Conn` 执行 `LISTEN nexo_events` + `WaitForNotification` 循环 | payload 上限 8000 字节；连接断开重连（退避 1s→30s），期间事件丢失 |
| `local` | 进程内 channel | 单机 / 测试 |

**已知失败必须可见。** `local.Publish` 对每个已满订阅队列各记一次丢弃，继续尝试其他订阅，遍历结束后只要发生丢弃就返回 `errcode.ErrBusFailed`；不能用报错推断其他订阅也没收到。Redis `PUBLISH` 返回零接收者时计一次并返回同一错误；返回正数只证明 Redis 报告存在接收者，不证明本节点以外的节点或客户端都收到。PG 没有对应的接收者信号，保持原行为；这一点必须能被上报方区分，见下方计数表的 `Bus.DegradedPublishes`。这里只报告已知失败，不增加阻塞投递或自动重试；消息 service 已提交后的 ACK 不因发布失败改变。

观测使用各实例自己的原子计数，不使用包级全局值：

| 来源 | 统计含义 |
| --- | --- |
| [`message.Service.RepublishCount`](../internal/service/message/message.go) | 幂等命中后实际调用 Publisher 的次数；跳过重发布不计，发布失败也计，既不证明原推送丢失，也不证明本次补投成功 |
| [`local.Bus.DroppedCount`](../internal/bus/local/local.go) | 因订阅队列满而跳过的投递次数；一次 Publish 可计多次 |
| [`redis.Bus.NoReceiversCount`](../internal/bus/redis/redis.go) | Redis `PUBLISH` 返回零接收者的次数；其他命令错误不计入这个值 |
| [`bus.Bus.DegradedPublishes`](../internal/bus/bus.go) | 接口层统一读数：local 返回 `DroppedCount`、redis 返回 `NoReceiversCount`，均带 `ok=true`；PG 没有接收者信号，返回 `(0, false)` 表示“无此信号”，不是“没有丢弃”。每个实现都必须回答，新增或包装 Bus 时编译器会强制补齐 |

两个 Bus 计数覆盖全部事件，包括 `presence_changed`，不限于消息推送；`DegradedPublishes` 的口径按驱动而定（local 一次 Publish 可计多次，redis 每次零接收者计一次），因此只能横向比较同一驱动。这些都是运行状态与失败信号，不能当作真实消息漏推率。读数为 `ok=false` 时该计数无意义，必须按“不可观测”上报，不能当作 0 去画图。

`app` 在现有网关统计之上汇总 service 与所选 Bus 的计数，通过现有 `server.Stats()` 暴露；Bus 计数经 `bus.Bus` 接口读取，不按驱动具体类型分支，因此宿主注入或被包装的 Bus 也会如实上报自己的口径。返回字段声明见 [`gateway.Stats`](../internal/gateway/gateway.go)。service 和 Bus 不依赖 gateway，不引入 exporter、`/metrics`、依赖、配置键或持久化统计。

PG LISTEN/NOTIFY 的已知性质（已评估，方案按 at-most-once 设计）：
- 不持久化、不重放，只投递给当时在 LISTEN 的会话；监听连接断开到重新 LISTEN 之间的通知全丢；
- 事务内 NOTIFY 在提交时发出，回滚不发；同一事务内相同 channel+payload 合并为一条（我们 payload 含 conv_id+seq，不会撞）；
- 通知队列满（默认 8GB，仅监听会话长期卡在事务里才会满）时发送方提交报错，不是静默丢；监听连接独占且不开事务，不会触发；
- **部署限制：必须直连 PG 主库或 PgBouncer session 模式；transaction 模式不支持 LISTEN。**
- 已评估并放弃 outbox 表 + NOTIFY 门铃的 at-least-once 方案（每事件多一次 insert + 清理），一期不做。

**Bus 重连后的 Resync（对丢失窗口的兜底）**：任何实现的 `onConnected` 触发时（首次连接除外），节点向本地所有 WS 连接推一帧 `2004 Resync`，客户端立即 `GetMaxSeqs` 补齐。丢失窗口 = 断线时长，恢复不用等下一次触发。连接未断时的静默漏推仍靠客户端周期对账；同步增量另见 §16。

**payload 大小规则（所有实现统一）**：`push` 序列化后 > 7500 字节 → 改发引用形式 `{type:"push", ref:true, conversation_id, seq}`，接收节点按 `(conversation_id, seq)` 回读 DB，避免消息正文与事件封装一起超过 PG 通知大小限制。`presence_changed` 的用户列表按固定 100 个用户 id 一片发布，仍复用 [`bus.MaxPayloadBytes`](../internal/bus/bus.go)，不引入独立上限：片大小取常量而不是按封装剩余空间反推，`node_id` 变长时不会悄悄退化到每片一个 id。封装本身放不下一个满片时整次发布放弃并计一次失败；单片编码仍按实际字节校验（自定义 Authenticator 可能发放超长 id），超限的片只失败该片。失败口径见 §7.4。

**消费端并发（网关侧）**：`Subscribe` 的回调跑在单个 goroutine 上，而 push 事件需要回读发送范围与可见性（引用形式还要回读消息本身），一次慢查询会把同一节点上的 kick / conv_read / group_changed 一起堵住。因此 push 事件不在回调里处理：按 `conversation_id` 哈希投递到 `ws.deliver_workers` 个分片队列（每片 `ws.deliver_queue`），由各自的 worker 解析并扇出。同一会话始终落在同一分片，会话内顺序不变；分片队列满时丢弃该事件并计入 `push_dropped`，符合 6.1 的 at-most-once 约定（客户端下次 pull 或 Resync 补齐）。kick / conv_read / group_changed 仍在回调里同步处理，它们只操作内存状态；`presence_changed` 只标记受影响的本地订阅待刷新，由 §7.4 的有界执行位读取，回调不查库。即使出站队列满，socket 关闭与本地注销也不等待全局在线登记清理。Redis 空闲订阅取消时主动关闭对应 PubSub，打断阻塞读取，不依赖下一条事件或整个 Redis Client 关闭。

**事件定义**：

| type | payload | 接收节点动作 |
| --- | --- | --- |
| `push` | `{conversation_id, seq, session_type, sender_id, sender_conn_id, recv_id?, group_id?, msg?}` | 解析目标用户 → 查本地 UserMap → 投递到连接 send 队列。排除 `sender_conn_id`（发送方当前连接通过请求响应获取 ACK，不重复推送；HTTP/internal 发送时为空 → sender 所有连接都收）|
| `kick` | `{user_id, platform_id, keep_token_id}` | 关闭本地该 user+platform 且 `token_id != keep_token_id` 的连接，先发 `2002 KickOnline` 帧 |
| `group_changed` | `{group_id}` | 失效本地群成员缓存 |
| `conv_read` | `{user_id, conversation_id, read_seq}` | 推给该用户其他在线端（`2003 ConvRead`）|
| `presence_changed` | 用户 ID 列表；声明见 [`bus.PresenceChanged`](../internal/bus/bus.go) | 合并受影响连接的待刷新标记；不携带权威在线状态，见 §7.4 |

**目标用户解析在接收端做**（而不是发送节点把 user_id 列表塞进事件）：
- 单聊：`[sender_id, recv_id]`，直接来自事件；
- 群聊：`GroupMemberCache.Members(group_id)`（每节点本地 TTL map，TTL 由 `limits.group_member_cache_ttl` 控制，`group_changed` 主动失效；容量满时先清过期项，仍满则整体清空，容量常量见 [`members.go`](../internal/service/message/members.go)）**只筛候选人，不作为授权依据**。候选人与本地 UserMap 取交集后，投递前批量查 DB：`SELECT owner_id FROM user_conversations WHERE conversation_id=? AND owner_id IN (...) AND min_seq <= seq AND (max_seq = 0 OR seq <= max_seq)`，只向匹配者投递。即使失效事件丢失，退群用户也收不到范围外正文；代价是每条群消息在有本地候选的节点增加一次主键范围查询。单聊同样检查 sender/recv 两行，路径统一。

**发送节点不做本地直推**：落库后只 `Publish`，自己作为订阅者按相同路径投递本地连接。这样本地与跨节点共用可见性校验，代价是本机推送也经过 Bus 往返；不承诺固定延迟。

### 6.2 Authenticator —— 鉴权（provider 顺序启动时配置）

类型以 [`internal/auth/auth.go`](../internal/auth/auth.go) 为准，宿主使用 `server.Identity` / `server.Authenticator` 别名。Identity 的 TokenId 用于同平台 kick 仲裁：native 取 jti，external 取 token 的 SHA-256 前 16 字节 hex。

| provider | 机制 |
| --- | --- |
| `external_jwt` | 用密钥列表依次验证 HS256，映射平台用户／agent，缺省角色取配置。**不查 TokenStore**，吊销归平台；claims 见 [平台 JWT](integration.md#client-tokens-platform-jwt) |
| `native` | 验证自签 HS256 和有效期后，再确认 TokenStore 中仍是该平台当前 token；签发字段见 [Native tokens](integration.md#native-tokens) |
| `chain` | 按 `auth.providers` 顺序逐个试，第一个通过者胜出；全部失败返回最后一个错误。未列出的 provider 不加载，对应路由也不注册 |

部署形态由 `auth.providers` 在**启动时**决定：`[external_jwt]`（随平台部署，生产主路径）、`[native]`（独立部署，没有外部平台）、`[external_jwt, native]`（过渡 / 测试）。

**platform_id**：native 以签名中的 `pid` 为准；external 由请求参数补充，仅影响该用户的连接分组与 kick 范围。请求来源、默认值与范围由 [接入协议](integration.md#client-tokens-platform-jwt) 定义。

**校验时机**：HTTP 鉴权中间件、WS 握手做完整 `Verify`（native 含一次 `TokenStore.Check`）；WS 连接上每帧比较 `ExpiresAt`（本地零成本）；每 60s 定时对 native 连接再做一次 `TokenStore.Check`，即使 `kick` 事件丢失，不同旧 token 也会在后续检查时失效；单次查询另有 5s context 预算，关闭还需队列排空，不承诺严格 1 分钟上限。`TokenStore` 不可用时前两次跳过，连续第三次失败则踢；成功检查会重置失败计数，握手仍 fail-closed。过期/失效 → `2002 KickOnline{reason:"token_expired"}` 后关闭。

### 6.3 Cache 与 TokenStore —— Redis 的 PG 替身

TokenStore 与 internal HMAC nonce 去重通过同一个 [`Cache` 接口](../internal/cache/cache.go)，支持 Redis、PG 和 local；TTL ≤ 0 表示不过期。接口只保留当前消费者需要的 Get、Set、SetNX、DelIfValue 与资源关闭；不预留批量读取、无条件删除、计数或单独修改 TTL。缩小内部接口不改变 token/nonce 的键、值、有效期或存储表，已有部署无需迁移。

| 实现 | 机制 |
| --- | --- |
| `redis` | GET、SET（可带 TTL/NX）与 Lua 原子比较删除 |
| `pg` | sqlc + pgx 操作普通 `cache` 表；读取按 DB `now()` 过滤过期项；SetNX 以单语句 upsert 把过期键视为不存在；cleaner 按 `cache.cleaner_interval` 分批删除过期项（每批 ≤1000 行、`FOR UPDATE SKIP LOCKED`），多节点不互相阻塞 |
| `local` | 进程内 map + TTL；仅单机 / 测试。多节点启用 native 或 internal 通道时必须用 redis / pg，否则 token 与 HMAC nonce 只在本节点有效（随机 401、跨节点重放） |

- `pg` 只支持 PostgreSQL（`SKIP LOCKED` / `ON CONFLICT`）；MySQL 部署本来就必须有 Redis（无 LISTEN/NOTIFY），组合表不变。
- `pg` cache 始终另开一个小 pgxpool（≤4 连接，启动时 Ping），与 `db.access` 无关；宿主注入 DB 连接时也一样，所以仍需 `db.dsn`（[嵌入规则](embedding.md#规则)）。
- 表不用 `UNLOGGED`，避免 PG 崩溃恢复清空 token、导致全体重新登录；key 统一前缀 `nexo:`。

**TokenStore**（自建 token 的每平台单 token）建在 Cache 之上，**只有一个实现**：

[`internal/tokenstore.TokenStore`](../internal/tokenstore/tokenstore.go) 是具体结构体，提供 `Set`、`Check`、`Delete`，均携带 userId、platformId 与 tokenId；Set 的 TTL 为 token 剩余有效期。键为 `nexo:tok:{user_id}:{platform_id}`，值为 token_id，覆盖旧值即使同平台旧 token 失效。

**Logout 只注销请求 token**：将已验证 Identity.TokenId 传给 Delete，通过 `Cache.DelIfValue(ctx, key, expected)` 原子比较并删除；键不存在、已过期或当前值不匹配时为成功无操作。local 在同一互斥锁内比较／删除，Redis 用单次 Lua，PG 用单条条件 DELETE，不得退回 Get 后 Del。A 已通过鉴权后若 B 在同平台登录，A 的迟到 Logout 返回成功但不得删除 B。若 A 在请求鉴权前已被替换，则仍返回 401。嵌入调用须传入非空 TokenId；缺失返回参数错误。SDK 的并发本地 token 写入规则不因此改变，仍见 §15.2。

外部 token 不进 TokenStore。native Bearer 验证或 WS 握手读取 TokenStore 失败时 fail-closed，返回 **HTTP 503 + `20101 AuthUnavailable`**，不是 401；external 验证不依赖 TokenStore。Login 写 token、Logout 在 Bearer 验证成功后删除 token 失败，返回 **HTTP 500 + `20001`**；若 Logout 先在 Bearer 读取阶段失败，则仍是 `503/20101`。这些依赖故障不能证明凭据失效，客户端不应据此清掉本地 token。已建立的 native WS 沿用 §6.2 的三次连续失败关闭策略。
Cache 不参与 seq 分配；消息与 seq 仍在同一数据库事务内持久化，后续设想见 §13。

### 6.4 Internal 通道 —— 平台后端代用户操作

头、签名字节与 TLS 条件统一见 [接入协议](integration.md#internal-channel-backend--nexo-hmac)。签名必须覆盖代操作身份及原始 request-target，否则持有合法签名的调用方仍能篡改用户、平台或查询条件；nonce 只防重放，不能代替完整签名。

验签顺序：服务白名单 → 时间窗 → 常量时间比较签名 → `Cache.SetNX("nexo:inonce:"+service+":"+nonce, ttl=max_skew_seconds×2)` 去重。Cache 不可用时 fail-closed，错误映射见 [鉴权依赖故障](integration.md#http-envelope-and-error-codes)。internal 通道只在 TLS 或仅内网可达的监听上暴露。

`InternalAuth` 只验签；`InternalAuthAsUser` 另注入 `Identity{UserId, PlatformId, Source:"internal"}`。同一业务 handler 复用两种入口，路由见 §9。

### 6.5 OnlineStore —— 全局在线连接登记

[`OnlineStore`](../internal/onlinestore/onlinestore.go) 管理节点连接的登记、续期、删除与清理；`Online` 查询全局有效连接并汇总用户的平台列表。

| 实现 | 机制 |
| --- | --- |
| `db` | `online_conns` 表；`Renew` 仅更新本节点存活快照中的 conn_id，按 [`db.go`](../internal/onlinestore/db/db.go) 的批次上限分批，同轮共用一个心跳时间；`Online` 查询有效心跳并汇总用户的平台 |
| `redis` | 每用户一个 ZSET `nexo:online:{user_id}`，member = `platform:node:conn_id`，score = 过期时间戳；`Renew` = pipeline ZADD，补回仍存活但登记已过期/被清扫的连接，并清理过期成员；`Online` 只读，用 pipeline `ZRANGEBYSCORE` 按有效期过滤并解析 platform |

网关有序执行 Add、快照生成与 Renew、Remove；迟到 Add 必须检查连接仍存活且未进入 draining，旧快照不能在 Remove 之后重新登记。首次 Add 失败保持 WS 可用，由下一次续期在同一 presence 锁下补登记；先批量 Renew 存活快照，再重试失败的 Add，避免单个补登记失败阻断其他连接的心跳。成功后不重复 Add；DB Renew 不能只按 node_id 续期整节点，否则 Remove 失败留下的离线连接会一直有效。conn_id 分批控制 SQL 参数数量，避免大节点超过 MySQL 占位符上限。登记操作不持有 UserMap 锁访问网络。socket 关闭与本地注销同步完成，Remove 转入受限、被退出流程跟踪的后台任务（每节点最多 64 个，含等待 presence 锁的任务）；每项最多 5 秒并受 §10 统一退出期限约束。任务满或清理失败时不阻塞事件回调，残留登记由 TTL 或节点 Purge 清理。

消费者是离线推送判定（§6.6）、在线状态查询接口（§9 `/user/online_status`）与在线状态订阅（§7.4），均复用同一全局查询。**不参与消息推送路由、不参与踢人**。有效期与续期间隔由 `online_store.ttl` / `renew_interval` 控制，续期必须早于有效期；节点宕机后残留登记靠 TTL 失效。
不建在 Cache 之上：`Online(userIDs)` 要按用户枚举连接，纯 KV 没有前缀枚举（Redis SCAN 是全键空间），Redis 用 ZSET、PG 用 `online_conns` 表各自最合适。

### 6.6 Pusher —— 离线推送接口（上层实现 APNs/FCM）

`Notification` 只带事实，不带文案。谁是发送者、群叫什么，是宿主自己系统的事（用户名可能来自外部平台），用 `SenderId` / `GroupId` 去查；正文由 push 侧决定是否解析、怎么解析。

完整 `Notification` / `Pusher` 声明见 [`internal/offlinepush/offlinepush.go`](../internal/offlinepush/offlinepush.go)，宿主使用对应的 `server` 类型别名；`EventId()` 为 `conversation_id + ":" + seq`，`Preview()` 委托 `msgbody`。

解析与默认预览由公开的 [`msgbody/`](../msgbody/) 提供，只依赖标准库，供服务端、宿主及 webhook 接收方共用。字段声明与预览算法以源码为准；推荐 content 形状见 [WebSocket 协议](integration.md#websocket)。发送接口只验证已知类型、JSON 合法性和大小，不按类型强制对象结构，因此存储成功不保证 `msgbody.Parse` 成功或产生非空预览。

| 实现 | 说明 |
| --- | --- |
| `noop` | 默认；只打 debug 日志 |
| `webhook` | 一次尝试、超时不重试，不保证 app push 到达。签名、HTTPS、禁止重定向与接收方幂等规则见 [Webhook 协议](integration.md#offline-push-webhook) |
| 自定义 | 组装时注入：`server.New(ctx, cfg, server.WithOfflinePusher(myPusher))` |

**触发规则**：只对新提交消息，在发送节点提交并 `Publish` 之后异步执行**一次**；幂等重发布不再触发（不在接收事件的节点做，否则 N 个节点重复推）。

```
targets := 单聊 [recv_id] / 群聊 members(group_id) - sender
targets  = 过滤 user_conversations.recv_msg_opt != 0（免打扰）
online  := keys(OnlineStore.Online(targets))
offline := targets - online
if len(offline) > 0 { Pusher.Push(ctx, offline, n) }   // 失败只记日志
```

## 7. WebSocket 网关

### 7.1 握手

参数、身份来源、编码与错误状态见 [WebSocket 协议](integration.md#websocket)。编码和压缩参数保留后续扩展位置，当前只支持 JSON 文本帧、不压缩。

握手步骤：`Authenticator.Verify` → 预留 UserMap 配额 → 生成 conn_id → Upgrade → Hijack 回调接管配额并注册连接 → `OnlineStore.Add` → 广播 `kick{user_id, platform_id, keep_token_id=Identity.TokenId}` → 进入读循环（native 连接每 60s `TokenStore.Check`）。预留计入用户、token、IP 和节点限额；同步拒绝、HTTP 101 写出失败、未接管时节点退出都必须归还一次。Upgrade 返回 nil 不代表回调已接管，不能在 HTTP handler 返回时提前释放；请求最终结束和接管之间必须有唯一所有权转移，已归还的预留不得被迟到回调再次采纳。等待接管的工作随节点退出取消并纳入现有退出期限。
预留等待接管采用与出站写入相同的时间预算（见 [`client.go`](../internal/gateway/client.go) 的 `writeWait`）；成功接管取消等待。通常由 Hertz 请求完成信号立即回收失败预留；宿主禁用 RequestContext 池或调用 Exile 时，完成信号可能不关闭，仍由时间预算兜底归还。超过预算才进入的回调关闭连接，客户端重新握手。该预算只约束预留配额，不保证强制中断宿主 HTTP 写入。
连接关闭：UserMap 移除 → `OnlineStore.Remove`。

### 7.2 帧格式（JSON 文本帧）

帧结构、编号、错误映射与心跳约定见 [WebSocket 协议](integration.md#websocket)。`data` 直接承载嵌套 JSON，避免额外的 base64 解码；网关只分派请求，业务事务仍由 service 执行。未实现的编号与能力只在 §16 草案中定义。

### 7.3 连接模型

- 每连接一个 `readLoop`、一个 `writeLoop`，send 缓冲由 `ws.send_queue` 控制；
- `ClientConn` 接口隔离 gorilla，使推送逻辑可以用 mock 连接单测；
- 所有出站（ACK、push、kick、resync）都进 `send` 队列，**单写协程**，避免并发写；
- 写超时 10s；队列满（慢消费者）→ 计数 + 直接关连接，客户端重连后按 seq 拉取。**不静默丢帧**；
- 进入 draining 后停止准入新帧，readLoop 不抢先关闭 socket，由 writer 排空已有帧后关闭；整个排空过程最多 10s（含最后控制帧），节点退出期限更早时以该期限为准。已准入请求保留原有语义：可在关闭前完成，实际关闭取消连接 context；不承诺撤销已提交事务，也不保证每个在途请求都收到 ACK，未确认发送仍用原 client_msg_id 重试；
- `readLoop` 内 panic recover，帧大小受 `ws.max_frame_bytes` 限制；业务 handler 出错只回错误帧，不断连；
- `UserMap` 按 user ID 索引并受读写锁保护，`GetAll` 返回切片副本；注册／注销有序，避免已关闭连接被迟到的注册重新加入；
- 网关定期以 UserMap 全量快照续期，遵守 §6.5 的 presence 并发约束；
- **连接与速率限制**（配置见 §11 `limits`，任一超限：握手阶段返回 429，运行中返回 `10005 TooManyRequests` 错误帧，连续超限 3 次关连接）：
  - 连接配额按 user、token、来源 IP 和节点分别计数；仅信任 `server.trusted_proxies` 中的代理提供的转发 IP，否则使用 socket peer；
  - 入站帧令牌桶在 JSON 解码前扣配额，畸形帧也计入；超限响应与连续关闭规则见 [WebSocket 协议](integration.md#websocket)。`ws_inflight_per_conn` 超限直接拒绝、不排队；
  - 每用户发送配额跨连接合计，HTTP/WS 共用，internal 通道不限；
  - `ws_send_bytes_total` 限制本节点推送队列总字节：入队前记账，2001 / 2003 超限丢弃并发 2004 Resync；2005 超限直接丢弃，由下次在线快照恢复，不触发消息补拉。ACK、错误、Kick 和 Resync 自身不受限。上限为 0 只关闭限制，仍记账；写出、入队失败或关闭均恰好释放已累计字节；
  - native 注册／登录另受 `auth_per_ip_per_min` 限制（0 关闭），也可由 nginx `limit_req` 承担。
  限流计数全部为节点本地内存（不走 Cache），多节点下上限按节点数放大，一期接受。
  按 key 的令牌桶只跟踪有界数量（登录 IP 10000、消息发送者 100000）；满表时未跟踪 key 直接共用 overflow 桶，不额外扫描。空闲超过 5 分钟的 entry 由 Allow 中原有的到期检查回收（距上次 sweep 超过 5 分钟时触发），overflow 请求不重置这个周期；容量、现有 key 独立桶和 overflow 配额不变。
- **握手 Origin 校验**：浏览器不对 WebSocket 施加同源策略，任何页面都能带着浏览器里的凭证发起握手，因此 `ws.allowed_origins` 是唯一的防线。留空 = 不校验（默认），只在 token 从不放进 cookie 时才安全；非浏览器客户端不发 `Origin`，恒放行。
- **握手失败的状态码**：`10601` 超连接数上限 → 429（退避后重试），`10604` 节点正在优雅下线 → 503（立即重连，LB 会换一个节点），两者用不同 errcode 而不是靠 message 文本区分。

### 7.4 在线状态订阅

订阅跟随 WS 连接，只覆盖客户端当前需要展示的用户。协议与客户端恢复规则统一见 [Online status subscriptions](integration.md#online-status-subscriptions)。gateway 管订阅集合、反向索引、引用计数和刷新调度，经 app 注入的 `user.Service.OnlineStatus` 批量查询；业务读取不直接访问 Redis 或 Store。不增加持久订阅表、服务包间依赖或跨连接的在线状态缓存，不改变消息同步兜底。

接受非空订阅时安排首次刷新，之后周期重读当前集合；每次成功读取都推完整快照，不维护差异基线。周期必须重新访问 OnlineStore，不能靠重发旧缓存续期：DB cutoff 与 Redis score 只在查询时排除过期登记，不产生过期事件。单连接最多一个在途读取；订阅切换、取消、关闭后丢弃迟到结果。采纳结果、组装与入队保持同连接有序，避免旧快照覆盖新状态；读取与其他网络 I/O 在状态锁外进行，因队列满关闭 socket 也在锁外完成。

节点使用有界刷新执行位，每连接最多保留一个待刷新标记，首次刷新、周期与事件触发共用该路径。Bus 回调只通过订阅索引标记受影响的本地连接；无本地订阅直接忽略，重复信号合并，不为每条事件建立读取或重试任务。刷新受每连接最小读取间隔约束，事件和 revision 变化不能绕过它；待刷新连接按到期时间排队，同一刻到期的按入队顺序先到先服务。调度没有固定扫描周期：定时器只按队首的到期时间设定，没有到期项的节点不做任何扫描。订阅数、节点引用总数、读取并发、最小读取间隔、快照周期、新鲜度期限和写出速率的取值统一见 [`internal/gateway/presence.go`](../internal/gateway/presence.go)，不增加配置键；读取超时沿用网关统一的 `connOpTimeout`（见 [`gateway.go`](../internal/gateway/gateway.go)）。订阅取消和连接注销立即释放索引与配额，调度和读取纳入现有 Gateway 工作跟踪及统一 Shutdown 期限。

读取失败不伪造离线、不重发旧状态快照，也不刷新成功读取时间。当前 revision 已有成功读取后，持续失败达到新鲜度期限时发送 `stale`；初次订阅或新 revision 从未读取成功时不发帧，由客户端独立期限显示未知。下一次读取成功恢复完整快照。客户端仍须独立超时，因为 `stale` 本身可能丢失。2005 受节点写出速率与现有字节预算约束：写出速率作为准入控制在读取 OnlineStore 之前生效，超速的连接排队等待下一次刷新而不是读完再丢帧（代价是没有产生帧的读取同样占用一次名额，令牌只能回退最近一次预约）；字节预算仍在写出时生效，超预算直接丢弃并计数，不替换为 2004。send 队列满仍关闭连接。相关累计计数沿用 [`gateway.Stats`](../internal/gateway/gateway.go)，没有独立监控依赖；`OnlineReadFails` 只计实际读取失败，订阅替换或连接关闭导致的取消不计为依赖故障。

成功 Add、Remove 及 Renew 后，gateway 向 Bus 发布受影响用户的 `presence_changed`。Renew 只发布本次真正补回的登记：驱动返回它新建而非续期的连接（Redis 先按 score 清理再 ZAdd，用新增计数判定；DB 的 UPDATE 建不出行，恒返回空），网关再并上本次补登成功的连接。稳态续期因此不发事件——按本节点连接批量发布会在每个心跳周期把所有节点上的相关订阅标记为待刷新，把刷新从快照周期拉到最小读取间隔，而在线状态并没有变化。驱动看不见的补回（DB 行心跳超期后被重新刷新）由周期查询兜底。未真正写入的 Add 不发布：连接已退出或已登记时 Add 直接返回，这种空操作不算成功 Add。Remove 仍无条件执行并在成功后发布，因为 Renew 会为 Add 失败的连接补建登记，节点无法只凭本地状态判断该连接是否有行。发布不持有登记或订阅状态锁，不阻塞 Bus 回调，不改变登记成功与否，也不重试。用户列表按 §6.1 的固定片大小分片，每片各尝试一次；单片失败只计一次失败并继续发布其余分片。本节点 `node_id` 把事件封装撑出预算时整次发布放弃并计一次失败，不改小分片、不静默降级。事件只加速重读，不携带权威状态；事件丢失、节点宕机或清理失败最终由周期查询发现。异常离线延迟为最后一次成功登记／续期的剩余 TTL，加上等待下次成功读取及发送的时间；TTL 加快照周期只是依赖健康且调度正常时的粗略预算，不是下界或故障期间硬上界。

本版启动即发布新事件，没有独立启用开关。同一 Bus 的节点必须协调升级到兼容版本后再启动服务，或全量替换；不能把滚动混部视为已经满足启用条件。旧节点对 1006 的回落仍按接入协议处理。

## 8. 核心流程

### 8.1 登录 / 建号
- **平台用户**（生产主路径）：无登录接口。平台后端在用户注册/改资料时调 `POST /internal/user/upsert {id:"u___123", nickname, avatar, extra}`；客户端直接拿平台 token 访问 nexo。
- **自建账号**（`auth.providers` 含 `native`）：`/auth/register {username, password, nickname}` → id `nx__<uuidv7>`；`/auth/login {username, password, platform_id}` → 生成 `jti` → 签 JWT `{sub, pid, jti, exp}` → `TokenStore.Set(user, platform, jti, ttl)`：**同平台旧 token 立即失效**（HTTP 请求／WS 握手下一次验证返回 401；已建立 WS 在定时复检发现失效后发送 `2002/token_expired` 并关闭，见 §6.2）；`/auth/logout` → `TokenStore.Delete(user, platform, request_token_id)`，仅原子删除仍匹配的请求 token（§6.3）。

### 8.2 WS 连接 + 跨节点互踢

```
Client(iOS, token T2) ──WS──► nexo2: Verify(T2) → 注册 → OnlineStore.Add → Publish kick{u, iOS, keep=hash(T2)}
                                                                        │
      nexo1 (持有 u/iOS/T1 的旧连接) ◄──── Bus ─────────────────────────┤
        └─ 发 2002 KickOnline{new_login} → Close → OnlineStore.Remove   │
      nexo2 自己 ◄──────────────────────────────────────────────────────┘ (本地无 T1 连接，无操作)
```

握手的在线登记返回后，只有仍存活且未进入 draining 的连接可以发起 kick；Bus 发布保留连接取消信号，关闭或 draining 时取消未完成的发布，取消后不得执行本地兜底 kick。已被 Bus 接受的在途事件仍按原有 at-most-once 语义处理，不新增跨节点代次排序保证。

本地 kick 仲裁与连接关闭／进入 draining 共用节点内的短临界区：先取得 UserMap 快照，再在仲裁锁内检查源连接仍 active、节点未退出，并将目标连接标记为 draining、取消其 active context；发帧、关闭 socket 和 presence 清理均在锁外执行。关闭或 draining 先取得锁，则旧源连接不得执行本地兜底；兜底先取得锁，则目标在源连接失效前已完成状态转换。仲裁锁不得嵌套 UserMap 锁或执行网络 I/O。

同一 token 重连（网络抖动）：keep_token_id 相同，不互踢。失联旧连接在没有消息／pong 续期时由读超时回收（默认 75s）；仍能续期的旧连接可与新连接并存，直到主动关闭、I/O 错误、token 到期或其他关闭条件发生，不保证重连后 75s 内退出。
不同 native token 的旧连接还有第二道校验：即使 `kick` 事件丢失，下一次定时 `TokenStore.Check`（默认间隔 60s，另有查询与排空耗时）也会发现 token 已被替换。同 token 不会因此失效。external 不查询 TokenStore；丢失 kick 后，持续活跃的旧连接可保留至 token 到期，不能靠读超时承诺固定回收上限。

### 8.3 发送单聊消息（WS 1003 / HTTP /message/send / internal /message/send，同一 service）

```
1. 参数校验；recv 用户存在（否则 10201）
2. 快路径幂等：SELECT messages WHERE conversation_id=? AND sender_id=? AND client_msg_id=?
   命中：发送额度允许时重新 Publish，始终返回原 ACK；未命中：先检查并扣发送额度
3. Store.WithTx:
   a. INSERT conversations(conversation_id, type=1, max_seq=0) ON CONFLICT DO NOTHING
   b. SELECT ... FROM conversations WHERE conversation_id=? FOR UPDATE     ← 串行化点
   c. seq = max_seq + 1
   d. INSERT messages(conversation_id, seq, ...) ON CONFLICT DO NOTHING
        ← 影响行数 0（并发双发）→ 回滚 → 回查 → 重新 Publish → 返回同一 ACK，seq 未消耗
   e. UPDATE conversations SET max_seq=seq, updated_at=now
   f. UPSERT user_conversations 两行：
        sender: peer=recv, updated_at=now, sender_read ? read_seq=GREATEST(read_seq, seq) : read_seq 不动
        recv:   peer=sender, updated_at=now            (read_seq 不动 → 未读 +1)
4. 提交事务（此时尚未返回 ACK）
5. 同步 Publish push{conversation_id, seq, session_type=1, sender_id, sender_conn_id, recv_id, msg}
   context 脱离原请求取消，另设 5s 超时；发布失败不回滚已提交消息
6. 订阅节点独立处理事件：目标 [sender, recv] → 本地 UserMap → 投递（排除 sender_conn_id），发送方法不等待投递完成
7. 发送节点启动异步离线推送：按 §6.6 规则算 offline 集合 → Pusher.Push
8. 返回 ACK（不等待步骤 7 的任务完成）
```

`sender_read=false`（平台发自定义消息时用）：sender 自己的各端也会收到 2001 推送并显示未读；HTTP/internal 路径没有 `sender_conn_id`，sender 所有连接都收。

**幂等命中的重新发布**：两条回查路径都使用已存消息构造 `PushEvent`，仅 `SenderConnId` 使用本次请求的连接；重试请求中的正文等字段不能覆盖原消息。快路径共用 `message_send_per_min` 额度，额度不足只跳过 Publish，仍返回原 ACK；群聊快路径在扣额度前复核发送方成员资格与群状态，不满足只跳过 Publish、不扣额度，仍返回原 ACK；该复核不在会话行锁内，与并发退群竞争时可能仍放行一次重发布，接收端按 `(conversation_id, seq)` 去重；事务内 duplicate 分支在进入事务前已经扣过额度，不能再扣。`Unlimited` 沿用原有豁免。重新发布与正常发布使用相同的有界、脱离原请求取消的 context，经过相同的接收端可见性校验；不限制消息年龄，不改变时间戳、seq、事务或会话数据，也不再次计算离线推送。离线通知的收件人会随时间变化，重复整批通知不能代替每用户补偿；这种补偿不属于当前能力。重发布尝试计数口径见 §6.1。

崩溃点分析：事务提交前崩溃，无 ACK，客户端用原 client_msg_id 重试；提交后、Publish 前崩溃，后续 Send 幂等命中可重新发布以加速恢复，同时返回原 ACK。重新发布仍可能被限流跳过或发布失败，发送方也可能不再重试，因此兜底仍是客户端周期对账，不承诺恢复时限。Publish 后也可能在 ACK 到达前崩溃，不能把未收到 ACK 当成未落库。步骤 7 前崩溃或该任务失败只少一次 app push，幂等重试不补发它，无数据影响。5s 是协作式 context 预算，不会强制打断不遵守 context 的宿主 Publisher；实时推送与离线推送的完成都不保证排在 ACK 前或后。

### 8.4 发送群聊消息
与 8.3 相同，差异：
- 步骤 1 只做参数校验和 `chat_groups` 存在性预检（快速失败）；**成员资格与群状态的最终校验放在步骤 3b 拿到 `conversations` 行锁之后**，同一事务内 `SELECT 1 FROM group_members WHERE group_id=? AND user_id=?` 且 `chat_groups.status` 未解散，不满足 → 回滚返回 403。因为退群/踢人/解散事务（§8.5）都先锁同一行，"校验通过后被踢仍能发出"的窗口被关闭；
- 步骤 f 改为一条语句：`UPDATE user_conversations SET updated_at=now WHERE conversation_id=? AND max_seq=0`，再按 `sender_read` 单独 upsert sender 的 read_seq；
- 事件带 `group_id` 不带成员列表；接收节点用 `GroupMemberCache` 解析；
- 步骤 7 的 targets 为成员列表减去 sender，并过滤免打扰；
- 步骤 2 幂等命中后，重新 Publish 前按 3b 的条件复核成员资格与群状态，但不在行锁内；不满足只跳过 Publish 并返回原 ACK，不返回 403。

### 8.5 入群 / 退群（seq 边界，与发消息抢同一把行锁）

入群事务：
```
1. SELECT conversations(sg_<gid>) FOR UPDATE  (不存在先 INSERT)
2. INSERT group_members(…, inviter_user_id)
3. UPSERT user_conversations(owner=u, conv=sg_<gid>):
     min_seq = max_seq + 1, max_seq = 0, read_seq = conversations.max_seq, updated_at = now
4. 提交 → Publish group_changed{gid}
```
退群 / 被踢事务：
```
1. SELECT conversations(sg_<gid>) FOR UPDATE
2. DELETE group_members
3. 若 conversations.max_seq > 0：UPDATE user_conversations SET max_seq = conversations.max_seq
   否则：DELETE 对应 user_conversations 行，避免 0 被解释为无上界
4. 提交 → Publish group_changed{gid}
```
因为入群、退群、发消息都锁同一行，"入群前的消息不可见 / 退群后的消息不可见"在并发下也严格成立。重新入群时 min_seq 重算、max_seq 归零。
建群时初始成员 `min_seq = 1`（能看全部历史），批量 INSERT，不逐条。

### 8.6 客户端同步

客户端只持久化每会话的连续同步基线 `local_max`，不要求完整本地消息库（A13）。[接入协议](integration.md#client-synchronization) 定义补拉、去重及重入群的基线处理。

`updated_at` 游标会随更新移动，翻页结束不是并发场景下的一致性覆盖证明；静默漏掉末条推送也不会自动形成 seq gap，因此不能移除周期对账兜底。未实现的恢复增量见 §16。

### 8.7 已读
`MarkRead(conv, read_seq)` → 校验归属 → `read_seq = GREATEST(read_seq, min(?, visible_max))` → Publish `conv_read` → 自己其他端收到 2003。一期不通知对端（已读回执二期）。

### 8.8 会话列表（服务端排序 + 服务端返回 last_message）

```
1. SELECT uc.*, c.max_seq FROM user_conversations uc JOIN conversations c USING(conversation_id)
   WHERE owner_id=? AND (updated_at, conversation_id) < (cursor_updated_at, cursor_conv_id)
   ORDER BY updated_at DESC, conversation_id DESC LIMIT limit+1          ← 走索引 (owner_id, updated_at DESC, conversation_id)
2. 每行算 visible_max、unread（§5.3/5.4）
3. with_last_message：SELECT * FROM messages WHERE (conversation_id, seq) IN ((c1, v1), (c2, v2), …)
   ← 主键点查，数量受分页上限约束；visible_max < min_seq 时无 last_message。MySQL 8 与 PG 均支持行构造器 IN
```
请求、响应、游标编码和置顶展示约定见 [HTTP API](integration.md#http-api)。

### 8.9 离线推送（发送节点视角）

触发顺序统一见 §8.3 的步骤 5–8，目标筛选见 §6.6；失败只影响本次 app push，不改变已提交消息或 ACK。Webhook wire 约定见 [接入协议](integration.md#offline-push-webhook)。

## 9. HTTP API

[公开接口与 Internal 目录](integration.md#http-api)、[响应及错误映射](integration.md#http-envelope-and-error-codes) 统一维护在接入文档。路由注册见 [`internal/api/server.go`](../internal/api/server.go)，请求/响应类型见 [`sdk/types.go`](../sdk/types.go)。

HTTP、WS 与进程内调用共享 service 的校验和事务；Internal 入口只替换身份验证方式，不绕过业务权限。

## 10. 多机部署要点与故障矩阵

要点：
- **无状态节点**：WS 连接、在线订阅与群成员缓存都是可丢弃的进程内状态；任何节点可服务任何用户。
- **LB**：透传 HTTP/1.1 Upgrade/Connection 头，代理读超时须大于 WS 心跳／读超时预算；示例配置见 [`deploy/nginx.conf`](../deploy/nginx.conf)。不需要 ip_hash。
- **PG 连接**：`bus=postgres` 时 LISTEN 连接必须直连主库或 PgBouncer session 模式。
- **优雅退出**：收到 SIGTERM → 停止接受新连接 → 取消 Bus 订阅、在线续期与订阅刷新 → 给所有 WS 发 close(1001 going away) 并排空队列 → 清理在线登记。排空、续期停止、订阅读取、Remove、Purge、离线推送等待共用调用方的绝对退出期限；期限到达立即硬关底层 socket，不等待控制帧或为每个连接重新分配 5 秒。socket 关闭时立即注销本地连接并释放配额，不等待全局登记清理。只在退出期限内等待在途工作；期限耗尽返回 context 错误，不再等待驱动网络 I/O 或提交后的后台发布，宿主随后关闭依赖，未结束的调用由依赖关闭或自身超时收尾。清理超时的残留登记靠 TTL 失效。客户端重连到别的节点后走 8.6 同步。HTTP 在途请求结束后，关闭依赖前等待离线推送；最多 5 秒且不得超出本次退出的剩余期限。`Shutdown(ctx)` 传播等待超时；分步 `Drain(ctx)` / `Close()` 沿用 Drain 的期限，Close 的无返回值 API 保持兼容并记录错误。未调用 Drain 的 Close 使用独立 5 秒上限。
- **DB 连接池**：先给运维／迁移等预留连接（例如 20 条）。每节点峰值预算为 Store 的 `max_open_conns`，加上 `bus=postgres` 时的最多 4 条发布池连接和 1 条独占 LISTEN，再加上 `cache=pg` 时独立池的最多 4 条连接（与 sqlc/GORM 无关）。总预算不得超过 DB 的 `max_connections`；PG-only 的 Store 上限应满足 `max_open_conns ≤ (max_connections - 运维预留) / 节点数 - 9`。这里是容量上限，不是启动时固定建立的连接数。
- **迁移**：部署前单独执行 `nexo migrate`，节点启动不跑 DDL。
- **时钟**：`send_time` 用服务端时间，仅展示用；顺序完全靠 seq。`online_conns.heartbeat_at` 与 internal 通道的时间窗都依赖节点间时钟一致，需 NTP。
- **密钥轮换**：`auth.external_jwt.secrets` 为列表，平台换密钥时新旧并存一段时间再删旧的。Internal 单密钥使用 `internal_auth.secret: "<old-key>"`；轮换时清空该单值字段，改用 `internal_auth.secrets: ["<new-key>", "<old-key>"]`，待签名方切换完成并经过请求有效窗口后移除旧值。`internal_auth.secret` 本身不接受列表。
- **日志脱敏**：请求日志默认不打 body（`log.request_body=false`）——IM 的 body 就是消息正文；打开后 login / register / message.send（含 internal）仍强制不打，`log.redact_paths` 只是往这个集合里追加。`Authorization` / `X-Signature` 头永不打。`log.skip_paths`（默认 `/healthz`）里的路径整行不写，避免探针刷屏。启动配置中的非空 DSN 整段隐藏。示例 nginx 不记录查询串，并在主级将原始 error log 写入 `/dev/null`，牺牲错误诊断日志以避免 WS query token 落盘；若需错误详情，必须在任何持久化之前脱敏，不能先写容器日志再处理。

| 故障 | 影响 | 恢复 |
| --- | --- | --- |
| 某节点崩溃 | 其上连接断开；已提交发送可能未 ACK；在线登记残留到 TTL 失效 | 客户端重连到其他节点 → GetMaxSeqs 补齐；重试发送走幂等；残留行过期被忽略，节点重启时 PurgeNode |
| Bus 丢事件（Redis/PG 连接抖动）| 少一次实时推送 / 少一次 kick / 成员缓存短暂过期 / 少一次在线快照加速 | 重连后 Resync 触发消息补齐；native 复检与 external 回收边界见 §8.2；成员缓存由事件失效或 TTL 过期；在线订阅靠周期重读 |
| Redis 宕机（作 Bus）| 消息事件推送停止；Publish 报错只记日志，消息照常落库；在线快照仍可经健康的 OnlineStore 周期读取 | go-redis 自动重连 → Resync；客户端拉取消息 |
| Redis 宕机（作 Cache）| native Bearer／WS 握手 `503/20101`；token 写入／删除 `500/20001`；internal nonce `500/20002`，见 §6.3／§6.4。external 验证不依赖 Cache | 恢复后重试，不能据依赖故障清 token；PG 部署也可选 `cache=pg` |
| Redis 宕机（作 OnlineStore）| `Online` 报错 → 本次不发 app push（fail-closed）；`/user/online_status` 返回 20001；在线订阅读取失败不刷新状态，客户端按协议显示未知 | 恢复后下一次读取返回完整在线快照 |
| PG LISTEN 连接断 | 同"Bus 丢事件" | 独占连接重连循环，退避 1s→30s → Resync |
| 平台密钥泄露/轮换 | — | 改 `secrets` 列表滚动重启；旧 token 到期自然失效 |
| Webhook 推送服务不可用 | 少 app push | 不重试；上层服务自行保证 |
| DB 宕机 | 全部写失败，WS 连接保持 | DB 恢复后自动正常 |
| 重复推送 | 客户端显示重复 | 客户端按 (conversation_id, seq) 去重 |

## 11. 配置约束

完整配置及默认值只维护在 [`config/config.example.yaml`](../config/config.example.yaml) 与 [`internal/config`](../internal/config/)；加载和环境变量用法见 [README](../README.md#configuration)。每个键必须由代码读取，完整示例覆盖配置结构；部署配置仅写必要覆盖值，其余由现有默认值补齐。测试检查未知键与应用默认值后的部署关键设置。

启动时校验：`db.driver=mysql` 要求 `db.access=gorm`；`bus=postgres` 要求 `db.driver=postgres`；`bus/online_store/cache` 任一为 `redis` 要求 `redis.addr` 非空；`cache=pg` 要求 `db.driver=postgres`；`offline_push=webhook` 要求 `webhook_url` 为 `https://` 且 `webhook_secret` 非空；`bus≠local`（多节点）且 `auth.providers` 含 `native` 或 `internal_auth.enabled` 时要求 `cache≠local`（token 与 nonce 必须跨节点共享，§6.3）；`auth.providers` 非空且每个 provider 的 secret 非空、互不相同；`internal_auth.enabled` 要求 secret 非空。

校验通过后打印全部生效配置，secret 打 `***`，非空 DSN 整段隐藏。未被代码读取的配置键不能加入示例。

## 12. 验证入口

[贡献指南](../CONTRIBUTING.md#acceptance) 维护行为验收与真实依赖测试入口；[跨节点压测](load-testing.md) 维护负载档位、逐条正确性核对、报告口径和资源边界。验收要求不等于已经通过的测量结果。

## 13. 后续扩展（不影响一期结构）

在线状态订阅由 §7.4 定义，幂等重发布与 Bus 失败可见化由 §8.3／§6.1 定义；延迟重试与服务端主导消息同步仍是 §16 的设计草案。

- seq 热点缓存（未验证设想）：现行 seq 在消息事务中经会话行锁分配；没有 `Store.AllocSeq` 方法。引入 Cache 分配必须另行设计并证明幂等、并发和回滚不留洞，不能视为简单替换接口；
- at-least-once Bus：outbox 表 + NOTIFY 门铃（PG），或 Redis Streams / NATS：新增一个 Bus 实现；
- MySQL 无 Redis 部署：DB 轮询表版 Bus；
- 平台侧 token 吊销回调：平台通知 nexo 踢掉某用户全部连接（复用 `kick` 事件，`keep_token_id=""`）；
- 帧压缩 / protobuf：握手参数已预留；
- 好友/黑名单、撤回、已读回执、群申请审批、置顶排序、内置 APNs/FCM Pusher。

## 14. 已确认假设

| # | 假设 | 影响范围 |
| --- | --- | --- |
| A1 | 平台账号与 native 账号可独立或混合部署；provider 在启动时选择（§6.2） | auth |
| A2 | 单聊无需好友关系，任意已存在用户可互发 | message service 校验 |
| A3 | 入群无需审批，知道 group_id 即可加入 | group service |
| A4 | 内容、群人数与分页受 `limits` 限制，默认值只在配置源维护（§11） | service / config |
| A5 | 平台编号是稳定协议，见 [平台 JWT](integration.md#client-tokens-platform-jwt) | wire contract |
| A6 | 无全局管理员身份；平台后端经 internal 代用户操作，仍受业务权限约束（§6.4） | auth |
| A7 | 离线通知只携带事实，文案由 push 侧决定（§6.6） | offlinepush / msgbody |
| A8 | OnlineStore 不可用时离线推送 fail-closed，不推送 app 通知 | message service |
| A9 | external token 不含平台，由请求自报；native token 的平台受签名 `pid` 约束（§6.2） | gateway / auth |
| A10 | 客户端收到 `2002 KickOnline` 后不自动重连 | 客户端约定 |
| A11 | 空库起，不迁移旧 nexo_im 的存量数据 | migrations |
| A12 | 平台用户先 upsert 资料，消息收件人必须存在（§5.10） | user / message service |
| A13 | 会话列表的排序与 last_message 由服务端计算返回；客户端不维护本地消息库，只持久化每会话的同步基线 `local_max`（§8.6） | conversation service |
| A14 | PG cache 使用可恢复的普通表和 DB 过期时钟（§6.3） | cache |

## 15. 嵌入与客户端（v3.2）

三种接入方式共用同一份代码，区别只在谁拥有 HTTP 服务器和进程生命周期：

| | 独立服务（`nexo serve`） | 嵌入宿主（`server/`） | 外部系统调 API（`sdk/`） |
| --- | --- | --- | --- |
| HTTP 服务器 | nexo 自建 Hertz | 宿主的 Hertz，`Mount` 挂路由 | 不涉及 |
| 端口 / TLS / 信号 | nexo | 宿主 | 不涉及 |
| 调用业务 | HTTP / internal HMAC | 进程内 `s.Message().Send(...)`，也可走 HTTP | HTTP，Bearer 或 internal HMAC |
| 鉴权 | `auth.providers` 配置 | 同左；或 `WithAuthenticator` 换成宿主自己的 | 由 sdk 带 token / 签名 |
| DB 连接 | DSN | DSN 或宿主注入 `*gorm.DB` / `*pgxpool.Pool` | 不涉及 |

### 15.1 `server/` —— 嵌入入口

`server` 只提供类型别名与生命周期，不搬出 `internal`，不放业务逻辑。外部只能从 `server.New` 取 service 实例，进程内调用也经过 service 的事务和校验；窄仓储接口仍由内部 consumer 定义。别名见 [`server/types.go`](../server/types.go)，导出 DTO 的可命名性由 `server/types_test.go` 校验。

`app` 只组装依赖，不处理信号、全局日志或 Hertz 监听；独立入口拥有这些职责，嵌入时交给宿主。公共 `server.WithXxx` 只解析一次，再将依赖结构直接交给 `app.Build`，内部不重复提供 functional options。service 的依赖、限流器与成员缓存配置在构造时固定，不支持运行时替换；缓存和限流器自身仍保护并发访问。启动、连接注入、退出顺序与限制只维护在 [embedding.md](embedding.md)。

### 15.2 `sdk/` —— HTTP 客户端

`sdk` 是同 module 的独立依赖边界：只用 `net/http`，不 import 本模块其他包，不让客户端引入 GORM / pgx / Hertz。手写 wire 类型与接入协议对齐，HTTP 往返测试通过 `api.Register` 验证；Internal 签名与 `auth.Sign` 逐字节一致。

使用、并发 token 与响应验证契约见 [Go SDK](integration.md#go-sdk)，完整声明见 [`sdk/`](../sdk/)。

## 16. 消息同步增量（草案，未实现）

[sync-design.md](sync-design.md) 保留延迟重试、合并定向 Resync 与服务端主导消息同步的草案，范围、暂缓项、准入门槛和验收条件都在那里维护。这些草案不表示消息同步新协议已可用。幂等重发布、Bus 失败可见化与在线状态订阅已归入本契约，不依赖这些后台补偿草案，也不允许客户端停止消息周期对账。

版本固定的外部证据与历史查询实验见 [sync-research.md](sync-research.md)，不构成新能力已实现或验收通过的证明。
