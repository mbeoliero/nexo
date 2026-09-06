# Nexo IM 系统设计（单体二进制 · 多机部署）

> 参考 open-im-server（本地 `~/projects/go/open-im-server`）的核心模型（conversation / seq / msggateway / offlinepush），
> 去掉微服务、Kafka、Mongo、etcd，做成一个可水平扩展的单体二进制。
>
> 历史修订摘要（旧版本编号保留；当前规范以正文为准，§13 仅为未实施设想）：
> 修订：2026-09-04 v3.2 —— 嵌入与客户端（§15）：`server/` 薄封装（New / Mount / Start / Shutdown + 类型别名，`internal` 不搬），`sdk/` HTTP 客户端；宿主可注入 DB 连接。
> 2026-09-02 v3.1 —— 恢复 TokenStore（自建 token 用），建在 `Cache` 接口上：Redis 或 PG `cache` 表（移植 momo_server `dbcache`）；`auth.providers` 启动配置。
> v4 —— internal 签名覆盖 user/platform/query + nonce 防重放 + TLS；群消息成员校验移入行锁内；WS 连接数/速率限制；推送前按 user_conversations 过滤（缓存只筛候选）；webhook HTTPS+HMAC+幂等键；GetMaxSeqs 分页与重入群同步基线。
> v3 —— 外部平台 token 鉴权（自建回落）、internal HMAC 通道、平台用户 id 格式、会话列表分页 + last_message、
> `sender_read`、在线状态接口；删除 TokenStore；schema 修正（conversation_id 宽度、幂等键、分隔符）。
> v2 —— sqlc+pgx 与 GORM 双实现、Bus 重连 Resync、OnlineStore、离线推送接口。

## 0. 一页总览

| 项 | 结论 |
| --- | --- |
| 形态 | 一个二进制 `nexo`，N 个同构节点，前置 LB；共享 MySQL 或 PostgreSQL；Redis 可选。与外部社交平台 app 同机/同集群部署 |
| 功能 | 用户资料、群（建/加/退/踢/成员）、单聊群聊、会话列表（游标分页 + 未读 + last_message）、已读、WS 实时推送、按 seq 同步、在线状态查询、离线推送接口、平台后端 internal 通道 |
| 不做（一期） | 好友/黑名单、撤回、已读回执、文件存储、消息压缩、open-im SDK 协议兼容、APNs/FCM 具体对接（只给接口）、在线状态订阅 |
| 鉴权 | `Authenticator` 链，provider 顺序由 `auth.providers` **启动时配置**：`external_jwt`（平台 HS256，claims `user_id int64 + role`）、`native`（自建 JWT + `TokenStore` 每平台单 token）。用户 id 固定格式 `u___{id}` / `ag__{id}` / 自建 `nx__{uuid}` |
| 服务间调用 | `/api/v1/internal/**`：HMAC-SHA256 签名，覆盖 nonce、方法、原始路径／查询串、用户／平台头和正文摘要；`X-User-Id` 代用户操作。完整协议见 §6.4，旧签名不兼容 |
| 数据访问 | 仓储接口 `Store` 双实现：MySQL 只能用 GORM 泛型；PostgreSQL 可选 GORM 泛型或 sqlc + pgx/v5 |
| seq 分配 | DB 行锁（`conversations.max_seq` FOR UPDATE），落库同事务，多机天然一致 |
| 跨节点推送 | `Bus` 接口：Redis Pub/Sub 版 + PostgreSQL LISTEN/NOTIFY 版 + 单机 local 版；重连后向本地连接推 `Resync` |
| 多端登录 | 同平台互踢：WS 连接级 kick 广播（两种 token 都有）+ 自建 token 走 `TokenStore` 覆盖旧 token（HTTP 也立即失效）；外部 token 的吊销归平台 |
| 离线推送 | `Pusher` 接口（内置 Noop / Webhook），发送节点对离线目标调用一次；离线判定靠 `OnlineStore` |
| 投递语义 | 实时推送 at-most-once；消息已落库；客户端按 `(conversation_id, seq)` 去重 + 拉取补齐 |
| 共享状态 | 三处：`Bus`、`Cache`（`TokenStore` 建于其上）、`OnlineStore`，各有 Redis 实现和 PG 实现；无 Redis 时 Cache 走一张 PG `cache` 表（移植 momo dbcache）|

多机部署的全部增量就是上表后 6 行。其余与单机 IM 相同。

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

节点身份：`node_id` 来自配置/环境变量，默认 `hostname`。用于日志、事件来源标记、`online_conns` 归属和启动清理。不参与路由。

## 2. 技术栈与关键决策

| 决策点 | 选择 | 原因 / 备注 |
| --- | --- | --- |
| HTTP | Hertz | 沿用偏好；日志用 `mbeoliero/kit/log` 的 `WithHertz()` |
| WebSocket | `hertz-contrib/websocket`（gorilla 分支）| 与 HTTP 同端口，LB 只配一个 upstream。若 netpoll 不支持 Hijack，切 `server.WithTransport(standard.NewTransporter)` |
| 仓储接口 | `store.Store`：全部仓储方法 + `WithTx(ctx, func(Store) error)`；不支持嵌套事务。service 不直接依赖它，只依赖自己声明的窄接口（§3） | service 层定义事务边界；callback 只能使用传入的 tx，不得再次调用 WithTx（包括捕获外层 Store 后调用），不提供嵌套保存点或跨后端一致的嵌套结果 |
| 实现 1：`gormstore` | GORM v1.31 泛型 API（`gorm.G[T]`），MySQL + PG | 一套代码跑双库。方言差异 3 处用 clause 抹平：upsert（`clause.OnConflict`）、行锁（`clause.Locking`）、唯一键冲突识别（`TranslateError: true` → `gorm.ErrDuplicatedKey`） |
| 实现 2：`pgstore` | sqlc 生成代码 + `pgx/v5` pool，仅 PG | SQL 手写、零反射；唯一键冲突用 `pgconn.PgError.Code == "23505"`；事务 `pool.Begin` + `Queries.WithTx` |
| 选择规则 | `db.driver=mysql` → 强制 `gorm`；`db.driver=postgres` → `db.access=gorm\|sqlc` | 启动时校验非法组合 |
| Schema 事实来源 | SQL 迁移文件：`migrations/postgres/*.sql`（同时是 sqlc 的 schema 输入）、`migrations/mysql/*.sql` | 放弃 AutoMigrate。GORM 模型必须与 SQL 一致，靠集成测试矩阵保证 |
| 迁移执行 | `goose`（`pressly/goose/v3`，embed.FS，同时给 sqlc 当 schema 输入），`nexo migrate` 子命令 | 部署前单独执行一次，节点启动不跑 DDL。一期空库起，无存量迁移 |
| PG 直连 | `pgx/v5` | `pgstore` 的驱动；两种 access 下 LISTEN 都用独立 `pgx.Conn`；NOTIFY 走 `pg_notify()` |
| Redis | go-redis v9（可选）| Bus / Cache / OnlineStore 的 Redis 实现；`kit/connector` 初始化 |
| Cache | `internal/cache`：`redis` / `pg`（移植 momo_server `internal/infrastructure/dbcache`：一张 `cache` 表 + cleaner）/ `local` | 所有"可选 Redis"的 KV 状态走这一个接口，上层只写一遍；一期用于 TokenStore 与 internal HMAC nonce 去重 |
| 鉴权 | `auth.Authenticator` 接口 + 链，provider 顺序由 `auth.providers` 启动配置；`external_jwt` / `native` 均 HS256（golang-jwt v5）| 外部 token claims `{user_id int64, role, exp}`，platform 由请求参数给出；自建 claims `{sub, pid, jti, exp}` + `TokenStore` 每平台单 token |
| 用户 id | `internal/identity`：`u___{int}`（平台用户）、`ag__{int}`（平台 agent）、`nx__{uuidv7}`（自建）。前缀固定 4 字节 | 逐字搬旧项目 `common/identity.go`。conversation_id 因此**必须用 `:` 分隔**（id 含 `_`）|
| 服务间鉴权 | HMAC-SHA256，默认时钟窗 300s、服务白名单、Cache nonce 去重；签名串唯一规则见 §6.4 | 旧项目签名未覆盖用户／平台／query，调用方必须按现行协议升级 |
| 其他 ID | group_id：服务端短 ID；server_msg_id / conn_id / token_id：UUIDv7 | 无 snowflake → 无需节点号协调 |
| 序列化 | JSON（WS 与 HTTP 一致）| 握手预留 `encoding`/`compression` 参数，后续加 gzip 不改协议 |
| 错误 | `errcode`：`Error{Code, Message, cause}`，5 位码 `K MM NN`（K=1 业务 / 2 系统，MM 模块，NN 序号），`Wrap` 用 `%w` 保留链；非 errcode 错误统一映射 `20001` | 旧项目 `response.Error` 遇非 errcode 错误返回 `code=0` 伪装成功，这里堵住 |
| 配置 | yaml + 环境变量覆盖（viper）| 每个配置项必须有引用；启动时打印生效配置（secret 脱敏）|

双实现的代价：每个仓储方法写两遍；改表要同步 pg SQL、mysql SQL、GORM model 三处。
建议实现顺序：先写 sqlc 的 `.sql` 当规格，再照着写 GORM 实现，最后跑同一套集成测试。

## 3. 目录结构

```
nexo/
├── cmd/nexo/main.go                # 子命令：serve / migrate（serve = server 包 + 自建 Hertz + 信号）
├── server/                         # 嵌入入口（§15.1）：New / Mount / Start / Shutdown / Migrate + internal 类型别名；无业务逻辑
├── sdk/                            # HTTP 客户端（§15.2）：net/http，覆盖 §9 全部接口 + internal HMAC 签名
├── internal/
│   ├── config/                     # 配置结构、加载、组合校验、生效值打印
│   ├── identity/                   # 用户 ID 编解码与校验
│   ├── ratelimit/                  # 令牌桶与按 key 分桶；api 中间件和 service/message 都用，故留在顶层
│   ├── store/
│   │   ├── store.go                # Store 接口 + 领域实体（纯 struct，无 ORM tag）
│   │   ├── gormstore/              # GORM 实现：model/（带 tag）+ user/group/conversation/message/online
│   │   ├── pgstore/
│   │   │   ├── queries/*.sql       # sqlc 输入
│   │   │   ├── gen/                # sqlc 输出（不手改）
│   │   │   └── *.go                # 适配到 Store 接口
│   │   ├── storetest/              # 一致性套件 + Mem（内存 Store，service 测试用；不被生产代码 import）
│   │   └── migrate/                # goose 应用 migrations/，取 (driver, dsn)
│   ├── auth/                       # Authenticator 接口 + external_jwt / native / chain + internal_hmac
│   ├── cache/                      # Cache 接口 + redis / pg（sqlc queries + cleaner）/ local + cachetest/
│   ├── tokenstore/                 # TokenStore：建在 Cache 上，单实现
│   ├── service/                    # user/group/message/conversation；群短 ID 在 group 内部生成
│   │   ├── conv/                   # 会话 ID、列表游标、消息可见范围
│   │   └── dto/                    # service 共用响应类型
│   ├── gateway/                    # gateway/client/usermap/frame 等，WS 网关与帧分发
│   ├── bus/                        # Bus 接口 + redis / postgres / local + bustest/
│   ├── onlinestore/                # OnlineStore 接口 + db / redis + onlinetest/
│   ├── offlinepush/                # Pusher 接口 + noop / webhook
│   ├── api/                        # Hertz 路由（public / internal 两组）、handler；只注册路由，不建 Hertz
│   │   ├── middleware/             # Trace / AccessLog / Bearer / InternalAuth / 限流
│   │   └── webx/                   # 响应封套 {code,message,data} + identity / errcode 取值；不 import 业务包
│   └── app/                        # 组装：wire 所有依赖，Build / Start / Shutdown；不含信号与监听
├── errcode/                        # 对外稳定的错误类型与错误码
├── msgbody/                        # 消息 content 的 content_type 常量、类型化解析、默认预览文案（§6.6）；只依赖 stdlib
├── migrations/                     # embed.go + postgres/*.sql（同时是 sqlc schema）+ mysql/*.sql
├── scripts/                        # test-all.sh（一次性 PG / MySQL / Redis 容器跑全量）、test-deploy.py
├── deploy/                         # docker-compose（pg-only / pg+redis / mysql+redis）、nginx.conf、smoke/（验收脚本）
├── config/config.example.yaml
├── docs/design.md, docs/integration.md, docs/embedding.md
├── Makefile, Dockerfile, sqlc.yaml, .github/workflows/ci.yml
└── AGENTS.md, CONTRIBUTING.md, README.md
```

四个 `*test/` 包（`storetest` / `bustest` / `cachetest` / `onlinetest`）是同一套路：一份用例跑该接口的全部实现，新增后端只写实现不写测试。

分层规则：`api` 和 `gateway` 只调用 `service`；`service` 调 `store` + `bus` + `onlinestore` + `offlinepush` + `tokenstore`（登录/登出）；`gateway` 订阅 `bus` 做本地推送，并向 `onlinestore` 登记/注销连接；`auth` 调 `tokenstore`，被 `api` 中间件、`gateway` 握手/心跳和 `service/user`（native 签发/吊销）调用；`tokenstore` 只依赖 `cache`；`server` 只调 `app` 和 `api`，`sdk` 不 import 本模块任何包（只依赖 `errcode` 的码值约定）。

依赖收窄：`store.Store` 是后端要实现的完整接口，不是 service 要依赖的接口。每个 consumer 只声明自己实际调用的那几个方法，接口定义在 consumer 包里（`service/group.Tx`、`service/message.Tx`、`service/conversation.Store`、`service/conv.Lister`），正好对上的直接复用 `store` 的子接口（`service/user` = `store.UserStore`，`onlinestore/db` = `store.OnlineConnStore`）。
事务视图同样收窄：`group` 和 `message` 的 `WithTx` 回调拿到的是本包的 `Tx`（没有 `WithTx`，嵌套事务变成编译错误），由包内唯一的 `Adapt(store.Store)` 桥接——`Adapt` 里那次 `store.Store` → `Tx` 的赋值就是两者一致的编译期证明，不用运行时断言。`app` 组装时调 `Adapt`，测试可以只实现这十几个方法。

## 4. 数据模型

时间列（created_at / updated_at / joined_at / send_time / heartbeat_at）PG 用 `timestamptz DEFAULT now()`、MySQL 用 `datetime(3) DEFAULT CURRENT_TIMESTAMP(3)`，业务写入时显式给值；对外 API 一律 Unix 毫秒整数。下表时间类型按 PG / MySQL 顺序列出；seq 仍是 `bigint`。JSON 字段用 `text` 存字符串（不用 `jsonb` 函数，保证两种 DB 行为一致）。
表名避开保留字：MySQL 8 的 `groups` 是保留字 → 用 `chat_groups`。
所有 `conversation_id` 列 **`varchar(256)`**：`si_` + 64 + `:` + 64 = 133 字节，128 装不下。
不用自增代理键：联合 PK 即唯一，少一个索引。

用户与群的 `extra` 写入统一上限为 **65535 UTF-8 字节**（MySQL TEXT 的容量），允许空字符串；service 在写库前校验，超出返回 `10001`，不裁剪、不截断。用户部分更新的 `extra=nil` 表示不修改，`extra=""` 表示清空。HTTP、internal 与嵌入调用遵守同一规则，不增加配置项或数据库迁移。

### users
| 列 | 类型 | 说明 |
| --- | --- | --- |
| id | varchar(64) PK | `u___{int}` / `ag__{int}`（平台，经 `/internal/user/upsert` 写入）/ `nx__{uuidv7}`（自建）|
| username | varchar(64) NOT NULL DEFAULT '' | 仅自建账号；平台用户为空串。普通索引，**不保证唯一**（注册时做一次存在性检查，仅防误操作；并发重复注册是可接受的） |
| password_hash | varchar(255) NOT NULL DEFAULT '' | 仅自建账号，bcrypt |
| nickname, avatar | varchar | |
| extra | text | 业务扩展 JSON |
| created_at, updated_at | timestamptz / datetime(3) | |

### online_conns（OnlineStore 的 DB 实现；Redis 实现时不用此表）
| 列 | 类型 | 说明 |
| --- | --- | --- |
| conn_id | varchar(64) PK | |
| user_id | varchar(64) | 索引 |
| platform_id | int | |
| node_id | varchar(64) | 索引；节点启动时 `DELETE WHERE node_id=?` 清残留 |
| heartbeat_at | timestamptz / datetime(3) | 节点每 20s 一条 `UPDATE ... WHERE node_id=?` 续期；读取时过滤 `> now-60s` |

### cache（Cache 的 PG 实现；Redis / local 实现时不用此表）
| 列 | 类型 | 说明 |
| --- | --- | --- |
| key | text PK | 统一前缀 `nexo:`，如 `nexo:tok:{user_id}:{platform_id}` |
| value | text | |
| expires_at | timestamptz NULL | NULL = 不过期；读时按 DB `now()` 过滤，不依赖节点时钟；cleaner 每分钟删 ≤1000 行过期（`FOR UPDATE SKIP LOCKED`，多节点不冲突）|
| updated_at | timestamptz | |
| 索引 | (expires_at) WHERE expires_at IS NOT NULL | cleaner 用 |

只在 `migrations/postgres` 里有；普通表，不用 momo 版本的 `UNLOGGED`（§6.3）。

### chat_groups
| 列 | 类型 |
| --- | --- |
| id | varchar(64) PK |
| name, avatar, introduction | varchar |
| owner_id | varchar(64) |
| status | int（0 正常 1 已解散）|
| extra | text |
| created_at, updated_at | timestamptz / datetime(3) |

### group_members
| 列 | 类型 | 说明 |
| --- | --- | --- |
| group_id, user_id | varchar(64) | 联合 PK |
| role | int | 1 成员 2 管理员 3 群主；当前不设置或由宿主预置管理员，不新增授予／撤销管理员入口 |
| nickname | varchar | 群内昵称 |
| inviter_user_id | varchar(64) | 邀请人，自行加入为空 |
| joined_at | timestamptz / datetime(3) | |
| 索引 | (user_id) | 查"我的群" |

退群/被踢 = 删除行（对齐 open-im）。可见边界记录在 `user_conversations`。

### conversations（会话 seq 行 = 锁对象）
| 列 | 类型 | 说明 |
| --- | --- | --- |
| conversation_id | varchar(256) PK | `si_<a>:<b>` / `sg_<group_id>` |
| type | int | 1 单聊 2 群聊 |
| group_id | varchar(64) | 群聊时 |
| max_seq | bigint | 已分配的最大 seq |
| created_at, updated_at | timestamptz / datetime(3) | |

### user_conversations（每个用户视角的会话，合并了 open-im 的 conversation + seq_user）
| 列 | 类型 | 说明 |
| --- | --- | --- |
| owner_id, conversation_id | varchar(64), varchar(256) | 联合 PK |
| type | int | |
| peer_user_id | varchar(64) | 单聊对端 |
| group_id | varchar(64) | |
| min_seq | bigint | 可见下界（入群时 = max_seq+1；单聊 = 1）|
| max_seq | bigint | 可见上界，0 = 无上界（退群/被踢时写入当时的 conversations.max_seq；若此时群还没有消息，即 max_seq 为 0，则直接删除该 user_conversations 行，避免 0 被当成无上界）|
| read_seq | bigint | 已读游标，只增不减 |
| recv_msg_opt | int | 0 正常 1 免打扰（免打扰不做离线推送）|
| is_pinned | bool | 一期只存不参与排序 |
| extra | text | |
| updated_at | timestamptz / datetime(3) | 会话列表排序键；发送等操作显式更新 |
| created_at | timestamptz / datetime(3) | |
| 索引 | **(owner_id, updated_at DESC, conversation_id)**、(conversation_id) | 游标分页 / 群消息批量更新 |

### messages
| 列 | 类型 | 说明 |
| --- | --- | --- |
| conversation_id, seq | varchar(256), bigint | **联合 PK**，与 open-im 一致，按会话顺序读；也是 last_message 批量点查的键 |
| server_msg_id | varchar(64) UNIQUE | UUIDv7 |
| client_msg_id | varchar(64) | 客户端幂等 ID，1–64 字节，不得以 Unicode 空白结尾（§5.6）|
| sender_id | varchar(64) | |
| recv_id | varchar(64) | 单聊接收方 |
| group_id | varchar(64) | |
| session_type | int | 1 单聊 2 群聊 |
| content_type | int | 1 文本 2 图片 3 视频 4 音频 5 文件 100 自定义 |
| content | text | JSON 字符串，按 content_type 解释；默认上限 8KB。推荐 wire 形状与解析后 Body 字段见 §6.6 |
| send_time | timestamptz / datetime(3) | 服务端时间 |
| created_at | timestamptz / datetime(3) | |
| 唯一索引 | **(conversation_id, sender_id, client_msg_id)** | 幂等键限定在会话内；跨会话共享会把发给别人的消息当幂等结果返回 |

## 5. 核心约定

1. **conversation_id**：单聊 `si_` + 两个 user_id 字典序小者 + `:` + 大者；群聊 `sg_` + group_id。分隔符**只能是 `:`**（user_id 含 `_`）。生成后不可变。
2. **seq**：每个会话独立、从 1 起、连续、单调。由 `conversations.max_seq` 在事务内 `FOR UPDATE` 后 +1 分配。
3. **可见区间**：用户 U 在会话 C 能看到的 seq 范围 =
   `[ max(begin, uc.min_seq), min(end, conv.max_seq, uc.max_seq>0 ? uc.max_seq : ∞) ]`。
   拉取、推送、last_message 都强制这个边界。入群不可看历史、退群不可看新消息都靠它。拉取和 MarkRead 通过 Store 的单条 JOIN 同时读取 `user_conversations` 和 `conversations.max_seq`，按同一语句快照计算可见边界。拉取再读取该范围的消息；不能混用退群前的权限与退群后的会话上界。普通 READ COMMITTED 事务中的两次 SELECT 不提供这个保证。
4. **未读数** = `visible_max_seq - read_seq`，其中 `visible_max_seq = uc.max_seq>0 ? min(uc.max_seq, conv.max_seq) : conv.max_seq`。
5. **read_seq 单调**：`UPDATE ... SET read_seq = GREATEST(read_seq, ?)`（MySQL/PG 都有 GREATEST）。MarkRead 的写入目标、返回值和广播使用同一快照算出的目标；目标不大于已有 read_seq 时保持原有无更新／不广播语义，不回退历史已读值。
6. **幂等**：唯一索引 `(conversation_id, sender_id, client_msg_id)`。`client_msg_id` 长度为 1–64 字节，末尾不得是 Unicode 空白（按 Go `unicode.IsSpace`，包括空格、制表符、换行及全角空格）；service 在幂等查询前校验，违规返回 `10001`，HTTP / internal / WS / 嵌入调用一致。只拒绝，不裁剪或归一化；前导和内部空白不受此规则限制。保留 MySQL 当前排序规则，避免 PAD SPACE 将合法的新 ID 与尾空白变体视为同一消息。本规则不迁移历史数据；如已有尾空白 ID，须在上线前另行处理，否则合法短 ID 仍可能命中旧记录。先查快路径；事务内在持有行锁后 `INSERT ... ON CONFLICT DO NOTHING`（gormstore 用 `clause.OnConflict{DoNothing: true}`，MySQL 实际生成自赋值的 `ON DUPLICATE KEY UPDATE`；所有 MySQL 连接必须禁用 `CLIENT_FOUND_ROWS`），**影响行数 0 → 回滚**（`max_seq` 尚未更新，seq 不浪费）→ 回查已存在消息 → 返回同一 ACK。并发双发必须得到相同 ACK，不能返错。
7. **推送 at-most-once**：服务端不重试推送。客户端三条规则兜底：
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

PG LISTEN/NOTIFY 的已知性质（已评估，方案按 at-most-once 设计）：
- 不持久化、不重放，只投递给当时在 LISTEN 的会话；监听连接断开到重新 LISTEN 之间的通知全丢；
- 事务内 NOTIFY 在提交时发出，回滚不发；同一事务内相同 channel+payload 合并为一条（我们 payload 含 conv_id+seq，不会撞）；
- 通知队列满（默认 8GB，仅监听会话长期卡在事务里才会满）时发送方提交报错，不是静默丢；监听连接独占且不开事务，不会触发；
- **部署限制：必须直连 PG 主库或 PgBouncer session 模式；transaction 模式不支持 LISTEN。**
- 已评估并放弃 outbox 表 + NOTIFY 门铃的 at-least-once 方案（每事件多一次 insert + 清理），一期不做。

**Bus 重连后的 Resync（对丢失窗口的兜底）**：任何实现的 `onConnected` 触发时（首次连接除外），节点向本地所有 WS 连接推一帧 `2004 Resync`，客户端立即 `GetMaxSeqs` 补齐。丢失窗口 = 断线时长，恢复不用等下一次触发。

**payload 大小规则（所有实现统一）**：序列化后 > 7500 字节 → 改发引用形式 `{type:"push", ref:true, conversation_id, seq}`，接收节点按 `(conversation_id, seq)` 回读 DB。一期 content ≤ 8KB，只有大 content 的消息走引用。

**消费端并发（网关侧）**：`Subscribe` 的回调跑在单个 goroutine 上，而 push 事件需要回读发送范围与可见性（引用形式还要回读消息本身），一次慢查询会把同一节点上的 kick / conv_read / group_changed 一起堵住。因此 push 事件不在回调里处理：按 `conversation_id` 哈希投递到 `ws.deliver_workers` 个分片队列（每片 `ws.deliver_queue`），由各自的 worker 解析并扇出。同一会话始终落在同一分片，会话内顺序不变；分片队列满时丢弃该事件并计入 `push_dropped`，符合 6.1 的 at-most-once 约定（客户端下次 pull 或 Resync 补齐）。其余三类事件仍在回调里同步处理，它们只操作内存连接表；即使出站队列满，socket 关闭与本地注销也不等待全局在线登记清理。Redis 空闲订阅取消时主动关闭对应 PubSub，打断阻塞读取，不依赖下一条事件或整个 Redis Client 关闭。

**事件定义**：

| type | payload | 接收节点动作 |
| --- | --- | --- |
| `push` | `{conversation_id, seq, session_type, sender_id, sender_conn_id, recv_id?, group_id?, msg?}` | 解析目标用户 → 查本地 UserMap → 投递到连接 send 队列。排除 `sender_conn_id`（发送方当前连接通过请求响应获取 ACK，不重复推送；HTTP/internal 发送时为空 → sender 所有连接都收）|
| `kick` | `{user_id, platform_id, keep_token_id}` | 关闭本地该 user+platform 且 `token_id != keep_token_id` 的连接，先发 `2002 KickOnline` 帧 |
| `group_changed` | `{group_id}` | 失效本地群成员缓存 |
| `conv_read` | `{user_id, conversation_id, read_seq}` | 推给该用户其他在线端（`2003 ConvRead`）|

**目标用户解析在接收端做**（而不是发送节点把 user_id 列表塞进事件）：
- 单聊：`[sender_id, recv_id]`，直接来自事件；
- 群聊：`GroupMemberCache.Members(group_id)`（每节点本地 LRU，**TTL 10s**，`group_changed` 事件主动失效）**只用来筛候选人**，不作为授权依据；候选人与本地 UserMap 取交集后，投递前再批量查一次 DB：`SELECT owner_id FROM user_conversations WHERE conversation_id=? AND owner_id IN (...) AND min_seq <= seq AND (max_seq = 0 OR seq <= max_seq)`，只向查得到的用户投递。这样 `group_changed` 丢失或 TTL 内的退群用户不会收到消息正文；代价是每条群消息每节点一次主键范围点查（IN 列表 = 本节点在线成员，通常远小于 500）。单聊同样过一次这个查询（2 行），路径统一。

**发送节点不做本地直推**：落库后只 `Publish`，自己也作为订阅者收到事件后按同一路径推本地连接。一条代码路径，代价是本机推送多一次 Redis/PG 往返（~1ms）。

### 6.2 Authenticator —— 鉴权（provider 顺序启动时配置）

类型以 [`internal/auth/auth.go`](../internal/auth/auth.go) 为准：`Authenticator.Verify(ctx, token)` 返回 `Identity`，字段为 `UserId`、`Role`、`PlatformId`、`TokenId`、`ExpiresAt`（Unix 毫秒）、`Source`。自建 token 的 TokenId 为 jti，外部 token 为 SHA-256 前 16 字节的 hex；外部身份的 PlatformId 由请求参数补充。公开别名为 `server.Identity`、`server.Authenticator`。

| provider | 机制 |
| --- | --- |
| `external_jwt` | HS256，`secrets: []`（轮换时新旧并存，逐个试）；claims `{user_id int64, role?, exp}`；`role` 缺省取配置 `default_role`；`identity.FromActor(role, id)` → `u___{id}` / `ag__{id}`。**不查 TokenStore**：吊销归平台 |
| `native` | HS256 自签；claims `{sub=user_id, pid, jti, exp}`；签名和 exp 通过后再 `TokenStore.Check(sub, pid, jti)`，不是该平台当前 token → 401。配合 `/auth/register` `/auth/login` `/auth/logout` |
| `chain` | 按 `auth.providers` 顺序逐个试，第一个通过者胜出；全部失败返回最后一个错误。未列出的 provider 不加载，对应路由也不注册 |

部署形态由 `auth.providers` 在**启动时**决定：`[external_jwt]`（随平台部署，生产主路径）、`[native]`（独立部署，没有外部平台）、`[external_jwt, native]`（过渡 / 测试）。

**platform_id**：自建 token 带 `pid`；外部 token 不带 → WS 握手 `platform_id` 必填，HTTP 取 `X-Platform-Id` 头，缺省 `auth.default_platform_id`。客户端自报，伪造只影响"自己哪条连接被踢"。

**校验时机**：HTTP 鉴权中间件、WS 握手做完整 `Verify`（native 含一次 `TokenStore.Check`）；WS 连接上每帧比较 `ExpiresAt`（本地零成本）；每 60s 定时对 native 连接再做一次 `TokenStore.Check`，即使 `kick` 事件丢失，不同旧 token 也会在后续检查时失效；单次查询另有 5s context 预算，关闭还需队列排空，不承诺严格 1 分钟上限。`TokenStore` 不可用时前两次跳过，连续第三次失败则踢；成功检查会重置失败计数，握手仍 fail-closed。过期/失效 → `2002 KickOnline{reason:"token_expired"}` 后关闭。

### 6.3 Cache 与 TokenStore —— Redis 的 PG 替身

所有"可选 Redis"的 KV 状态走一个接口、三个后端，上层只写一遍。完整接口见 [`internal/cache/cache.go`](../internal/cache/cache.go)：`Get`、`MGet`、`Set`、`SetNX`、`Del`、`DelIfValue`、`IncrBy`、`Expire`、`Close`；TTL ≤ 0 表示不过期。

| 实现 | 机制 |
| --- | --- |
| `redis` | 直接映射 GET / MGET / SET PX / DEL / INCRBY+PEXPIRE / PEXPIRE |
| `pg` | 移植 momo_server `internal/infrastructure/dbcache`（sqlc + pgx，~600 行含测试）：`cache(key, value, expires_at)` 表；读时 `expires_at IS NULL OR expires_at > now()`；`IncrBy` 用 `INSERT ... ON CONFLICT DO UPDATE` 单语句原子（过期键视为不存在重新计数）；后台 cleaner 每分钟 `DELETE ... LIMIT 1000 FOR UPDATE SKIP LOCKED`，多节点同时跑不冲突 |
| `local` | 进程内 map + TTL；仅单机 / 测试。多节点启用 native 或 internal 通道时必须用 redis / pg，否则 token 与 HMAC nonce 只在本节点有效（随机 401、跨节点重放） |

- `pg` 只支持 PostgreSQL（`unnest` / `SKIP LOCKED` / `ON CONFLICT`）；MySQL 部署本来就必须有 Redis（无 LISTEN/NOTIFY），组合表不变。
- `pg` cache 始终另开一个小 pgxpool（≤4 连接，启动时 Ping），与 `db.access` 无关；宿主注入 DB 连接时也一样，所以仍需 `db.dsn`（§15.1 规则 3）。
- 与 momo 版本的两处差异：表不用 `UNLOGGED`（token 读多写少，WAL 开销可忽略；UNLOGGED 在 PG 崩溃恢复时清空 = 全体重新登录）；key 统一前缀 `nexo:`。

**TokenStore**（自建 token 的每平台单 token）建在 Cache 之上，**只有一个实现**：

[`internal/tokenstore.TokenStore`](../internal/tokenstore/tokenstore.go) 是具体结构体，提供 `Set`、`Check`、`Delete`，均携带 userId、platformId 与 tokenId；Set 的 TTL 为 token 剩余有效期。键为 `nexo:tok:{user_id}:{platform_id}`，值为 token_id，覆盖旧值即使同平台旧 token 失效。

**Logout 只注销请求 token**：将已验证 Identity.TokenId 传给 Delete，通过 `Cache.DelIfValue(ctx, key, expected)` 原子比较并删除；键不存在、已过期或当前值不匹配时为成功无操作。local 在同一互斥锁内比较／删除，Redis 用单次 Lua，PG 用单条条件 DELETE，不得退回 Get 后 Del。A 已通过鉴权后若 B 在同平台登录，A 的迟到 Logout 返回成功但不得删除 B。若 A 在请求鉴权前已被替换，则仍返回 401。嵌入调用须传入非空 TokenId；缺失返回参数错误。SDK 的并发本地 token 写入规则不因此改变，仍见 §15.2。

外部 token 不进 TokenStore。native Bearer 验证或 WS 握手读取 TokenStore 失败时 fail-closed，返回 **HTTP 503 + `20101 AuthUnavailable`**，不是 401；external 验证不依赖 TokenStore。Login 写 token、Logout 在 Bearer 验证成功后删除 token 失败，返回 **HTTP 500 + `20001`**；若 Logout 先在 Bearer 读取阶段失败，则仍是 `503/20101`。这些依赖故障不能证明凭据失效，客户端不应据此清掉本地 token。已建立的 native WS 沿用 §6.2 的三次连续失败关闭策略。
一期 Cache 用于 TokenStore 与 internal HMAC nonce 去重。`IncrBy` 当前不用于 seq 分配；后续设想见 §13。

### 6.4 Internal 通道 —— 平台后端代用户操作

协议沿用旧项目 `internal/middleware/internal_auth.go` 的头名和 HMAC 形式，但签名串**扩展**（破坏性变更，平台侧签名代码要同步改）：旧签名不覆盖 `X-User-Id`、`X-Platform-Id` 和 query，持有任意合法签名的请求可改头冒充任意用户发消息或踢人，这不是重放而是篡改。

```
Headers:  X-Service-Name   调用方服务名（必须在 allowed_services 内）
          X-Timestamp      Unix 秒；|now - ts| ≤ max_skew_seconds(300)
          X-Nonce          随机 ≥16 字节 hex/base64，每请求唯一
          X-User-Id        代操作的用户，IM 格式（u___123）；InternalAuthAsUser 路由必填，其余路由为空串
          X-Platform-Id    缺省 5；参与签名时用请求实际携带的值（缺省则空串）
          X-Signature      hex(HMAC-SHA256(secret,
                             service + "\n" + ts + "\n" + nonce + "\n" + METHOD + "\n"
                             + rawPath + "\n" + rawQuery + "\n" + userID + "\n" + platformID + "\n"
                             + hex(sha256(body))))
```

- `rawPath` / `rawQuery` 取实际 HTTP request-target 在首个 `?` 两侧的原始值；路径包含挂载前缀与百分号转义，query 不解码、不重排、不含 `?`，无 query 时为空串。正文摘要基于实际发送的原始字节，不能验签前重排 JSON；
- 验签顺序：service 在名单 → 时间窗 → 常量时间比较签名 → nonce 去重；nonce 用 `Cache.SetNX("nexo:inonce:"+service+":"+nonce, ttl=max_skew_seconds×2)`。服务／时间／签名／nonce 重放拒绝为 `401/10002`；nonce Cache 不可用仍 fail-closed，但返回 **HTTP 500 + `20002 StoreFailed`**，不映射为 401 或 native 的 `503/20101`；
- 部署要求：internal 通道**只在 TLS 或仅内网可达的监听上暴露**；配置 `internal_auth.require_tls=true` 时，明文 HTTP 且无 `X-Forwarded-Proto: https` 的请求直接 403。该头只在对端落在 `server.trusted_proxies` 内时才被采信——否则任何客户端加一个头就能绕过 TLS 要求；`trusted_proxies` 为空时只认真实的 https 连接。

两个中间件：`InternalAuth`（只验签）、`InternalAuthAsUser`（验签 + 从头注入 `Identity{UserId, PlatformId, Source:"internal"}`）。路由见 §9。

### 6.5 OnlineStore —— 全局在线连接登记

完整接口见 [`internal/onlinestore/onlinestore.go`](../internal/onlinestore/onlinestore.go)：`ConnRef{UserId, PlatformId, ConnId}`；`Add` / `Remove` 均传 nodeId 与 ConnRef，`Renew` 传 nodeId 与连接快照，`Online` 返回 userId → 平台列表，`PurgeNode` 清理指定节点残留。

| 实现 | 机制 |
| --- | --- |
| `db` | `online_conns` 表；`Renew` = 一条 `UPDATE online_conns SET heartbeat_at=? WHERE node_id=?`；`Online` = `SELECT DISTINCT user_id, platform_id WHERE user_id IN (...) AND heartbeat_at > ?` |
| `redis` | 每用户一个 ZSET `nexo:online:{user_id}`，member = `platform:node:conn_id`，score = 过期时间戳；`Renew` = pipeline ZADD，补回仍存活但登记已过期/被清扫的连接，并清理过期成员；`Online` 只读，用 pipeline `ZRANGEBYSCORE` 按有效期过滤并解析 platform |

网关有序执行 Add、快照生成与 Renew、Remove；迟到 Add 必须检查连接仍存活且未进入 draining，旧快照不能在 Remove 之后重新登记。首次 Add 失败保持 WS 可用，由下一次续期在同一 presence 锁下补登记；先批量 Renew 存活快照，再重试失败的 Add，避免单个补登记失败阻断其他连接的心跳。成功后不重复 Add，不改 DB Renew 合同。登记操作不持有 UserMap 锁访问网络。socket 关闭与本地注销同步完成，Remove 转入受限、被退出流程跟踪的后台任务（每节点最多 64 个，含等待 presence 锁的任务）；每项最多 5 秒并受 §10 统一退出期限约束。任务满或清理失败时不阻塞事件回调，残留登记由 TTL 或节点 Purge 清理。

两个消费者：离线推送判定（§6.6）和在线状态查询接口（§9 `/user/online_status`）。**不参与推送路由、不参与踢人**。节点宕机后脏数据最多存活 60s。
不建在 Cache 之上：`Online(userIDs)` 要按用户枚举连接，纯 KV 没有前缀枚举（Redis SCAN 是全键空间），Redis 用 ZSET、PG 用 `online_conns` 表各自最合适。
旧项目的在线状态只读本地 map，多机下把别的节点上的用户报成离线；这里是全局的。

### 6.6 Pusher —— 离线推送接口（上层实现 APNs/FCM）

`Notification` 只带事实，不带文案。谁是发送者、群叫什么，是宿主自己系统的事（用户名可能来自外部平台），用 `SenderId` / `GroupId` 去查；正文由 push 侧决定是否解析、怎么解析。

```go
// internal/offlinepush
type Notification struct {
    ConversationId string
    Seq            int64
    SessionType    int32
    SenderId       string
    GroupId        string // 群聊才有
    ContentType    int32
    Content        string // 原始 JSON（≤ 8KB），按 content_type 解释
    SendTime       int64  // unix ms
}
func (n Notification) EventId() string // conversation_id + ":" + seq
func (n Notification) Preview() string // = msgbody.Parse(...).Preview()，便利方法

type Pusher interface {
    Push(ctx context.Context, userIds []string, n Notification) error
}
```

解析在公开包 **`msgbody/`**（与 `errcode/` 同级，只依赖标准库，使用 `encoding/json/v2` 和 `encoding/json/jsontext`）。`msgbody.Parse(contentType, raw)` 返回 `Body{Type, Text, Url, Name, Custom}`：Text 存文本，Url 存媒体地址，Name 存文件名，Custom 为原始 `jsontext.Value`。推荐 wire 对象键为 `text`、`image`、`video`、`audio`、`file`（文件可另带 `name`），不是按 Body 字段序列化；例如发送文本时外层字段为 `"content":"{\"text\":\"hello\"}"`。`Body.Preview()` 默认文本取前 50 字、媒体返回占位文案、Custom 返回空串。解析器不等于发送接口的字段验证器：发送对所有已知 content_type 接受任意合法 JSON 值（包括对象、数组、标量及 null），仅验证 JSON 合法性与大小，不强制上述对象形状或按类型检查字段。推荐对象供类型化解析与预览使用；存储成功不保证 msgbody.Parse 成功或生成非空预览。

| 实现 | 说明 |
| --- | --- |
| `noop` | 默认；只打 debug 日志 |
| `webhook` | `POST {url}` JSON `{event_id, user_ids, notification, preview}`，超时 3s，不重试；`preview` 是服务端顺手算的 `Preview()`，接收方可用可弃，其他语言直接读 `notification.content`。上层在自己的服务里对接 APNs/FCM/厂商通道。`url` 必须为 `https://`；**不跟随任何重定向**（含同主机 HTTPS），3xx 作为本次推送失败且不重试，接收方须配置最终地址；请求头 `X-Nexo-Timestamp`（Unix 秒）、`X-Nexo-Signature` = `hex(HMAC-SHA256(webhook_secret, ts + "\n" + hex(sha256(body))))`；`event_id = conversation_id + ":" + seq` 作幂等键，接收方按它去重（同一消息重试或多节点误触发时不会重复推） |
| 自定义 | 组装时注入：`server.New(ctx, cfg, server.WithOfflinePusher(myPusher))` |

**触发规则**：只在发送节点、事务提交并 `Publish` 之后、异步执行**一次**（不在接收事件的节点做，否则 N 个节点重复推）。

```
targets := 单聊 [recv_id] / 群聊 members(group_id) - sender
targets  = 过滤 user_conversations.recv_msg_opt != 0（免打扰）
online  := keys(OnlineStore.Online(targets))
offline := targets - online
if len(offline) > 0 { Pusher.Push(ctx, offline, n) }   // 失败只记日志
```

## 7. WebSocket 网关

### 7.1 握手
`GET /ws?token=<jwt>&platform_id=<int>&encoding=json&compression=none`（也接受 `Authorization: Bearer` 头）。`platform_id` 必填，1–10。

一期只接受 `encoding=json`、`compression=none`，其他值返回 400。参数先占位，后续加 `gzip`/`protobuf` 只需增加实现。

握手步骤：`Authenticator.Verify` → Upgrade → 生成 conn_id → 注册到 UserMap → `OnlineStore.Add` → 广播 `kick{user_id, platform_id, keep_token_id=Identity.TokenId}` → 进入读循环（native 连接每 60s `TokenStore.Check`）。
连接关闭：UserMap 移除 → `OnlineStore.Remove`。

### 7.2 帧格式（JSON 文本帧）

```jsonc
// 请求
{"req_id": 1003, "op_id": "uuid", "msg_incr": "c-17", "data": { ... }}
// 响应（同 op_id / msg_incr 回带）
{"req_id": 1003, "op_id": "uuid", "msg_incr": "c-17", "code": 0, "data": { ... }}
// 服务端主动推送（无 msg_incr，op_id 服务端生成）
{"req_id": 2001, "op_id": "uuid", "data": { ...message... }}
```

`data` 是嵌套 JSON 对象，**不是** base64 字符串（旧项目 `Data []byte` 会被 encoding/json 编成 base64，JS 端要多解一层；这里是有意的破坏性变更）。上下行都是文本帧。响应 `message` 为空时省略，客户端应将它视为可选。`code` 用 `errcode` 的完整错误码，不压平成 1。

| req_id | 名称 | 方向 | data |
| --- | --- | --- | --- |
| 1001 | GetMaxSeqs | C→S | `{cursor?, limit≤200}` → `{items:[{conversation_id, max_seq, min_seq, read_seq}], next_cursor, has_more}`（已应用可见边界；游标与 §8.8 同形，按 updated_at 倒序）|
| 1002 | PullMsgBySeqRange | C→S | `{conversation_id, begin_seq, end_seq, limit≤100}` → `{messages[], has_more}` |
| 1003 | SendMsg | C→S | `{client_msg_id, session_type, recv_id \| group_id, content_type, content, sender_read=true}` → ACK `{server_msg_id, conversation_id, seq, send_time}` |
| 1004 | MarkRead | C→S | `{conversation_id, read_seq}` → `{read_seq}` |
| 2001 | PushMsg | S→C | 完整消息（含 conversation_id, seq）|
| 2002 | KickOnline | S→C | `{reason: "new_login" \| "token_expired"}`，随后服务端关闭连接；客户端不得自动重连 |
| 2003 | ConvRead | S→C | `{conversation_id, read_seq}` 多端已读同步 |
| 2004 | Resync | S→C | `{reason}`；客户端收到后立即 1001 + 按需 1002。Bus 重连后发 |

心跳默认每 30s 发 ping，`ws.pong_wait` 默认 75s：收到 pong 或进入下一次消息读取时续读期限。只有在期限内未续期且读取失败时才按读超时关闭；持续收发／pong 的连接不会因为存活满 75s 被回收。

### 7.3 连接模型（对齐 open-im client.go 与旧项目 client_conn.go 的底线）

- 每连接：`readLoop` 一个协程、`writeLoop` 一个协程、`send chan []byte` 缓冲 256（**来自配置 `ws.send_queue`，不是常量**）；
- `ClientConn` 接口隔离 gorilla（旧项目 `client_conn.go:12-18`，逐字搬），推送逻辑可用 mock 连接单测；
- 所有出站（ACK、push、kick、resync）都进 `send` 队列，**单写协程**，避免并发写；
- 写超时 10s；队列满（慢消费者）→ 计数 + 直接关连接，客户端重连后按 seq 拉取。**不静默丢帧**；
- 进入 draining 后停止准入新帧，readLoop 不抢先关闭 socket，由 writer 排空已有帧后关闭；整个排空过程最多 10s（含最后控制帧），节点退出期限更早时以该期限为准。已准入请求保留原有语义：可在关闭前完成，实际关闭取消连接 context；不承诺撤销已提交事务，也不保证每个在途请求都收到 ACK，未确认发送仍用原 client_msg_id 重试；
- `readLoop` 内 panic recover；单帧上限 64KB；业务 handler 出错只回错误帧，不断连；
- `UserMap`：`map[userID][]*Client`，读写锁，`GetAll` 返回切片副本；注册/注销走同一个有序通道，避免旧项目 register/unregister 乱序导致的幽灵连接；
- 网关另起一个协程每 20s 把 UserMap 全量快照调用 `OnlineStore.Renew`；
- **连接与速率限制**（配置见 §11 `limits`，任一超限：握手阶段返回 429，运行中返回 `10005 TooManyRequests` 错误帧，连续超限 3 次关连接）：
  - 每用户 `ws_conns_per_user`（默认 10）、每 token_id `ws_conns_per_token`（默认 3）、每来源 IP `ws_conns_per_ip`（默认 50，取 `X-Forwarded-For` 首项，需 nginx 可信）、本节点 `ws_conns_total`（默认 20000）；
  - 每连接入站 `ws_frames_per_sec`（默认 20，令牌桶，桶容量 40），在 JSON 解码前扣配额，畸形帧也计入；超限帧不解析，错误帧 `req_id=0` 且不回显 `op_id` / `msg_incr`，连续超限 3 次关闭连接。每连接同时在处理的请求 `ws_inflight_per_conn`（默认 8，超出直接回限流错误、不排队）；
  - 每用户发消息 `message_send_per_min`（默认 120，跨连接合计；HTTP/WS 共用，internal 通道不限）；
  - 本节点 send 队列总字节 `ws_send_bytes_total`（默认 512MiB）：入队前累计，超限则丢弃本次推送并对该连接发 2004 Resync，客户端按 seq 补齐。只作用于推送帧（如 2001 / 2003）；ACK、错误帧、2002 Kick、2004 本身不受此上限。上限为 0 只关闭限制，仍记账；写出、入队失败或关闭均恰好释放已累计字节。
  - 自建账号的 `/auth/register`、`/auth/login` 每来源 IP `auth_per_ip_per_min`（默认 0 = 关，超限 429）；生产通常由 nginx `limit_req` 承担。
  限流计数全部为节点本地内存（不走 Cache），多节点下上限按节点数放大，一期接受。
  按 key 的令牌桶只跟踪有界数量（登录 IP 10000、消息发送者 100000）；满表时未跟踪 key 直接共用 overflow 桶，不额外扫描。空闲超过 5 分钟的 entry 由 Allow 中原有的到期检查回收（距上次 sweep 超过 5 分钟时触发），overflow 请求不重置这个周期；容量、现有 key 独立桶和 overflow 配额不变。
- **握手 Origin 校验**：浏览器不对 WebSocket 施加同源策略，任何页面都能带着浏览器里的凭证发起握手，因此 `ws.allowed_origins` 是唯一的防线。留空 = 不校验（默认），只在 token 从不放进 cookie 时才安全；非浏览器客户端不发 `Origin`，恒放行。
- **握手失败的状态码**：`10601` 超连接数上限 → 429（退避后重试），`10604` 节点正在优雅下线 → 503（立即重连，LB 会换一个节点），两者用不同 errcode 而不是靠 message 文本区分。

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
2. 快路径幂等：SELECT messages WHERE conversation_id=? AND sender_id=? AND client_msg_id=? → 命中直接返 ACK
3. Store.WithTx:
   a. INSERT conversations(conversation_id, type=1, max_seq=0) ON CONFLICT DO NOTHING
   b. SELECT ... FROM conversations WHERE conversation_id=? FOR UPDATE     ← 串行化点
   c. seq = max_seq + 1
   d. INSERT messages(conversation_id, seq, ...) ON CONFLICT DO NOTHING
        ← 影响行数 0（并发双发）→ 回滚 → 回查 → 返回同一 ACK，seq 未消耗
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

崩溃点分析：事务提交前崩溃，无 ACK，客户端用原 client_msg_id 重试；提交后、Publish 前崩溃，消息已落库但无推送且尚无 ACK，客户端靠拉取补齐，重试返回原 ACK。Publish 后也可能在 ACK 到达前崩溃，不能把未收到 ACK 当成未落库。步骤 7 失败只少一次 app push，无数据影响。5s 是协作式 context 预算，不会强制打断不遵守 context 的宿主 Publisher；实时推送与离线推送的完成都不保证排在 ACK 前或后。

### 8.4 发送群聊消息
与 8.3 相同，差异：
- 步骤 1 只做参数校验和 `chat_groups` 存在性预检（快速失败）；**成员资格与群状态的最终校验放在步骤 3b 拿到 `conversations` 行锁之后**，同一事务内 `SELECT 1 FROM group_members WHERE group_id=? AND user_id=?` 且 `chat_groups.status` 未解散，不满足 → 回滚返回 403。因为退群/踢人/解散事务（§8.5）都先锁同一行，"校验通过后被踢仍能发出"的窗口被关闭；
- 步骤 f 改为一条语句：`UPDATE user_conversations SET updated_at=now WHERE conversation_id=? AND max_seq=0`，再按 `sender_read` 单独 upsert sender 的 read_seq；
- 事件带 `group_id` 不带成员列表；接收节点用 `GroupMemberCache` 解析；
- 步骤 7 的 targets 为成员列表减去 sender，并过滤免打扰。

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
3. UPDATE user_conversations SET max_seq = conversations.max_seq   (可见上界)
4. 提交 → Publish group_changed{gid}
```
因为入群、退群、发消息都锁同一行，"入群前的消息不可见 / 退群后的消息不可见"在并发下也严格成立。重新入群时 min_seq 重算、max_seq 归零。
建群时初始成员 `min_seq = 1`（能看全部历史），批量 INSERT，不逐条。

### 8.6 客户端同步
客户端**持久化每会话一个 `local_max`**（同步基线，一个整数，不是消息库；与 A13 不冲突），首次为 0。

```
连接建立 / 收到 2004 Resync / app push 唤起
         → 1001 GetMaxSeqs（分页：{cursor, limit≤200} → {items[], next_cursor}，按 updated_at 倒序，客户端翻到空为止）
         → 对每个会话:
              begin = max(local_max + 1, min_seq)              ← 重入群后 min_seq 重算，基线跟着抬到 min_seq-1，不再拉不可见区间
              if begin > server_max: local_max = server_max; 跳过   ← 覆盖 local_max > server_max 的情况（重入群、服务端重置）
              else 1002 拉 [begin, server_max]，每批 ≤100，拉完 local_max = server_max
运行中   → 收到 2001 push: seq == local_max+1 → 直接用；seq > local_max+1 → 先拉缺口；seq ≤ local_max → 丢弃(重复)
被踢     → 收到 2002 → 不重连，回到登录
```

### 8.7 已读
`MarkRead(conv, read_seq)` → 校验归属 → `read_seq = GREATEST(read_seq, min(?, visible_max))` → Publish `conv_read` → 自己其他端收到 2003。一期不通知对端（已读回执二期）。

### 8.8 会话列表（服务端排序 + 服务端返回 last_message）

```
GET /conversation/list?cursor=&limit=20&with_last_message=true
1. SELECT uc.*, c.max_seq FROM user_conversations uc JOIN conversations c USING(conversation_id)
   WHERE owner_id=? AND (updated_at, conversation_id) < (cursor_updated_at, cursor_conv_id)
   ORDER BY updated_at DESC, conversation_id DESC LIMIT limit+1          ← 走索引 (owner_id, updated_at DESC, conversation_id)
2. 每行算 visible_max、unread（§5.3/5.4）
3. with_last_message：SELECT * FROM messages WHERE (conversation_id, seq) IN ((c1, v1), (c2, v2), …)
   ← 主键点查，一页最多 100 组；visible_max < min_seq 的会话无 last_message。MySQL 8 与 PG 都支持行构造器 IN
4. 返回 {conversations:[{…, unread, read_seq, max_seq, last_message?}], next_cursor, has_more}
   cursor = 无填充 base64url("<updated_at 的 Unix 毫秒>:<conversation_id>")
```
排序只按 `updated_at`，`is_pinned` 一期只存不排（旧项目同样），客户端在已拿到的列表内前移。

### 8.9 离线推送（发送节点视角）

```
提交事务 → 同步 Publish（5s context 预算）→ 启动离线任务 → 返回 ACK
                                            └─ 异步：targets - 免打扰 → OnlineStore.Online → offline
                                                     Pusher.Push(offline, Notification{...})
                                                     // 默认超时 3s，失败记日志，不影响 ACK
```

## 9. HTTP API

响应统一 `{"code":0,"message":"","data":{}}`，错误码在 `errcode`。成功及一般业务错误为 HTTP 200，但鉴权拒绝为 401、权限拒绝为 403、限流 `10005` 为 429；系统错误默认 500，`20101 AuthUnavailable` 为 503，超时码为 504。非 errcode 错误映射为 `500/20001`。WS 握手另有参数错误 400、连接配额 429、节点 draining 503（§7.3）。认证读／写与 nonce 依赖故障分别按 §6.3／§6.4 映射，不统一为 401。
错误码 5 位 `K MM NN`：`K`=1 业务（客户端处理，不告警）/ 2 系统（DB / bus / 超时 / bug，error 级日志 + 告警，客户端只看到通用文案）；`MM` 模块（00 通用 01 鉴权 02 用户 03 群 04 消息 05 会话 06 WS）；`NN` 序号。分类编在码里，日志 / metrics 用正则过滤：系统错 `^2\d{4}$`，某模块 `^\d04\d{2}$`。一个码的类别和模块不可变更。

### 9.1 公开接口（前缀 `/api/v1`，除 register／login 外都需 `Authorization: Bearer <platform 或自建 token>`，可带 `X-Platform-Id`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | /auth/register | 自建账号：username/password/nickname（`auth.providers` 含 `native` 时才注册路由）|
| POST | /auth/login | 自建账号：+ platform_id → token |
| POST | /auth/logout | 自建账号：仅注销请求 token，不删除并发新登录的 token |
| GET | /user/me | |
| PUT | /user/me | nickname/avatar/extra |
| GET | /user/info?user_ids=a,b | 批量 |
| GET | /user/online_status?user_ids=a,b | `{items:[{user_id, online, platform_ids}]}`，来自 OnlineStore |
| POST | /group/create | name, member_ids |
| POST | /group/join | group_id（一期无审批）|
| POST | /group/quit | |
| POST | /group/kick | 管理员/群主 |
| GET | /group/info?group_id= | 仅成员可查 |
| GET | /group/members?group_id= | 仅成员可查 |
| POST | /message/send | 同 WS 1003（含 `sender_read`）|
| GET | /message/pull | 同 WS 1002 |
| GET | /message/max_seqs | 同 WS 1001 |
| GET | /conversation/list | §8.8：cursor / limit≤100 / with_last_message |
| POST | /conversation/read | 同 WS 1004 |
| PUT | /conversation/opt | recv_msg_opt / is_pinned |

LB 探活是独立的 `GET /healthz`（有挂载前缀时为 `<prefix>/healthz`），不在 `/api/v1` 下、不需 Bearer。响应为顶层 `status`／`node_id`，不使用业务 envelope。

### 9.2 Internal 接口（前缀 `/api/v1/internal`，HMAC 签名，§6.4）

| 方法 | 路径 | 中间件 | 说明 |
| --- | --- | --- | --- |
| GET | /health | InternalAuth | `{"code":0,"message":"","data":{"status":"ok"}}`；成功也使用统一 envelope |
| POST | /user/upsert | InternalAuth | `{id, nickname, avatar, extra}`；id 必须是 `u___`/`ag__` 格式；幂等 |
| GET | /user/info?user_ids= | InternalAuth | |
| GET | /user/online_status?user_ids= | InternalAuth | |
| POST | /message/send | InternalAuthAsUser | 以 `X-User-Id` 为 sender；自定义消息 `content_type=100` + `sender_read=false` |
| GET | /conversation/list | InternalAuthAsUser | |
| POST | /group/create, /group/join, /group/kick | InternalAuthAsUser | |

同一套 handler，只换中间件；实现成本约 0.5 天。

`/internal/health` 的成功响应统一 envelope 已确认；原先读取顶层 `status` 的调用方改读 `data.status`。这不改变独立的 LB `/healthz`：它继续返回顶层 `status`／`node_id`，不使用 envelope。

## 10. 多机部署要点与故障矩阵

要点：
- **无状态节点**：进程内只有 WS 连接和群成员缓存，都可丢；任何节点可服务任何用户。
- **LB**：nginx `proxy_http_version 1.1; Upgrade/Connection` 头透传；`proxy_read_timeout ≥ 90s`（大于心跳 75s）。不需要 ip_hash。
- **PG 连接**：`bus=postgres` 时 LISTEN 连接必须直连主库或 PgBouncer session 模式。
- **优雅退出**：收到 SIGTERM → 停止接受新连接 → 取消 Bus 订阅和在线续期 → 给所有 WS 发 close(1001 going away) 并排空队列 → 清理在线登记。排空、续期停止、Remove、Purge、离线推送等待共用调用方的绝对退出期限；期限到达立即硬关底层 socket，不等待控制帧或为每个连接重新分配 5 秒。socket 关闭时立即注销本地连接并释放配额，不等待全局登记清理。只在退出期限内等待在途工作；期限耗尽返回 context 错误，不再等待驱动网络 I/O 或提交后的后台发布，宿主随后关闭依赖，未结束的调用由依赖关闭或自身超时收尾。清理超时的残留登记靠 TTL 失效。客户端重连到别的节点后走 8.6 同步。HTTP 在途请求结束后，关闭依赖前等待离线推送；最多 5 秒且不得超出本次退出的剩余期限。`Shutdown(ctx)` 传播等待超时；分步 `Drain(ctx)` / `Close()` 沿用 Drain 的期限，Close 的无返回值 API 保持兼容并记录错误。未调用 Drain 的 Close 使用独立 5 秒上限。
- **DB 连接池**：先给运维／迁移等预留连接（例如 20 条）。每节点峰值预算为 Store 的 `max_open_conns`，加上 `bus=postgres` 时的最多 4 条发布池连接和 1 条独占 LISTEN，再加上 `cache=pg` 时独立池的最多 4 条连接（与 sqlc/GORM 无关）。总预算不得超过 DB 的 `max_connections`；PG-only 的 Store 上限应满足 `max_open_conns ≤ (max_connections - 运维预留) / 节点数 - 9`。这里是容量上限，不是启动时固定建立的连接数。
- **迁移**：部署前单独执行 `nexo migrate`，节点启动不跑 DDL。
- **时钟**：`send_time` 用服务端时间，仅展示用；顺序完全靠 seq。`online_conns.heartbeat_at` 与 internal 通道的时间窗都依赖节点间时钟一致，需 NTP。
- **密钥轮换**：`auth.external_jwt.secrets` 为列表，平台换密钥时新旧并存一段时间再删旧的。Internal 单密钥使用 `internal_auth.secret: "<old-key>"`；轮换时清空该单值字段，改用 `internal_auth.secrets: ["<new-key>", "<old-key>"]`，待签名方切换完成并经过请求有效窗口后移除旧值。`internal_auth.secret` 本身不接受列表。
- **日志脱敏**：请求日志默认不打 body（`log.request_body=false`）——IM 的 body 就是消息正文；打开后 login / register / message.send（含 internal）仍强制不打，`log.redact_paths` 只是往这个集合里追加。`Authorization` / `X-Signature` 头永不打。`log.skip_paths`（默认 `/healthz`）里的路径整行不写，避免探针刷屏。启动配置中的非空 DSN 整段隐藏。示例 nginx 不记录查询串，并在主级将原始 error log 写入 `/dev/null`，牺牲错误诊断日志以避免 WS query token 落盘；若需错误详情，必须在任何持久化之前脱敏，不能先写容器日志再处理。

| 故障 | 影响 | 恢复 |
| --- | --- | --- |
| 某节点崩溃 | 其上连接断开；未 ACK 的发送；`online_conns` 残留 ≤60s | 客户端重连到其他节点 → GetMaxSeqs 补齐；重试发送走幂等；残留行过期被忽略，节点重启时 PurgeNode |
| Bus 丢事件（Redis/PG 连接抖动）| 少一次实时推送 / 少一次 kick / 成员缓存短暂过期 | 重连后 Resync 触发补齐；不同 native token 的旧连接靠定时 TokenStore 复检失效；external 靠 token 到期或 I/O／读超时等条件关闭，活跃连接不保证 75s 内回收，见 §8.2；成员缓存默认 10s TTL |
| Redis 宕机（作 Bus）| 推送全部停止；Publish 报错只记日志，消息照常落库 | go-redis 自动重连 → Resync；客户端拉取 |
| Redis 宕机（作 Cache）| native Bearer／WS 握手 `503/20101`；token 写入／删除 `500/20001`；internal nonce `500/20002`，见 §6.3／§6.4。external 验证不依赖 Cache | 恢复后重试，不能据依赖故障清 token；PG 部署也可选 `cache=pg` |
| Redis 宕机（作 OnlineStore）| `Online` 报错 → 本次不发 app push（fail-closed）；`/user/online_status` 返回 20001 | 恢复后正常 |
| PG LISTEN 连接断 | 同"Bus 丢事件" | 独占连接重连循环，退避 1s→30s → Resync |
| 平台密钥泄露/轮换 | — | 改 `secrets` 列表滚动重启；旧 token 到期自然失效 |
| Webhook 推送服务不可用 | 少 app push | 不重试；上层服务自行保证 |
| DB 宕机 | 全部写失败，WS 连接保持 | DB 恢复后自动正常 |
| 重复推送 | 客户端显示重复 | 客户端按 (conversation_id, seq) 去重 |

## 11. 配置示例

```yaml
node_id: ""                 # 空 = hostname；env NEXO_NODE_ID
server:
  addr: ":8080"
  trusted_proxies: []       # 允许设置 X-Forwarded-For / -Proto 的 CIDR；空 = 只认对端地址
db:
  driver: postgres          # postgres | mysql
  access: sqlc              # gorm | sqlc（mysql 只能 gorm）
  dsn: "postgres://nexo:nexo@pg:5432/nexo?sslmode=disable"
  max_open_conns: 30
redis:                      # 可选；为空则不初始化
  addr: ""
  password: ""
  db: 0
bus:
  driver: postgres          # redis | postgres | local
online_store:
  driver: db                # db | redis
  ttl: 60s
  renew_interval: 20s
cache:
  driver: pg                # redis | pg | local
  cleaner_interval: 1m      # pg 专用
offline_push:
  driver: noop              # noop | webhook
  webhook_url: ""
  webhook_secret: ""        # env NEXO_OFFLINE_PUSH_WEBHOOK_SECRET；webhook 模式必填
  timeout: 3s
auth:
  providers: [external_jwt, native]   # 启动时决定链顺序：随平台部署 [external_jwt]；独立部署 [native]
  default_platform_id: 5    # HTTP 无 X-Platform-Id 时（external token）
  external_jwt:
    secrets: [""]           # env NEXO_AUTH_EXTERNAL_JWT_SECRETS；可多个，轮换用
    default_role: user      # token 无 role 时
  native:
    secret: ""            # 必填；用 NEXO_AUTH_NATIVE_SECRET 注入
    expire_hours: 168
internal_auth:
  enabled: true
  secret: ""                # env NEXO_INTERNAL_AUTH_SECRET
  secrets: []               # 轮换时新旧并存，任一匹配即通过
  allowed_services: ["my-backend"]
  max_skew_seconds: 300
  require_tls: true         # 明文且无 X-Forwarded-Proto: https → 403（且对端须在 trusted_proxies 内）
ws:
  max_frame_bytes: 65536
  send_queue: 256
  ping_interval: 30s
  pong_wait: 75s
  allowed_origins: []       # 握手 Origin 白名单，空 = 不校验
  deliver_workers: 8        # push 扇出分片数，按 conversation_id 哈希
  deliver_queue: 1024       # 每片队列长度，满则丢弃该 push
limits:
  max_content_bytes: 8192
  pull_page_max: 100
  conversation_page_max: 100
  max_seqs_page_max: 200
  group_max_members: 500
  group_member_cache_ttl: 10s
  ws_conns_per_user: 10
  ws_conns_per_token: 3
  ws_conns_per_ip: 50
  ws_conns_total: 20000
  ws_frames_per_sec: 20
  ws_inflight_per_conn: 8
  message_send_per_min: 120
  ws_send_bytes_total: 536870912
  auth_per_ip_per_min: 0      # 0 = 关；/auth/register、/auth/login 每 IP 每分钟上限（bcrypt 开销），默认交给 LB 限流
log:
  redact_paths: ["/api/v1/auth/login", "/api/v1/auth/register"]  # login/register/message.send 恒不打
  skip_paths: ["/healthz"]  # 整行不写访问日志
  request_body: false       # true 才打 body 前 512 字节
```

启动时校验：`db.driver=mysql` 要求 `db.access=gorm`；`bus=postgres` 要求 `db.driver=postgres`；`bus/online_store/cache` 任一为 `redis` 要求 `redis.addr` 非空；`cache=pg` 要求 `db.driver=postgres`；`offline_push=webhook` 要求 `webhook_url` 为 `https://` 且 `webhook_secret` 非空；`bus≠local`（多节点）且 `auth.providers` 含 `native` 或 `internal_auth.enabled` 时要求 `cache≠local`（token 与 nonce 必须跨节点共享，§6.3）；`auth.providers` 非空且每个 provider 的 secret 非空、互不相同；`internal_auth.enabled` 要求 secret 非空。
校验通过后**打印全部生效配置**（secret 打 `***`）。任何在代码里没有读取的配置项都不允许出现在此文件中（旧项目 `ws.*` 四项写了没生效的教训）。

## 12. 实施计划

| Phase | 内容 | 验收 | 估时 |
| --- | --- | --- | --- |
| 1 骨架 | 配置与组合校验 + 生效值打印、日志（脱敏）、迁移文件（pg + mysql；`cache` 表仅 pg，§11 要求 `cache=pg` 时 `db.driver=postgres`）、`nexo migrate`、sqlc 生成管线、`Store` 接口 + `gormstore`/`pgstore` 空壳、healthz、Hertz 路由（public/internal 两组）、`errcode`（`%w`、非 errcode → 20001）、`internal/identity` | 三种组合（mysql/gorm、pg/gorm、pg/sqlc）都能 `migrate && serve`，`/healthz` 200；identity 单测 | 1.5 天 |
| 2 鉴权与用户 | `Cache` redis + pg（移植 dbcache）+ local、`TokenStore`、`Authenticator` external/native/chain、HTTP 中间件、internal HMAC 中间件、users 两套实现、`/internal/user/upsert`、自建 register/login/logout、`/user/*` | 平台 secret 签的 token 通过并映射成 `u___`；自建同平台登录两次，第一个 token 立即 401（`cache=pg` 与 `cache=redis` 都过）；logout 后 401；两者 secret 互换失败；HMAC 签名错/超窗/服务不在白名单 → 401/403；upsert 幂等 | 2 天 |
| 3 群 | chat_groups / group_members 两套实现，建/加/退/踢/查，入群退群事务写 seq 边界 | 退群后 max_seq 上界正确；重入群 min_seq 重算；建群初始成员批量写入 | 1.5 天 |
| 4 消息与会话 | 发送事务、幂等（DO NOTHING 路径）、`sender_read`、拉取边界、max_seqs、会话列表游标分页 + last_message、未读/已读，两套实现 | 并发 50 协程同会话发送 seq 连续无洞；同 client_msg_id 并发双发得到相同 ACK；`sender_read=false` 双方未读各 +1；列表翻页无重无漏且 last_message 受可见边界约束 | 3.5 天 |
| 5 WS 网关 | 握手、帧协议、`ClientConn` 抽象、连接模型、UserMap、单机 local bus 推送、exp 复检 | 单机两客户端互发实时收到；慢消费者被断开且有计数；定时检查发现 token 过期后发送 Kick 并关闭；检查间隔与排空耗时见 §6.2 | 2 天 |
| 6 多机 | Bus redis + postgres、WS 级 kick 广播、Resync、GroupMemberCache、OnlineStore db + redis、`/user/online_status`、Renew/PurgeNode、优雅退出 | docker-compose 起 3 节点 + nginx：跨节点互发实时收到；同平台第二条连接建立后旧连接收到 2002；同 token 重连不踢；kill 一个节点客户端重连补齐；断 Redis 再恢复收到 Resync；online_status 跨节点正确 | 2.5 天 |
| 7 离线推送 | Pusher 接口、noop、webhook、发送后异步触发、免打扰过滤、默认文案 | 接收方无连接时 webhook 收到一次且仅一次；在线时不收；免打扰不收 | 1 天 |
| 8 收尾 | 集成测试矩阵（3 种数据访问 × 2 种 bus）、README、nginx 配置、docker-compose 三套、平台对接文档（token 格式、HMAC 签名示例、user id 格式）| 矩阵全绿 | 1.5 天 |

| 9 嵌入与客户端 | `server/`（别名 + New/Mount/Start/Shutdown）、`app` 拆 Build/Start/Shutdown、`api.Register` 只挂路由、DB 连接注入、`cmd/nexo` 改调 `server`；`sdk/` net/http 客户端 + internal 签名、`docs/embedding.md` | `cmd/nexo serve` 行为不变（smoke 全过）；示例宿主用自己的 Hertz 挂 `/im` 前缀并进程内 `Message().Send`，WS 客户端实时收到；sdk 对 `api.Register` 起的 httptest engine 跑通 §9 全部接口 | 1 天 |

合计约 16.5 个工作日。

## 13. 后续扩展（不影响一期结构）

- seq 热点缓存（未验证设想）：现行 seq 在消息事务中经会话行锁分配；没有 `Store.AllocSeq` 方法。引入 Cache 分配必须另行设计并证明幂等、并发和回滚不留洞，不能视为简单替换接口；
- at-least-once Bus：outbox 表 + NOTIFY 门铃（PG），或 Redis Streams / NATS：新增一个 Bus 实现；
- MySQL 无 Redis 部署：DB 轮询表版 Bus；
- 平台侧 token 吊销回调：平台通知 nexo 踢掉某用户全部连接（复用 `kick` 事件，`keep_token_id=""`）；
- 在线状态订阅（open-im SubscribeUsersStatus）：复用 OnlineStore + 新事件类型；
- 帧压缩 / protobuf：握手参数已预留；
- 好友/黑名单、撤回、已读回执、群申请审批、置顶排序、内置 APNs/FCM Pusher。

## 14. 已确认假设

| # | 假设 | 影响范围 |
| --- | --- | --- |
| A1 | 账号由外部社交平台管理，token 由平台签发（HS256 共享密钥，claims `user_id int64 + role`）；自建 username/password 账号（`native` provider）用于没有外部平台的独立部署和本地测试；启用哪些 provider 由 `auth.providers` 启动时决定 | auth |
| A2 | 单聊无需好友关系，任意已存在用户可互发 | message service 校验 |
| A3 | 入群无需审批，知道 group_id 即可加入 | group service |
| A4 | 消息 content ≤ 8KB，群 ≤ 500 人，拉取每页 ≤ 100，会话列表每页 ≤ 100 | limits 配置 |
| A5 | 平台 ID 沿用 open-im 编号（1 iOS 2 Android 3 Windows 4 macOS 5 Web 6 MiniWeb 7 Linux 8 AndroidPad 9 iPad 10 Admin）| constant |
| A6 | 无管理员角色；平台后端通过 internal 通道以 `X-User-Id` 代用户操作 | auth |
| A7 | 离线推送文案由 push 侧决定：`Notification` 只带 content_type + 原始 content，服务端只提供 `msgbody.Preview()` 默认实现 | offlinepush, msgbody |
| A8 | 离线推送 fail-closed：OnlineStore 不可用时宁可不推，不打扰在线用户 | message service |
| A9 | `platform_id` 由客户端自报（token 不带），伪造只影响自己哪条连接被踢 | gateway |
| A10 | 客户端收到 `2002 KickOnline` 后不自动重连 | 客户端约定 |
| A11 | 空库起，不迁移旧 nexo_im 的存量数据 | migrations |
| A12 | 平台用户资料由平台后端 `/internal/user/upsert` 写入；收件人无 `users` 行 → 10201 | user service |
| A13 | 会话列表的排序与 last_message 由服务端计算返回；客户端不维护本地消息库，只持久化每会话的同步基线 `local_max`（§8.6） | conversation service |
| A14 | PG `cache` 表为普通表（非 UNLOGGED），过期判断用 DB `now()` | cache |

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

`internal/` 一个包都不搬。`server` 是薄封装：类型别名 + 生命周期，无业务逻辑。

```go
import "github.com/mbeoliero/nexo/server"

cfg := server.DefaultConfig()          // 或 server.LoadConfig(path)：同一套校验
cfg.Db.Access = "gorm"                 // Driver 也须匹配宿主 db；WithPgxPool 则用 sqlc
s, err := server.New(ctx, cfg,
    server.WithGormDb(db),             // 或 WithPgxPool(pool)；传了就不按 DSN 打开 Store，Close 不关它
    server.WithOfflinePusher(p),       // p 实现 server.Pusher（= offlinepush.Pusher 别名）
    server.WithAuthenticator(a),       // 整条 chain 替换；a 实现 server.Authenticator
    server.WithRoutePrefix("/im"),     // 默认 ""，即 /api/v1/** 与 /ws
)
if err != nil {
    return err
}
s.Mount(hz.Engine)                     // 注册 /healthz、/api/v1/**、/api/v1/internal/**、/ws + Trace/AccessLog
s.Start(ctx)                           // PurgeNode、在线续期、Bus 订阅；ctx 取消即停订阅
s.User() / s.Group() / s.Message() / s.Conversation()   // 返回可命名的 server.*Service 别名指针
s.Kick(userId, platformId, keepTokenId)                  // gateway 透传
s.Shutdown(ctx)                        // WS drain（正常 close code 1001）+ PurgeNode + 关自己打开的连接
server.Migrate(ctx, cfg.Db)            // goose，内嵌 SQL
```

别名完整列表见 [`server/types.go`](../server/types.go)：包括 config、鉴权／推送类型、四个 `*Service`，以及 `SendInput`、`Ack`、`ProfileUpdate`、`ConversationList` 等输入输出 DTO。Go 允许对 `internal` 类型取别名并被外部使用；新增 DTO 时同步加别名（`server/types_test.go` 用反射断言每个 service 导出方法的参数/返回类型都可从 `server` 命名）。

规则：

1. 只支持 Hertz 宿主。WS 走 `hertz-contrib/websocket`，不做 net/http 适配；非 Hertz 宿主用 `server.ListenAndServe(ctx, cfg, opts...)` 在宿主 goroutine 里独立起端口（等于 `serve` 去掉信号处理）。
2. `service.New` 的参数是 service 自己声明的仓储接口（其方法签名引用 `internal/store` 的实体类型），外部无法构造 service，只能从 `server.New` 取实例——刻意如此：进程内调用也必须经过 service 的事务和校验。
3. 注入的 DB 连接只覆盖 `Store`。`bus=postgres`（专用 LISTEN 连接）和 `cache=pg` 仍需 `db.dsn`；`config.Validate` 在这两种组合下缺 DSN 时报错。`WithGormDb` 要求 `db.access=gorm`，`WithPgxPool` 要求 `db.access=sqlc`，不匹配报错。注入 MySQL 时，宿主必须保证所有连接禁用 `CLIENT_FOUND_ROWS`；配置 DSN 不代表已注入池的真实参数，不能靠配置校验替宿主证明。无法保证时改由 Nexo 按 DSN 创建连接。
4. `app` 不再处理信号、不调 `log.WithHertz()`、不建 Hertz；这些只在 `cmd/nexo` 和 `server.ListenAndServe`。嵌入时日志沿用 `kit/log` 的全局配置，由宿主决定。
5. 进程内调用不经过 Bearer / 限流（`SendInput.Unlimited` 由宿主自己决定），事件仍经 Bus 推到所有节点。Logout 须携带完整的已验证 Identity（含 TokenId）。注入的 GORM DB 必须是非事务连接，不支持从宿主活动事务内调用业务来获得嵌套／共同提交语义；当前不统一后端误用时的返回结果，也不新增运行时嵌套检测。
6. 一个进程只允许一个 `server.New` 实例：`node_id` 和 online_conns 归属按进程算。

### 15.2 `sdk/` —— HTTP 客户端

结构沿用旧项目 `nexo_im/sdk`（`Client` + 按域分文件 + `Internal*` 方法 + `RequestOption`），三点不同：

1. 同一个 module 的子包，不单独 `go.mod`；外部 `go get` 只编译 `sdk`，不会拉进 GORM / pgx / Hertz。
2. 用 `net/http`，不依赖 Hertz client；`WithHttpClient(*http.Client)` 可换。
3. 不 import 本模块其他包：请求/响应 struct 在 `sdk/types.go` 手写并与 §9 对齐；错误为 `sdk.Error{Code int, Message string, HttpStatus int}`，`Code` 按 §5 的 `K MM NN` 约定判断（`IsBusiness()` = 首位 1）。

方法覆盖 §9.1 公开业务接口和 §9.2 internal 接口（不含独立 `/healthz`）；internal 签名实现与 `auth.Sign` 逐字节一致（`docs/integration.md`），测试里用 `api.Register` 起 httptest engine 做往返验证。`Login` 成功后自动 `SetToken`；客户端默认平台用 `WithPlatformId` 设置，只有声明了 `...RequestOption` 的方法才支持逐请求平台／用户头选项（如 internal 方法的 `sdk.AsUser`）。

- **并发 token：**同一 `*Client` 支持请求与 `SetToken`／`Token`／Login／Logout 并发；请求头只读取一次 token 快照，不持锁执行网络 I/O。成功 Login 更新 token，成功 Logout 清空；失败不改。只同步本地字符串读写，不增加认证操作的代次排序；并发认证操作按实际本地写入顺序覆盖，调用方若需要顺序应串行调用。构造选项只在 `New` 时应用，使用后不复制 Client 或并发修改其他配置；自定义 HTTP transport 的并发安全由宿主保证。
- **成功 envelope：**必须为 JSON 对象且显式携带非 null 的整数 `code`，仅 HTTP 2xx 且 `code=0` 才可能成功。有返回数据的方法要求 `data` 存在、非 null，且能解码为对应 DTO；不增加逐字段业务校验。无返回数据的方法允许省略／null data。非零业务码及原 HTTP 错误映射保留；无效 envelope 返回非 nil 错误，不能静默产生空 DTO。响应体保留 8 MiB 上限，超限报错，不接受截断后看似合法的 JSON。
