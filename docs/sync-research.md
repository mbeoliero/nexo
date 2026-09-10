# 同步研究：OpenIM 源码证据与 Nexo 查询实验

## 1. 范围、版本与结论

本文只记录研究证据与验证边界，实施范围、客户端契约及准入门槛以 [同步设计](sync-design.md) 为准。§1–6 是 OpenIM 固定版本静态核查，未部署 OpenIM 或运行其端到端测试；§7–10 是 Nexo 历史复核与实验，不代表当前版本已经通过相同验证。iOS Swift 实现与设备未取得。

### 版本固定

| 来源 | 实际核查版本 | 永久 commit |
| --- | --- | --- |
| `openimsdk/open-im-server` | `v3.8.3`，2025-01-14 | [`cb4ac19968ec2ff0a4621578f8a51c1c67519a10`](https://github.com/openimsdk/open-im-server/tree/cb4ac19968ec2ff0a4621578f8a51c1c67519a10) |
| `openimsdk/openim-sdk-core` | `v3.8.3`，2025-01-08 | [`8e55d323792ba189154efe42144262c3ced7ea31`](https://github.com/openimsdk/openim-sdk-core/tree/8e55d323792ba189154efe42144262c3ced7ea31) |
| `openimsdk/open-im-sdk-ios` | `3.8.3`，2025-01-08 | [`aa86cd460e0d4835b355cfee8d58a2b049a01f3f`](https://github.com/openimsdk/open-im-sdk-ios/tree/aa86cd460e0d4835b355cfee8d58a2b049a01f3f) |
| SDK Core 补充抽查 | 当次 `main` 快照，2026-06-10；只用于核对心跳 | [`9267a252faab02bbc8ffab6223e6ae2341a0f7c9`](https://github.com/openimsdk/openim-sdk-core/tree/9267a252faab02bbc8ffab6223e6ae2341a0f7c9) |
| 官方文档 | 当次 `main` 快照；不代表上述旧 tag 的实现承诺 | [`7caa852c58b28382c43ea21e99be6814576abb47`](https://github.com/openimsdk/docs/tree/7caa852c58b28382c43ea21e99be6814576abb47) |

选择同代 `3.8.3` 是为了能串起 server、Go Core 与 iOS 包装层，不宣称它是当前最新稳定补丁。iOS 的 [podspec:76–80](https://github.com/openimsdk/open-im-sdk-ios/blob/aa86cd460e0d4835b355cfee8d58a2b049a01f3f/OpenIMSDK.podspec#L76-L80) 明确依赖 `OpenIMSDKCore 3.8.3`；未验证发布的二进制 framework 与 Go tag 的可复现构建一致性。

### 核心结论

1. **Presence 的协议是“订阅时拿快照，随后 WS 推送变化”**，不是聊天页必须每 8 秒查询；但该 Core tag 的取消/重连恢复实现有明显静态核查疑点，不能把 API 名字当成功保证。
2. **Core v3.8.3 的心跳每 24 秒发送 Ping，只携带 operation ID；不带 maxSeq，也不调用 `GetNewestSeq`。** 服务端 Pong 回显 Ping 数据，不负责消息对账。
3. **已核消息主链是连接成功/重连、前台唤醒取最大 seq，加收到消息后的 gap 补拉。** 在该版本非测试 Core 源码中，未找到独立周期性 `GetNewestSeq` 对账；这不证明所有版本、商业 SDK 或业务 App 都没有轮询。
4. **“最后一条推送丢了，连接仍活着，之后没有新消息”不产生 seq gap。** 所核心跳不会发现这种业务层漏推；不能据 OpenIM 有 WS 就删除另一个系统的定时兜底。
5. **OpenIM 会话列表 API 读 SDK 本地库，变更通过 listener 更新 UI；会话元数据另有 version 增量同步。** 这与 Nexo A13 的非完整消息库边界不同，不能直接照搬。

## 2. 在线状态：查询、订阅与断线不是同一件事

### 2.1 服务端 WS 订阅是真实实现，不只是文档

[server `internal/msggateway/subscription.go:21–41`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/subscription.go#L21-L41)：`SubUserOnlineStatus` 解码 `SubscribeUserID` / `UnsubscribeUserID`，更新当前连接的订阅关系；对新增订阅逐个读取 `OnlinePlatformIDs`，在响应中返回快照。

同文件 [98–147](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/subscription.go#L98-L147) 维护目标用户到订阅连接的映射；[150–166](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/subscription.go#L150-L166) 仅向这些连接发送变化。实际帧由 [`client.go:338–344`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/client.go#L338-L344) 以 `WsSubUserOnlineStatus` 写入 WS。

Core 收到该帧后更新本地状态并触发变化回调：[`long_conn_mgr.go:520–540`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/long_conn_mgr.go#L520-L540)。不是每一条状态变化都要求 App 再发 HTTP 查询。

当前官方 iOS 文档建议只订阅可见用户、离开可见范围/页面结束时取消：

- [订阅用户在线状态](https://github.com/openimsdk/docs/blob/7caa852c58b28382c43ea21e99be6814576abb47/content/zh/docs/chat/sdk/ios/user/retrieving-and-updating-user-information/subscribe-user-status.mdx)
- [取消用户状态订阅](https://github.com/openimsdk/docs/blob/7caa852c58b28382c43ea21e99be6814576abb47/content/zh/docs/chat/sdk/ios/user/retrieving-and-updating-user-information/unsubscribe-user-status.mdx)

这些是接口使用建议，不代表已替旧 tag 验证所有生命周期行为。未在本次所核服务端订阅实现中验证统一订阅人数上限；不把旧搜索摘要中的“3000”当作此版本已执行的限制。

### 2.2 查询与订阅必须区分入口

| 入口 | 此版本实际行为 |
| --- | --- |
| 服务端 `GetUserStatus` RPC（由在线状态 HTTP 查询入口调用） | 读取指定用户的平台列表，返回当时快照；这段代码不建立 WS 订阅。 |
| Core `SubscribeUsersStatus` | 经 `GetUserOnlinePlatformIDs` 首次发订阅请求；已有已完成缓存时可直接返回缓存。 |
| Core 对外 `GetUserStatus` | 实际调用 `SubscribeUsersStatus`，不能因名字是 Get 就当成纯查询。 |
| Core `GetSubscribeUsersStatus` | 本 tag 存在空参数提前返回问题，见下节。 |
| 服务端 `SubscribeOrCancelUsersStatus` / `GetSubscribeUsersStatus` RPC | 本 tag 是返回空响应的占位函数；不要与已实现的 gateway WS 订阅混淆。 |

证据：[server `internal/rpc/user/online.go:12–52,73–75`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/rpc/user/online.go#L12-L75)、[Core `open_im_sdk/online.go:7–24`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/open_im_sdk/online.go#L7-L24)、[Core `long_conn_mgr.go:543–565`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/long_conn_mgr.go#L543-L565)、[Core `subscription.go:138–177`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/subscription.go#L138-L177)。

### 2.3 连接范围、取消与自动恢复的核查边界

**服务端明确按连接持有订阅。** 注销连接时调用 `subscription.DelClient`，从目标用户映射移除该连接；不意味着同账号其他设备的订阅也被删除。证据：[server `ws_server.go:383–397`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/ws_server.go#L383-L397)、[`subscription.go:59–82`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/subscription.go#L59-L82)。

**Core 有恢复订阅的代码意图，但这个 tag 不能认证为可靠完成：**

1. 重连会调用 `writeConnFirstSubMsg`，用本地 `sub` 集合重新发送订阅；但 `subscription.go` 中 `sub` 创建为空 map，所核包内未找到添加成员的赋值，只有查询/删除/取 keys。不能据该函数存在就断言恢复有效。[`long_conn_mgr.go:574–585`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/long_conn_mgr.go#L574-L585)、[`subscription.go:41–63,138–177`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/subscription.go#L41-L177)。
2. `UnsubscribeUserOnlinePlatformIDs` 只改本地集合并返回；没有立即发送 WS 取消帧。待取消集合原本要在后续有新增订阅时搭载发送，因此“取消 API 返回成功”不是“服务端已确认取消”的证明。[`long_conn_mgr.go:567–572`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/long_conn_mgr.go#L567-L572)、[`subscription.go:123–177`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/subscription.go#L123-L177)。
3. `GetSubscribeUsersStatus` 调用 `subscribeUsersStatus(ctx, nil)`，后者开头遇空 slice 就返回空数组，无法进入底层“nil 表示全部订阅”的逻辑。[`internal/interaction/online.go:9–13,44–46`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/online.go#L9-L46)。

以上为直接读源码所得，未跑设备/服务端集成复现，也未确认这些疑点在每个发行包中的修复状态。**可借鉴的是按需订阅协议，不应把此 tag 的客户端生命周期代码当成无缺陷参考实现。**

### 2.4 多设备与在线状态传播

在线不是单一连接布尔值：返回 `PlatformIDs`，平台列表非空才视为 Online。[server `online.go:12–25`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/rpc/user/online.go#L12-L25)。网关记录每个用户的连接列表，移除一条连接后仍保留其他连接，并上报剩余在线平台及离线平台；不能把“手机断开”直接当成“该用户全端离线”。[`user_map.go:119–162`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/user_map.go#L119-L162)。

跨节点传播使用 Redis 在线状态与 Pub/Sub：状态变化脚本更新平台集合，变化时 Publish，网关接到变化后向订阅连接推送。[Redis `online.go:84–137`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/pkg/common/storage/cache/redis/online.go#L84-L137)、[gateway `subscription.go:12–18`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/subscription.go#L12-L18)。

普通状态更新有 **1 秒合并 ticker**；此外在线登记有租约续期，不是客户端轮询：`OnlineExpire = 30 分钟`、续期 ticker 为其三分之一，即 **10 分钟**。这不是“正常下线要等 30 分钟”，也不是在线状态端到端时延 SLA；异常进程死亡、缓存、丢失通知需要另测。[gateway `online.go:20–36,115–128`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/online.go#L20-L128)、[`cachekey/online.go:8–12`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/pkg/common/storage/cache/cachekey/online.go#L8-L12)。

## 3. 消息同步：不能把心跳误说成周期对账

### 3.1 默认心跳到底发什么

Core 常量为 `pongWait = 30s`、`pingPeriod = pongWait × 8/10 = 24s`。`heartbeat` 的 ticker 只调用 `sendPingMessage`；后者在已连接时执行：

```go
c.conn.WriteMessage(PingMessage, []byte(opid))
```

这里的 `opid` 是新生成的 operation ID，**不是 maxSeq**。证据：[Core `long_conn_mgr.go:45–54`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/long_conn_mgr.go#L45-L54)、[`300–344`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/long_conn_mgr.go#L300-L344)。

服务端 `pingHandler` 延长读超时并原样 `writePongMsg(appData)`；实际 Pong payload 仍是 `appData`，没有查消息服务/最大 seq：[server `client.go:113–120`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/client.go#L113-L120)、[`415–439`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/client.go#L415-L439)。Core 的 `pongHandler` 只延长读超时：[Core `long_conn_mgr.go:738–745`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/long_conn_mgr.go#L738-L745)。

服务端主动发 Ping 的分支限定 Web 平台，周期是 `30s × 9/10 = 27s`，不能误写成 iOS 的 27 秒心跳：[server `client.go:375–411`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/client.go#L375-L411)、[`constant.go:60–67`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/constant.go#L60-L67)。

补充抽查 Core `main` 固定快照的 [`long_conn_mgr.go:436–484`](https://github.com/openimsdk/openim-sdk-core/blob/9267a252faab02bbc8ffab6223e6ae2341a0f7c9/internal/interaction/long_conn_mgr.go#L436-L484)，心跳仍只写 Ping/opid；该快照多了前后台 context 取消路径，**不能反向把它的后台行为套到 3.8.3**。

### 3.2 哪些事件真正请求最大 seq

| 触发 | 所核 Core v3.8.3 行为 |
| --- | --- |
| 初次成功连接 / 断线重连成功 | `reConn → TriggerCmdConnected → CmdConnSuccesss → doConnected → GetNewestSeq`，比较服务端 maxSeq 与本地同步游标。 |
| 从后台回前台 | `SetAppBackgroundStatus(false)` 请求成功后发送 `CmdWakeUpDataSync`；`doWakeupDataSync` 触发业务数据同步并请求 `GetNewestSeq`。 |
| 收到新消息 | 比较收到的 seq 与 `syncedMaxSeqs`；不连续时 `PullMsgByRange` 补拉，不必先取所有会话 maxSeq。 |
| 定时心跳 | 仅 Ping/Pong；不走以上命令。 |

证据：[Core 连接成功 `long_conn_mgr.go:680–697`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/long_conn_mgr.go#L680-L697)、[消息事件分派 `msg_sync.go:198–219`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/msg_sync.go#L198-L219)、[gap / 连接 / 唤醒 `msg_sync.go:311–381`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/msg_sync.go#L311-L381)、[后台状态入口 `userRelated.go:457–473`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/open_im_sdk/userRelated.go#L457-L473)。

本次检索整个该 tag 的非测试 Go Core 中 `GetNewestSeq`、`TriggerCmdWakeUpDataSync`、`CmdWakeUpDataSync`、ticker/timer 调用，再沿调用链核查：发 `GetNewestSeq` 的两个位置是 `doConnected` / `doWakeupDataSync`；`MsgSyncer.DoListener` 本身只消费命令和 context 结束，没有周期对账分支。`startSync` 内的 **5 秒 Sleep 是重置同步抑制标记，不是 5 秒消息轮询**。[`msg_sync.go:168–186,284–307`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/msg_sync.go#L168-L307)。

因此准确说法是：**在这个固定版本的所核 Core 主链，没有找到常驻周期最大 seq 对账；不能泛化成“OpenIM 绝不轮询”。** App 可以另设计时器调用 API；私有/商业二进制与其他发行版本未全部核查。

### 3.3 初始同步不是一次下载全部历史

`loadSeq` 从本地会话/聊天记录/通知 seq 初始化 `syncedMaxSeqs`，根据安装标记区分重装；重装路径跳过旧通知内容并登记通知水位。默认 `connectPullNums = 1`、`defaultPullNums = 10`、批量门槛 `SplitPullMsgNum = 100`。实际拉取请求有每会话 `Begin / End / Num`，所以“发现缺口”不意味着此次一定下载范围内全部聊天历史。

证据：[Core `msg_sync.go:39–45,86–165`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/msg_sync.go#L39-L165)、[重装与比较 `221–282`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/msg_sync.go#L221-L282)、[请求组装 `560–578`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/interaction/msg_sync.go#L560-L578)。

服务端 `GetMaxSeq` 会取用户会话 ID，追加对应通知会话和用户自通知会话，再批量读 seq；不是常量大小的无成本 Ping。[server `internal/rpc/msg/sync_msg.go:120–147`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/rpc/msg/sync_msg.go#L120-L147)。

### 3.4 静默漏掉末条消息：已核机制的边界

假设服务器已提交 seq=42，客户端仍在 41；42 的业务推送未到达，但 Ping/Pong 正常，后续没有 seq=43。则：

- 心跳 payload 没有水位，无法比较 41/42。
- gap 检测需要收到后续带 seq 的消息；此时没有输入。
- 连接成功/前台唤醒可以触发补偿，但用户可能一直留在前台且连接不断。

**推论：本次所核链路没有证明对此情形有有界时间的自动发现保证。** 这不是声称 OpenIM 线上一定发生该故障，也不是宣称其所有服务器重试/商业增强都不存在；只是不能以所核 Ping/Pong、重连和 gap 代码证明“周期兜底多余”。

## 4. 会话增量同步、本地库与 listener

OpenIM 的“刷新会话列表”需区分 **SDK 读本地列表** 和 **向服务端同步会话元数据**，不能与业务 HTTP 每 60 秒全量刷新等同。

1. **本地存储**：Core 用 SQLite/GORM 初始化数据库；收到消息后持久化 `LocalChatLog`。证据：[`pkg/db/db_init.go:154`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/pkg/db/db_init.go#L154)、[`conversation_msg.go:671–691`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/conversation_msg/conversation_msg.go#L671-L691)。
2. **列表读本地**：`GetAllConversationList`、`GetConversationListSplit` 直接调用本地 DB。证据：[`internal/conversation_msg/api.go:36–42`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/conversation_msg/api.go#L36-L42)。
3. **增量同步协议**：客户端请求带 `UserID + Version + VersionID`；服务端返回 `VersionID / Version / Full / Delete / Insert / Update`；SDK `VersionSynchronizer` 合并本地记录，`Full` 时走完整同步/核对路径。证据：[Core `server_api.go:70–72`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/conversation_msg/server_api.go#L70-L72)、[`incremental_sync.go:26–87`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/conversation_msg/incremental_sync.go#L26-L87)、[server `conversation/sync.go:33–56`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/rpc/conversation/sync.go#L33-L56)。
4. **同步由事件驱动**：初始化/重连同步链、唤醒同步链、会话变更通知均会触发会话同步；不是只有消息 seq 变化才更新元数据。证据：[`notification.go:71–117`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/conversation_msg/notification.go#L71-L117)、[`430–449`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/conversation_msg/notification.go#L430-L449)、[`488–505`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/conversation_msg/notification.go#L488-L505)。
5. **UI 用变更回调**：本地会话更新后发 `OnConversationChanged` / `OnNewConversation`，并有 `OnSyncServerStart/Finish/Failed`；显示层能按回调更新，不必每次向服务端拉全表。证据：[`notification.go:195–228`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/conversation_msg/notification.go#L195-L228)、[`332–363`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/internal/conversation_msg/notification.go#L332-L363)。

这证明 OpenIM 有另一套完整的数据同步/本地读模型，**不构成让 Nexo 一期新增完整消息库或会话 version 协议的批准**。

## 5. iOS 前后台：包装层、Core、操作系统各有边界

### 3.8.3 包装层可核行为

`OIMManager` 自动监听 `UIApplicationDidEnterBackgroundNotification` 与 `UIApplicationWillEnterForegroundNotification`，分别调用 Core `SetAppBackgroundStatus(YES/NO)`。相关回调在这里没有业务处理。证据：[iOS `OIMManager.m:35–46`](https://github.com/openimsdk/open-im-sdk-ios/blob/aa86cd460e0d4835b355cfee8d58a2b049a01f3f/OpenIMSDK/Interface/OIMManager.m#L35-L46)、[`86–105`](https://github.com/openimsdk/open-im-sdk-ios/blob/aa86cd460e0d4835b355cfee8d58a2b049a01f3f/OpenIMSDK/Interface/OIMManager.m#L86-L105)。

在这个 Core tag 中，设置后台状态主要发 WS 请求并保存标记；回前台且请求成功后才发唤醒同步事件。**该函数没有主动 Close WS，也没有停止 heartbeat ticker**。[Core `userRelated.go:457–473`](https://github.com/openimsdk/openim-sdk-core/blob/8e55d323792ba189154efe42144262c3ced7ea31/open_im_sdk/userRelated.go#L457-L473)。服务端对应入口保存 `Client.IsBackground`，也不是在这里断开连接：[server `client.go:259–268`](https://github.com/openimsdk/open-im-server/blob/cb4ac19968ec2ff0a4621578f8a51c1c67519a10/internal/msggateway/client.go#L259-L268)。

### 未能验证的边界

- 不将上述代码解读为“iOS 后台永远有可靠长连接”。本次未在设备上验证挂起、系统杀进程、后台权限、APNs 到达/唤醒条件及业务 App 的额外生命周期代码。
- 不将 `SetAppBackgroundStatus(false)` 等同于每次必然完成补偿：所核函数在 WS 请求失败时直接返回，没有在这条失败分支发唤醒同步事件。
- Core 较新 `main` 心跳已有 `fgCtx.Done()` 退出逻辑；只抽查该处，未核整个新版本的后台停连/恢复契约，不能混写为 3.8.3 行为。
- 未验证用户当前使用的 iOS SDK/Swift 文件与上述 OpenIM tag 或 Nexo 客户端的对应关系；用户提供的 8s/60s/15s 间隔暂按描述，而非代码实证。

## 6. 社区版、商业版与可借鉴范围

上述 WS 订阅、消息 seq 补偿、本地会话库和增量同步均在官方公开源码中，不需要以“商业版特性”解释它们的存在。官方 [Edition 页面](https://openim.io/enterprise/) 将 Open Source 列为 Server、IMSDK、Messaging、Groups & contacts、自托管与 Demo；Business 增加成品客户端/UI，Enterprise 增加高级消息/音视频等能力。

该产品页不是逐个 API 的版本/授权矩阵。**本次未取得商业 SDK/成品 App 私有源码，不能确认它们的默认轮询频率、可靠性增强或后台行为**。也不能用当前文档覆盖旧 tag 的实现疑点。

可借鉴的事实分成两层：

| 层次 | 已有证据 | 不能据此推导 |
| --- | --- | --- |
| 机制 | 按需订阅 + WS 状态变更；连接/唤醒 + seq gap 补偿；会话 version 同步与本地 listener | Nexo 已拥有这些机制，或应跳过设计确认新增它们 |
| 可靠性 | 心跳测连接活性，不测末条业务消息完整性 | 有 WS 就能删除兜底对账；仅 seq 不变就能忽略会话选项变化 |

## 7. Nexo 对照（已复核本地源码）

### 设计与实现事实

研究时复核了以下分支；当前契约由 [主设计](design.md) 和 [接入协议](integration.md) 维护，候选改进由 [同步草案](sync-design.md) 维护。

| 本地证据 | 研究结论 |
| --- | --- |
| [OnlineStore](design.md#65-onlinestore--全局在线连接登记)、A13 与同步规则 | 更频繁地查 presence 不能消除登记 TTL 的滞后；Nexo 只持久化连续基线，不能直接照搬 OpenIM 完整本地消息库。 |
| [gateway/push.go](../internal/gateway/push.go)、[message/bus.go](../internal/service/message/bus.go) | 分片丢弃、查询或 Publish 失败时连接可保持正常；重连及后续 gap 不保证发现静默末条漏推。 |
| [gateway/subscribe.go](../internal/gateway/subscribe.go)、[conversation.go](../internal/service/conversation/conversation.go) | 群缓存失效不是完整元数据通知；水位不含置顶／免打扰，seq 不变不能证明列表资料不变。 |

本研究未运行本地功能测试。仓库及邻近 `nexo_dev` 未找到所述 Swift 文件，业务 `/api/v1/im/conversations` 也不在已查路由内。以下仍是**用户描述，未实证**：聊天页每 8 秒查询在线状态；前台每 60 秒 max_seqs→缺口 pull→业务 conversations；每 15 秒做本地凭证检查；后台停止定时器并断 WS。它们不是服务端协议常量，也不能据此认定客户端缺陷或业务接口成本。

### 空闲请求量示例：算术估算，不是压测

假设 **10,000 个前台用户全部停在聊天页**，每轮 `max_seqs` 与会话列表各恰好 1 个请求，平均摊开而非同时触发：

```text
presence：10,000 / 8                  = 1,250 QPS
max_seqs + conversations：10,000×2/60 ≈   333 QPS
合计                                 ≈ 1,583 QPS
```

未计缺口 pull、认证/失败重试、分页、实际在线人数分布及同时唤醒尖峰。15 秒 credential 若确为纯本地检查，不计入服务端 QPS。

单把 presence 周期从 **8 秒改成 30 秒**，其请求量从 1,250 降到约 333 QPS，**下降约 73%**；不代表总体 CPU、数据库负载或延迟也等比例变化。这里只是算术对照，不是已实施/批准的策略，也不是删除消息兜底或元数据校准的证据。

## 8. Nexo 现有测试与兼容边界

历史记录涉及现有 gateway、消息/会话服务与 SDK；复跑入口：

```sh
go test -json -race -count=1 -p=1 ./internal/gateway ./internal/service/message ./internal/service/conversation ./internal/service/conv ./sdk
```

未提供真实依赖时，`PresenceRecovery`、`MessageDatabaseRegressions`、`MarkReadQuitSnapshot` 的数据库子项跳过。专属可销毁实例、`NEXO_TEST_DISPOSABLE=1`、串行运行与完整矩阵入口遵守 [贡献指南](../CONTRIBUTING.md#build-and-test)；以下局部证据不能替代数据库矩阵。

| 现有证据 | 不能证明 |
| --- | --- |
| `TestFullDeliverShardDropsAndCounts`：队列满丢弃并计数 | 已实现补偿 |
| `TestResyncOnlyAfterReconnect`：模拟 Bus 重连回调发送 2004 | 真实断网恢复 |
| `TestSendBytesCapSendsResync` / `TestSlowConsumerIsClosedAndCounted`：预算超限发 2004、慢连接关闭 | 客户端已完成补拉 |
| Push / Pull / MarkRead 与 `TestSendSingle`：内存 Store 的扇出、可见范围、已读、发送幂等 | 真实数据库完整验证，或草案 A 已重新发布 |

静态核查只支持有限兼容判断：旧 gateway 对新请求编号返回协议错误；SDK 容忍额外 HTTP 字段不代表支持新 WS 行为；Web 忽略未知推送或 2004 的 items 后仍走全量 1001，不证明设备端恢复成功。未跑 Web/iOS 设备兼容或新协议验收，不能据现有测试宣称旧 App 兼容或端到端 60 秒恢复通过。

## 9. 查询成本实验

历史基准位于 `internal/store/pgstore/sync_cost_test.go`；仅允许专属可销毁数据库，缺少测试 DSN 或 `NEXO_TEST_DISPOSABLE=1` 时跳过。复跑：

```sh
bash scripts/test-all.sh -run '^$' -bench '^BenchmarkSyncCost$' -benchtime=1x -count=3
```

`-run '^$'` 跳过普通测试，不能称完整数据库矩阵通过。环境：Go 1.27、darwin/arm64、Apple M2；Docker 29、8 CPU、8 GiB、PG 16。仅测 PG/sqlc，MySQL/Redis 没有性能数据；合成每人 250 会话、预热、串行完整分页，每组 3 个单轮样本。现有路径与原型均读全部水位并断言 min/max/read 投影一致。

| 用户数 | 路径 | 整轮中位 ms | 查询次数/轮 | Go 分配字节/轮 |
| ---: | --- | ---: | ---: | ---: |
| 100 | MaxSeqs | 79.354 | 200 | 31,631,056 |
| 100 | 批量水位原型 | 262.401 | 13 | 5,509,048 |
| 100 | MaxSeqs＋List | 301.389 | 800 | 115,129,152 |
| 1000 | MaxSeqs | 1,090.719 | 2000 | 329,637,432 |
| 1000 | 批量水位原型 | 3,861.088 | 130 | 68,477,424 |
| 1000 | MaxSeqs＋List | 3,849.397 | 8000 | 1,166,198,936 |

**公平比较是 MaxSeqs 与批量原型：查询次数下降 93.5%、Go 累计分配下降约 79–83%，但整轮耗时变为 3.31/3.54 倍。** 不支持“批量化降低整轮耗时”。MaxSeqs＋List 只展示连带刷新成本；原型没有置顶、免打扰、资料等完整列表能力，不能用组合路径的差额宣称业务成本节省，也不能把 Nexo List 等同于尚未取得的业务 conversations 接口。

这些数据不是单请求 p95、生产吞吐、DB CPU、常驻内存或端到端恢复时限。投影一致只覆盖本种子，不覆盖完整群生命周期、并发写入或真实 iOS；未证明完全隔离其他进程竞争。此轮未采集执行计划，不能由查询次数推断数据库工作量；后续诊断见 §10。

## 10. 执行计划与修正对照

### 10.1 探针与取样范围

`NEXO_SYNC_EXPLAIN=1` 读取 pgx 预热连接的 `pg_prepared_statements`，对原始预编译语句执行 `EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) EXECUTE`，不是另写针对常量优化的查询。复跑：

```sh
NEXO_SYNC_EXPLAIN=1 bash scripts/test-all.sh -run '^$' \
  -bench '^BenchmarkSyncCost/users=1000/conversations=250/max_seqs$' \
  -benchtime=1x -count=1 -v
```

在 1000 用户 × 250 会话数据集中，只取第一组 100 用户的首、第 7、末（第 13）页，对比 auto / force_custom_plan / force_generic_plan；不是全用户、全页统计。整轮基准才遍历全部用户。

### 10.2 原始 SQL：后页索引读取放大，不是 generic plan

| 页 | 返回行数 | auto 执行 ms | 关键路径 |
| ---: | ---: | ---: | --- |
| 1 | 2000 | 3.631 | 主键索引＋Nested Loop；UC 扫描约 2037 次缓存块命中 |
| 7 | 2000 | 68.900 | 同类路径；UC 扫描约 25174 次命中，耗时 65.715ms |
| 13 | 1000 | 292.779 | Bitmap Index Scan 约 99252 次命中，随后 Hash Join、排序 |

缓存命中可重复，不是独立页数或磁盘读取次数。后两页无物理块读取、无 JIT；末页虽扫描全部 250000 行 conversations，主要时间仍在 UC 的 Bitmap Index Scan（约 279ms），不能笼统归因于 JOIN 或网络。

原始语句为 `generic_plans=0, custom_plans=130`，强制 custom 中/末页仍约 70/301ms。因此这些样本排除了“原始语句被 generic plan 拖慢”；主要时间已在 DB 执行阶段，而非 Go 解码。

### 10.3 单变量：owner 下界与计划选择是两个问题

保留元组游标，仅为非空主键字段增加逻辑冗余下界：

```sql
WHERE uc.owner_id = ANY($1::text[])
  AND (uc.owner_id, uc.conversation_id) > ($2::text, $3::text)
  AND uc.owner_id >= $2::text
```

条件不改变返回行，明确约束索引首列；两种查询都完整分页并与 MaxSeqs 全部 min/max/read 比较，未新增索引或修改生产 SQL/migration。后续同轮计划对照：

- 原始 SQL 中/末页约 69.5/294.6ms；下界＋custom 约 3.8/2.4ms，后页索引放大减轻。
- 加下界的 auto 变为 `generic_plans=125, custom_plans=5`；首页约 25000 个候选经查询/JOIN/排序后限制 2000 行，约 40.4ms；相同参数 custom 首页约 4.2ms。

下界修复后页读取放大，不保证其他页计划合适；一页变快不能证明整轮变快。

### 10.4 完整一轮复测：1000 用户仍慢约 1.65 倍

仅独立测试连接设 `plan_cache_mode=force_custom_plan`，保留 pgx 预编译；不改默认连接、应用或 DB 全局设置。去掉 EXPLAIN 后，每路径再取 3 个整轮样本：

```sh
bash scripts/test-all.sh -run '^$' \
  -bench '^BenchmarkSyncCost/users=(100|1000)/conversations=250/(max_seqs|batch_heads_prototype|batch_heads_bounded_prototype|batch_heads_bounded_custom_prototype)$' \
  -benchtime=1x -count=3
```

| 路径 | 100 用户整轮中位 ms | 1000 用户整轮中位 ms |
| --- | ---: | ---: |
| MaxSeqs | 86.415 | 1097.819 |
| 原始批量 SQL / auto | 263.756 | 3805.606 |
| owner 下界 / auto | 129.831 | 3043.784 |
| owner 下界 / custom | 57.320 | 1815.972 |

100 用户下界＋custom 较快，1000 用户仍慢约 1.65 倍；其他用户分组/页面剩余耗时未逐一归因，不能泛化首组改善。批量仍为 13/130 次查询、现有路径 200/2000 次，返回水位一致；结论仅限 PG 16 合成数据及上述条件。

负结论：该原型不是已验证的降压实现；统一调度不等于必须跨用户批量 SQL，同用户多连接共享读取的收益也未通过此实验验证。D 的有界观察调用形态仍须另测，不能继承这些数据为性能或 60 秒恢复保证。实施选择只见 [同步设计](sync-design.md)，本记录不批准生产索引、全局计划设置或协议变更。
