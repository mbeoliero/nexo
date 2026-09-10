# 工程文档实践研究：AGENTS.md、README 与技术方案

## 1. 范围与结论

检索日期：**2026-09-09**。本文记录一手指南及据此提出的建议，不是新增的项目规范，也不改变已有契约。
本稿覆盖 AGENTS.md、README、技术方案、架构说明和 ADR；指南网页按访问时内容核查，论文引用已核实的版本。
下文“来源建议”是对原文的转述；“综合建议”是针对工程项目的归纳，不代表某个来源规定了统一模板。

**综合结论：**

1. AGENTS.md 告诉编码代理如何在本仓库工作，重点是可执行规则、特殊约束与验证入口。
2. README 帮读者判断项目是否适合自己，并完成第一次使用。
3. 技术方案帮助团队审查改变；架构说明帮助维护者理解当前系统；ADR 保存重要选择及原因。
4. 同一事实需要明确归属，其他文档通过摘要和链接引用；文档维护应进入日常变更流程。

这些结论分别基于 [OpenAI 的 AGENTS.md 建议](https://learn.chatgpt.com/guides/best-practices)、[GitHub README 指南](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes)、[Go 提案流程](https://go.googlesource.com/proposal/)、[AWS ADR 流程](https://docs.aws.amazon.com/prescriptive-guidance/latest/architectural-decision-records/adr-process.html)和 [Google 文档实践](https://google.github.io/styleguide/docguide/best_practices.html)。

## 2. AGENTS.md：仓库工作的规则与入口

### 2.1 来源建议与工具边界

AGENTS.md 开放格式采用普通 Markdown，没有必填字段；它补充代理工作所需的构建、测试、约定等上下文，与面向人的 README 分工。[AGENTS.md 官方站点](https://agents.md/)

OpenAI 建议保留实用、准确、简短的长期指导；根据反复出现的错误迭代规则，篇幅变大时引用任务相关文档。自动生成的初稿仍需按团队实际流程修订。[OpenAI：Best practices](https://learn.chatgpt.com/guides/best-practices)

GitHub 的经验文章强调完整命令及参数、具体示例、清晰操作边界。但该文以 `.github/agents/*.md` 自定义代理为背景，其 YAML frontmatter 和角色定义不能直接当作根目录 AGENTS.md 的格式要求。[GitHub：Lessons from over 2,500 repositories](https://github.blog/ai-and-ml/github-copilot/how-to-write-a-great-agents-md-lessons-from-over-2500-repositories/)

**Codex 特有行为：**启动时按全局、项目根目录到当前工作目录发现指导文件；每个目录优先选择 `AGENTS.override.md`，然后是 `AGENTS.md` 和配置的备用名称，至多取一个文件；合并后更深层指导优先。默认 `project_doc_max_bytes` 为 32 KiB，这是加载上限，不是推荐篇幅。子目录加载行为应在所用代理及启动位置验证，不能假设所有工具相同。[OpenAI：Custom instructions with AGENTS.md](https://learn.chatgpt.com/docs/agent-configuration/agents-md)

### 2.2 推荐内容（综合建议）

| 内容 | 应写到什么程度 |
| --- | --- |
| 必要上下文 | 项目用途、关键入口、会影响本次修改的特殊约束。 |
| 工作与验证入口 | 可执行的构建、测试、生成命令，以及运行条件和完整流程链接。 |
| 工程边界 | 哪层可调用哪层、哪些生成文件应通过工具更新、哪些契约要同步。 |
| 特殊约定 | 项目明确采用且容易误判的写法，必要时链接一个现有示例。 |
| 决策权限 | 哪些动作可自行执行、哪些变化需要确认、哪些操作禁止；触发条件要具体。 |
| 完成条件 | 必须验证什么，结果怎样报告，跳过验证时需要说明什么。 |

这份清单综合 [OpenAI](https://learn.chatgpt.com/guides/best-practices)与 [GitHub](https://github.blog/ai-and-ml/github-copilot/how-to-write-a-great-agents-md-lessons-from-over-2500-repositories/)的实践建议。只有子项目规则确实不同时才考虑嵌套文件；根文件保留所有相关任务都必须知道的约束。

建议采用“**触发条件 → 必须动作 → 验证或权威入口**”写法。例如 Nexo 已有的 SQL 生成规则可以表述为：修改指定 SQL 输入后运行 `make sqlc`；生成目录通过生成器更新；完整开发流程见 CONTRIBUTING。这种规则能直接指导执行。详细命令说明只维护一处，AGENTS.md 保留必要调用点。[本项目规则](../AGENTS.md#commands)

可按以下骨架筛选现有内容，无需先增加章节填充文字：

```markdown
# 项目工作指南

## 入口与权威文档

项目约束；按修改类型说明应读取哪些文档。

## 执行与验证

关键命令、适用条件、完成判据、完整流程链接。

## 必须保持的规则

分层边界、生成文件、特殊编码约定及对应示例。

## 决策边界

需要确认的具体变更；明确禁止的操作。
```

**筛选建议：**逐条检查一项规则是否会改变代理的实际动作；合并重复规则，删除失效事项；本次任务的待办留在 issue／任务记录。能稳定自动检查的要求，可由格式化器、测试或 CI 执行，并在文档中保留入口。这是本次维护建议；不要机械移除尚未被检查覆盖的规则。

### 2.3 效果证据与局限

研究结果支持谨慎评估实际收益。Gloaguen 等人的 v2 在所测任务中发现，上下文文件未普遍提高成功率，平均推理成本增加超过 20%；作者仍认可它表达非标准编码约定的用途。[Evaluating AGENTS.md，v2，2026-06-23](https://arxiv.org/abs/2602.11988v2)

另一项研究在 10 个仓库、124 个 PR 的实验中观察到更低的运行时间中位数和输出 token 消耗。[On the Impact of AGENTS.md Files，v2，2026-03-30](https://arxiv.org/abs/2601.20404v2)

本次仅核查两篇论文的版本和摘要结果，未复现实验；它们的任务和衡量指标不同。**综合建议：**用本项目常见任务检查规则能否减少误操作、返工和不必要执行，不能据此承诺“有 AGENTS.md 必然提效”，也不应据一项研究删除必要约束。

## 3. README：使用入口与导航

### 3.1 来源建议

GitHub 将 README 定位为访客首先接触的项目说明：介绍用途、价值、如何开始、如何求助以及维护者。内容应围绕开始使用和参与项目所需的信息；仓库内文件优先使用相对链接。[GitHub：About the repository README file](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes)

Google 的包级 README 指南补充了发布／弃用状态、可复制的使用示例和详细文档入口。它针对 Google 的代码浏览环境，不能据其目录要求推导出所有仓库都必须采用同样布局。[Google：READMEs](https://google.github.io/styleguide/docguide/READMEs.html)

Google 技术写作课程建议先明确读者、已有知识、阅读目标与范围，并在开头回答关键问题。对 README 的启发是先满足新使用者的第一项任务，再提供深入阅读路径。[Google：Documents](https://developers.google.com/tech-writing/one/documents)

### 3.2 推荐内容与顺序（综合建议）

| 内容 | 读者应能得到的答案 |
| --- | --- |
| 项目定位 | 解决什么问题，适合谁，当前可用范围是什么？ |
| 快速开始 | 需要什么环境，执行哪些步骤，怎样确认成功？ |
| 最小使用示例 | 如何完成一个真实且常见的任务？ |
| 文档导航 | API、配置、部署、架构和开发说明分别在哪里？ |
| 帮助与反馈 | 遇到问题在哪里查、向哪里反馈？ |
| 参与及许可 | 如何贡献，适用什么许可证？ |

顺序可随受众调整：库优先展示调用示例，服务优先展示启动和验证，内部组件优先说明接入条件及负责人。
这是对 [GitHub README 指南](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes)与 [Google README 指南](https://google.github.io/styleguide/docguide/READMEs.html)的应用，不是强制目录。

一个足够开始的骨架：

```markdown
# 项目名
一句话用途、目标使用者和当前状态。

## 快速开始
前置条件、最短操作步骤、成功判据。

## 使用示例
一个常见任务及预期结果。

## 文档
按任务链接到配置、API、部署、架构、贡献指南。

## 帮助与贡献
支持入口、问题反馈、贡献指南和许可证链接。
```

### 3.3 常见反模式（综合判断）

- 开头长篇介绍技术栈，却没有说明项目用途或第一次如何运行。
- 快速开始省略凭证、配置、迁移等必要前置步骤，或者缺少成功判据。
- 把全部配置、完整 API、架构推演和变更记录塞进入口，增加后续维护点。
- 只给文档列表而没有任务标签，读者仍需猜测该打开哪个文件。
- 宣传尚未交付的能力时没有注明状态，读者无法分清可用功能与计划。

这些是按 [Google 的读者与任务原则](https://developers.google.com/tech-writing/one/documents)作出的质量判断。没有从本次来源中得出适用于所有 README 的固定行数上限。

## 4. 技术方案：先确定文档的职责和生命周期

### 4.1 三种需要区分的内容

| 类型 | 主要问题 | 维护方式（综合建议） |
| --- | --- | --- |
| 提案／RFC／变更设计 | 为什么改、如何改、有哪些取舍？ | 评审时修改；决议后保留当时背景，链接实现和后续决定。 |
| 当前架构说明 | 系统现在怎样组织、有哪些边界与约束？ | 随已生效行为演进，明确适用版本及尚未支持的部分。 |
| ADR | 某个重要选择为什么这样定？ | 保留决策上下文、状态及后果；改变选择时建立替代关系。 |

**来源边界：**Google 的指南把设计提案用于征求反馈，并建议实现完成后将其作为决策档案。AWS 的 ADR 流程要求接受／拒绝后的记录保留历史，新的决定用新 ADR 取代旧记录。这不等于所有名为 `design.md` 的文件都应冻结。[Google：Documentation Best Practices](https://google.github.io/styleguide/docguide/best_practices.html)、[AWS：ADR process](https://docs.aws.amazon.com/prescriptive-guidance/latest/architectural-decision-records/adr-process.html)

arc42 则组织完整架构信息，包括目标、边界、结构、运行场景、部署、质量与风险。它明确允许裁剪，因此适合作为架构说明的检查清单，无需机械填满十二章。[arc42：Template Overview](https://arc42.org/overview/)

**综合建议：**在文件开头声明“当前支持的设计”“待评审提案”或“历史决定”，并标明适用范围。文件名本身不能表达这些状态。

### 4.2 一手模板能提供什么

Go 模板覆盖摘要、问题背景、精确改动、替代方案及取舍、兼容性、实施安排与未决问题。Go 流程允许先提交简短 issue，再按讨论需要补充设计文档，说明文档规模可以与问题复杂度匹配。[Go：Design template](https://go.googlesource.com/proposal/+/master/design/TEMPLATE.md)、[Go：Proposal process](https://go.googlesource.com/proposal/)

Rust RFC 模板强调用具体场景说明动机，先用示例解释使用体验，再写清技术规则、交互与边界情况；还单列缺点、替代方案、先例和未决问题。这有助于避免方案只描述内部实现而不解释使用者受到的影响。[Rust：RFC template](https://raw.githubusercontent.com/rust-lang/rfcs/master/0000-template.md)

AWS 要求 ADR 至少包含上下文、决定和后果，并明确负责人及状态。arc42 补充：重要选择需要记录理由，后果应包括正面、负面和中性影响；已有位置解释过的决定应引用，避免重复。[AWS：ADR process](https://docs.aws.amazon.com/prescriptive-guidance/latest/architectural-decision-records/adr-process.html)、[arc42：Architecture Decisions](https://docs.arc42.org/section-9/)

### 4.3 变更方案的推荐骨架（综合建议）

| 部分 | 应回答的问题 |
| --- | --- |
| 元信息与摘要 | 谁负责，当前状态，相关讨论，提议改变什么？ |
| 问题与目标 | 谁受到什么影响，成功后能获得什么结果？ |
| 范围与约束 | 本次边界在哪里，哪些既有承诺需要保持？ |
| 建议方案 | 模块职责、接口、数据流和关键流程如何变化？ |
| 正确性与失败行为 | 不变量是什么，超时、重试、并发、部分失败如何处理？ |
| 替代方案与代价 | 比较了哪些选择，为什么采用当前方案，代价由谁承担？ |
| 兼容、迁移与回滚 | 旧数据及旧客户端如何过渡，失败后如何恢复？ |
| 验证 | 用哪些可观察结果判断正确性、性能和可运维性？ |
| 未决问题 | 还有哪些未知，哪些必须在实施前解决？ |
| 决议与关联 | 评审结果在哪里，当前契约和实施工作在哪里？ |

该骨架综合 [Go 模板](https://go.googlesource.com/proposal/+/master/design/TEMPLATE.md)、[Rust 模板](https://raw.githubusercontent.com/rust-lang/rfcs/master/0000-template.md)及 [arc42](https://arc42.org/overview/)。失败行为、回滚和验收写到什么深度，应由变更风险决定。

简单内部改动可在 issue／PR 中回答必要问题；涉及持久化、公开契约、重要依赖或难以回滚的变更时，再展开完整方案。
这是粒度选择建议；[Go 流程](https://go.googlesource.com/proposal/)支持“按需要扩展提案”，并未规定所有工程都采用同一审批流程。

### 4.4 当前架构说明与 ADR 的最小内容（综合建议）

当前架构说明可以先覆盖：系统边界及外部依赖、模块职责、关键运行场景、部署关系、质量目标与已知风险，再链接重要决定。
这组选项取自 [arc42 的架构视角](https://arc42.org/overview/)，适用章节应按维护者需要保留。

单条 ADR 可采用：**标题／日期／状态／负责人 → 背景及约束 → 选择 → 理由及替代方案 → 后果 → 相关和替代记录**。
它聚焦一个值得长期记住的决定，不必复制整个实现方案。[AWS：ADR process](https://docs.aws.amazon.com/prescriptive-guidance/latest/architectural-decision-records/adr-process.html)、[arc42：Architecture Decisions](https://docs.arc42.org/section-9/)

### 4.5 常见反模式（综合判断）

- 用目录树和技术名词代替模块职责、调用关系及关键行为。
- 只介绍选中方案，没有比较依据和不利后果。
- 用“最终一致”“高可用”等描述替代可验证的失败条件和恢复行为。
- 把“已接受”当成“已实现”：Go 的 Accepted 阶段之后仍会跟踪实施工作。[Go：Accepted](https://go.googlesource.com/proposal/#accepted)
- 直接覆盖旧决定，丢失原来的限制条件和否决理由；AWS 建议保留旧 ADR 及替代关系。[AWS：Best practices](https://docs.aws.amazon.com/prescriptive-guidance/latest/architectural-decision-records/best-practices.html)

## 5. 避免重复与漂移

### 5.1 来源建议

Google 提倡少量、准确、持续修整的文档，文档与代码在同一次变更中更新，移除失效内容，公共技术和流程通过链接复用。[Google：Documentation Best Practices](https://google.github.io/styleguide/docguide/best_practices.html)

Diátaxis 按读者需要区分教程、操作指南、参考和解释。参考适合工作时查阅的事实；解释帮助理解原因和关系，混写会妨碍两者的用途。[Diátaxis：Overview](https://diataxis.fr/)、[Reference and explanation](https://diataxis.fr/reference-explanation/)

AWS 建议将 ADR 集中存放、从文档主页提供入口，维护负责人及历史。[AWS：Best practices](https://docs.aws.amazon.com/prescriptive-guidance/latest/architectural-decision-records/best-practices.html)

### 5.2 按事实确定归属（综合建议）

| 信息 | 适合的唯一完整定义位置 | 其他位置如何使用 |
| --- | --- | --- |
| 最短启动路径 | README 或专门快速开始页面，择一完整维护 | 用必要摘要和入口链接引导。 |
| 完整 API、配置、Schema | 对应参考文档或可生成它们的源文件 | 只引用本段需要的行为及准确链接。 |
| 系统边界、机制与约束 | 当前架构说明 | README 简要介绍；方案引用既有约束。 |
| 一次变更的比较与论证 | 提案／RFC | 当前说明链接其必要理由。 |
| 重要选择的决议历史 | ADR 或已有决策章节，择一 | 建立索引及替代关系。 |
| 构建、测试与贡献流程 | 开发指南 | 入口文档链接到实际执行位置。 |
| 代理必须遵循的工作规则 | AGENTS.md | 引用已有流程和契约，保留触发条件与必要动作。 |

这里的“唯一”指维护完整定义的位置；允许为不同读者提供短摘要和必要示例。它是对 [Google 避免重复](https://google.github.io/styleguide/docguide/best_practices.html)与 [Diátaxis 内容分工](https://diataxis.fr/)的综合应用，不要求立即增加更多文件。

### 5.3 低维护成本的检查（综合建议）

1. 修改行为时定位对应契约文档，随代码一起审阅；修正文档不能掩盖未经确认的行为改变。
2. 新增段落前确认是否已有完整定义；有则引用，无则明确归属。
3. 对能执行的快速开始和示例做适量验证，对链接和生成参考做自动检查。
4. 评审时区分计划、当前承诺和历史决定；让已过期记录指向现行说明。
5. 精简时优先删除重复和失效内容，保留影响使用和维护的边界条件及理由。

这些检查将 [Google 的同步更新原则](https://google.github.io/styleguide/docguide/best_practices.html)、[AWS 的决策历史原则](https://docs.aws.amazon.com/prescriptive-guidance/latest/architectural-decision-records/best-practices.html)转化为仓库工作流；自动检查是本次综合建议。

## 6. 对 Nexo 的文档观察与建议

以下基于 2026-09-09 当前工作区文档的阅读，包含尚未提交的内容；这是文档观察，不代表代码与文档一致性审计。

现有 [文档归属规则](../AGENTS.md#documentation)已经区分架构、协议、宿主操作、压测验收、开发流程和研究证据。建议沿用这些边界，按实际阅读问题调整内容。

| 位置 | 已有做法 | 可考虑的改进 |
| --- | --- | --- |
| [AGENTS.md](../AGENTS.md) | 生成规则、分层约束、时间精度、测试前提都能影响具体实现动作。 | 把 Architecture 中的长段按独立约束拆成易读条目；逐项判断哪些细节可改为“触发条件＋设计章节链接”，保留容易破坏正确性的规则。无需仅按行数删减。 |
| [README](../README.md#quick-start) | 有启动、首条消息、集群示例和按任务组织的导航。 | 在最短路径前显式列出 Go 1.27、可连接的数据库等前提；消息示例注明 jq 依赖；补充启动及健康检查的成功判据。完整开发命令继续由 CONTRIBUTING 维护。 |
| [design.md](design.md) | 开头声明 v3.2 约束，记录事务、故障窗口、可见性和必要理由。 | 按现行架构契约持续维护；引用迁移、接口和配置源文件时保留语义解释，尤其是字段之间的约束及跨步骤保证。 |
| [sync-design.md](sync-design.md) | 已注明未实现草案，区分本轮范围、暂缓项和验收条件。 | 决议和实现状态分别表达；将来落地时同步更新对应现行契约及入口中的草案提示。 |
| [sync-research.md](sync-research.md) | 固定研究版本，保留实验环境和未验证边界。 | 继续将历史测量与当前支持承诺分开；方案引用结论及证据位置即可。 |

本项目已经在设计文档保存关键理由。只有当具体决定需要独立评审、长期追溯或多次替代时，才考虑单独的 ADR；如已有章节足够，应引用现有记录。无需为了套模板预建 `adr/`、`rfc/` 等目录。这是基于现有组织和 [arc42 避免重复决策记录原则](https://docs.arc42.org/section-9/)的应用建议。

## 7. 研究边界

- 本次只使用并打开了指南发布者或项目自身的一手页面；没有将搜索摘要或第三方模板汇总当作依据。
- 不同来源服务于不同组织及文档类型，不能把其特定目录、发布流程或审批习惯直接提升为普遍要求。
- 这些来源提供实践方法，不能据此证明某个字数、目录层级或模板能提高任意项目的研发效率。
- 本研究读取了项目入口与主要设计内容，未审计全部文档与代码的一致性，也没有执行快速开始命令；上述项目建议尚未改入现有契约文档。
