# Phase 0 制品认证台账

最后更新：2026-09-10

## 原候选认证对象（历史 SHA，未覆盖）

| 字段 | 值 |
| --- | --- |
| 候选制品 | `ape-dts-2.0.26-centos7-x86_64.tar.gz` |
| SHA-256 | `b63cd1b0feb66debddc3443de1c950d7a45e16faf0d2c92439d2e6b43f2413ab` |
| 实际版本输出 | `dt-main 2.0.26` |
| 适配器 | `ape-dts-ini@1` |
| 测试运行时 | `centos:7`、linux/amd64 |
| 同步逻辑任务总配额 | 4 CPU、8 GiB 内存 |
| 外部依赖 | PostgreSQL 12.14（`wal_level=logical`）与 Kafka 3.8.0；不计入同步运行单元配额 |

本台账只认证上述 SHA-256 在当前平台工作树、当前适配器和本地 Docker 环境中的行为。它不能外推到同版本号的其他文件、其他编译参数或其他部署环境。

表中的 4 CPU / 8 GiB 是候选场景的历史测试约束，不证明生产并发容量或每任务硬隔离。当前平台使用单容器多子进程，共享容器总体 CPU、内存限额，不再要求 cgroup v2 委派；现行容量语义与验证要求见 [容量包络](./capacity-envelope.md)。

## 原候选结论

该制品当前为 **INACTIVE / 不可激活**。制品执行门中的 CDC 优雅停止失败，且 PG 目标 Topic 名含点号时仍会 panic；不得通过管理员操作、UI 或默认配置绕过任一状态。平台已能读取 Kafka Source Topic ID，但任务预检与运行时身份变更阻断链路尚未实现，不能据此提前开放任务创建。

| 门与场景 | 状态 | 已验证事实 | 证据 |
| --- | --- | --- | --- |
| 制品可执行、动态库、4C/8G | PASSED | 在 CentOS 7 中 `--version` 返回 `dt-main 2.0.26`；`ldd` 无缺失库；Docker cgroup 为 4 CPU / 8 GiB | `tests/integration/phase0/artifact-behavior.integration.spec.ts` |
| 平台生产 Docker clean build/smoke | PASSED | 使用本地 `node:24.19.0-bookworm-slim` 完成无缓存构建；workspace 包、API deploy、固定非 root 用户、liveness/readiness、迁移文件和 SIGTERM 退出检查均通过 | `Dockerfile`，`tests/integration/docker-smoke.integration.spec.ts`（1/1） |
| PG Snapshot 基线 | PASSED | 三条预置行通过真实 PG、候选二进制和 Kafka 写入三个连续 offset | `tests/integration/phase0/pg-snapshot-cdc.integration.spec.ts` |
| PG Snapshot → CDC 并发切换 | PASSED（至少一次） | Slot 在 Snapshot 前创建；Snapshot 已输出后执行 INSERT/UPDATE/DELETE，CDC 从同一 Slot 输出三个对应操作码。该次 Snapshot 产生 10,001 条而初始行数为 10,000，切换重复已观察到且属于首期已确认的至少一次语义 | 同上，`keeps every change...` 场景 |
| PG CDC 数据传输 | PASSED | 预建 Publication/Slot 后，INSERT/UPDATE/DELETE 都写入 Kafka 三个连续 offset | 同上，`captures PostgreSQL...` 场景的停止前断言 |
| PG CDC SIGTERM | FAILED | 候选日志确认收到 SIGTERM 并停止 HTTP 接收线程，但 CDC pipeline 在 15 秒窗口持续运行；Docker 最终以 `137` 强杀 | 同上，`captures PostgreSQL...` 场景 |
| Kafka Raw 单分区 | PASSED（局部） | 新 Group 从 latest 起步，跳过历史消息；保留 headers、null key、null payload；确认 offset 后强制重启从已提交位置继续 | `tests/integration/phase0/kafka-raw.integration.spec.ts` |
| Kafka Raw 三分区启动集 | PASSED（局部） | 对三分区 Source 同时启动三个独立候选进程并共用一个 Group；三条历史消息均从 latest 跳过，六条实时消息全部到达 Target，三个 Source Partition 的已提交 offset 均追至 Log End。每个候选进程配置为 1.33 CPU / 2730 MiB，总配置约 3.99 CPU / 8190 MiB | `tests/integration/phase0/kafka-raw.integration.spec.ts`，`forwards live records from every source partition...` 场景 |
| Kafka Raw Source Topic ID 基线（平台客户端） | PASSED | Kafka 3.8 的 `cluster.id` 可读取；平台通过 Metadata API v10 读取 Broker 返回的真实 Topic UUID，返回值与 Kafka CLI 的 `TopicId` 一致，同一 Topic 连续读取的 ID 稳定，同集群同名 Topic 删除重建后的 ID 变化。Broker 不支持 v10 或返回零 UUID 时明确返回 `METADATA_UNSUPPORTED`，不按名称或地址兜底 | `tests/integration/data-sources/kafka-data-source.adapter.integration.spec.ts`，`reads a stable Topic ID...` 场景 |
| Kafka Raw Source Partition 增加 | PASSED（阻断性观察） | 三个固定 Partition 运行单元启动后将 Source 从 3 扩至 4；以 key 定向写入的新记录仅进入 Partition 3，三秒观察窗口内 Target 无记录。候选不会自动转发新增 Partition；平台尚未实现检测、停止、重新预检和人工确认链路，因此不得将该结果解释为扩容可用 | `tests/integration/phase0/kafka-raw.integration.spec.ts`，`does not forward a record...` 场景 |
| Kafka Raw Target Partition 增加（重启后） | PASSED（映射变化已观察） | Target 从 3 扩至 4 后重启候选进程，再次写入相同的 12 个 key，至少一个 key 落入了不同 Target Partition。该场景未验证长驻进程的元数据刷新时机；平台仍须检测 Target 变化、产生高优先级告警并在重新预检时要求确认，不能声称具体分区映射稳定 | `tests/integration/phase0/kafka-raw.integration.spec.ts`，`changes at least one keyed Target partition...` 场景 |
| Kafka Raw 其余变更矩阵 | UNTESTED | 平台元数据层已观察到 Source 同名重建后的 Topic ID 变化；任务预检/运行时尚未覆盖 Source/Target Topic ID 变化后的永久阻断、cluster ID 变化、所有分区重启后的重复窗口和长驻候选进程遇到 Target Partition 变化时的元数据刷新 | 待补 Phase 0 测试 |
| Topic 名含点号的 PG 目标 | FAILED | `topic_map` 目标为 `phase0.pg.snapshot` 时，候选在路由层发生 `index out of bounds: len is 5 but index is 5` panic | `tests/integration/phase0/pg-snapshot-cdc.integration.spec.ts`，`records the candidate panic...` 场景 |
| console-only 日志正常路径 | PASSED（局部） | Kafka Raw 正常转发和强制重启路径中，候选就绪日志出现在容器 stdout/stderr，挂载的运行目录 `logs` 保持为空 | `tests/integration/phase0/kafka-raw.integration.spec.ts`，`forwards live records...` 场景 |
| 日志洪泛、错误路径旁路与持久化缺口 | UNTESTED | 平台有界脱敏日志流水线、LOCAL 持久化、容量滚动、故障缓冲、缺口记录和手动重试已经实现并通过平台侧非契约测试；但正常路径无文件输出及平台测试不能证明真实候选制品在洪泛和错误路径下不存在 appender 旁路，也不能替代目标部署环境中的真实存储故障与缺口压力认证 | 待补真实制品与目标部署环境认证 |
| PG Metrics 写入、聚合与保留基准 | UNTESTED | Unit 9 的生产表和聚合流程尚未实现 | 待 Unit 4/9 |

## 强制约束

- 2026-09-10 用户已授权修改 ape-dts 源码并重新打包，范围为 CDC 优雅停止与点号 Topic 修复。旧候选及其 SHA-256 认证记录保留；新制品使用独立路径与新 SHA-256 重新认证，不覆盖旧包或继承旧认证。
- 不把 PG CDC 的 `137` 退出解释为成功停止，不增加无限等待、静默重试或放宽退出码断言。
- 不把 Kafka Raw 的单分区、三分区启动集或新增 Partition 阻断性观察外推为扩容处理能力、Topic/cluster 身份变化、全部故障矩阵或生产容量结论。
- Source Topic ID 只接受 Metadata API v10 返回的真实 UUID；Broker 不支持或不返回时必须明确失败，仍不得按 Topic 名或 bootstrap servers 字符串替代身份。任务预检与运行时身份阻断链路完成前，不得创建首期 Kafka Source 任务。
- 在任务选择能力实现前，必须把本 SHA-256 保持为不可选；尤其不得生成 PG 目标 Topic 含点号的运行配置。
- 所有测试均使用隔离 Compose 项目并在结束时删除测试容器与 volume，不操作既有 PostgreSQL/Kafka 资源。

## 复验

2026-09-10 局部复验：PG 4 项中 3 通过、1 失败（CDC 仍以 `137` 退出；点号 Topic panic 再次复现）；候选执行、Kafka Raw 和 Topic ID 共 7 项通过。另完成最终 Debian 镜像非 root 加载/动态库检查，以及 Bootstrap/迁移 12 项回归。普通 Docker 容器的 cgroup 为只读且缺少委派根，资源隔离仍未通过部署认证。详见 本次复验记录与边界。以下 2026-09-03 全量结果保留为历史记录，不把本次局部复验声称为全量重跑。

2026-09-03 使用上述制品重跑全部 Phase 0 场景：5 个测试文件共 12 个测试，11 项通过、1 项失败；失败仍为 PG CDC 收到 SIGTERM 后未在 15 秒窗口内退出，最终 Docker 以 `137` 强杀。该结果不改变制品 `INACTIVE` / No-Go 结论。

PowerShell：

```powershell
$env:SCS_PHASE0_APE_DTS_ARCHIVE = '<ape-dts-source>\dist\ape-dts-2.0.26-centos7-x86_64.tar.gz'
corepack pnpm@10.12.4 run test:integration:phase0
```

源码修复授权后，点号 Topic 测试已改为正向写入验收，并补充 CDC 停止后恢复对账。因此旧包执行当前测试时，CDC 停止与点号 Topic 都应失败；上方历史测试数量仅对应当时的测试快照。新候选进展见 源码修复与复验，不得以修改断言或继承旧证据放行新包。

## 新候选独立复验（2026-09-10）

新 SHA-256：`ba8ae732eb248a810179167ff78391a0a423fc7f95e2d617f5940e5547b71ebe`，独立目录 `<ape-dts-source>\dist\20260910-shutdown-topic-fix`，包内版本仍为 `dt-main 2.0.26`。

- Rust 相关测试累计 91 项通过；标准优化 release 打包成功。
- 完整 Phase 0：5 个文件 12 项全部通过，包括 CDC SIGTERM 两次退出 0、checkpoint 恢复后 100 条尾部数据无遗漏、点号 Topic 实际写入成功。
- 最终 Debian 平台镜像：默认 UID/GID 10001 的版本/动态库检查通过；真实 PG 四场景 4/4 通过。
- 全量测试有一条 Node 负定时值警告，未导致失败，来源尚未定位；最终镜像 PG 复跑未出现该警告。
- 仅解决上述两项候选缺陷并取得本地回归证据。未执行自动激活、提交、推送或部署；单容器多 run 子 cgroup 委派、真实容量、日志压力及其他未验证运行门仍不能由这些结果替代。

源码、制品身份、命令与适用边界详见 新候选修复与复验记录。上方旧 SHA 的失败结论不修改为通过。
