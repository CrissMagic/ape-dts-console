# Roadmap

以 1.0.0 的实际能力为起点，按链路交付。以下为规划顺序，不代表日期承诺。中文为默认语言，欢迎英文讨论。

## 已交付 / Available in 1.0.0

- PostgreSQL → Kafka、MySQL → Kafka、Kafka → Kafka Raw。
- 多来源映射到多个 Topic、任务卡片、任务组与拖拽排序。
- 数据源管理与授权、运行制品管理、预检、独立子进程启动与检查点恢复。
- 运行日志、历史运行与最长 7 天指标、企业微信告警和可配置恢复策略。
- 配置导入导出（不导出密码）、审计、登录与会话管理、明暗主题。
- Docker/Rancher 单容器部署，不要求平台管理 Docker 或 cgroup 委派。

“已交付”指平台接入，不代表所有源库版本、制品和运行环境均已验证。Doris 目前只有数据源与元数据功能，尚无同步任务。

## 1.0.1：易用性与诊断

| 顺序 | 工作 | 跟踪 |
| --- | --- | --- |
| 1 | 数据源服务端分页、搜索、类型筛选，保持授权与任务编辑回显 | [#1](https://github.com/CrissMagic/ape-dts-console/issues/1) |
| 2 | 统一失败上下文、恢复尝试与告警投递诊断，脱敏诊断包 | [#2](https://github.com/CrissMagic/ape-dts-console/issues/2) |
| 3 | 多表任务的真实表级阶段与进度 | [#3](https://github.com/CrissMagic/ape-dts-console/issues/3) |
| 4 | 新增/移除表与 Topic 映射变更影响预览 | [#4](https://github.com/CrissMagic/ape-dts-console/issues/4) |
| 基线 | 中英文 README、协议、社区文档与 CI | [#16](https://github.com/CrissMagic/ape-dts-console/issues/16) |

## 1.1.0：Doris 链路

优先接入 PostgreSQL/MySQL → Doris，明确目标表模型、类型映射、删除/更新、写入确认和恢复语义。完整覆盖全量转增量及故障恢复后再标注支持。跟踪 [#6](https://github.com/CrissMagic/ape-dts-console/issues/6)。

## 后续能力 / Backlog

| 优先级 | 工作 | 跟踪 |
| --- | --- | --- |
| 高 | 结构变化检测与兼容/不兼容变更策略 | [#5](https://github.com/CrissMagic/ape-dts-console/issues/5) |
| 中 | StarRocks、ClickHouse 目标，按具体链路验证 | [#7](https://github.com/CrissMagic/ape-dts-console/issues/7) |
| 中 | MySQL → MySQL、PostgreSQL → PostgreSQL | [#8](https://github.com/CrissMagic/ape-dts-console/issues/8) |
| 中 | MongoDB、Redis、TiDB 的具体引擎链路评估 | [#9](https://github.com/CrissMagic/ape-dts-console/issues/9) |
| 中 | 字段映射、过滤与轻量转换及样例预览 | [#10](https://github.com/CrissMagic/ape-dts-console/issues/10) |
| 中 | 数据校验、差异预览与显式订正 | [#11](https://github.com/CrissMagic/ape-dts-console/issues/11) |
| 中 | 任务模板与逐项可见的批量操作 | [#12](https://github.com/CrissMagic/ape-dts-console/issues/12) |
| 中 | 离线交付、自检、备份恢复和升级演练 | [#13](https://github.com/CrissMagic/ape-dts-console/issues/13) |
| 后续 | 真实容量基准之后评估多个执行节点 | [#14](https://github.com/CrissMagic/ape-dts-console/issues/14) |
| 后续 | 指定新增表的历史补采及快照/CDC 顺序处理 | [#15](https://github.com/CrissMagic/ape-dts-console/issues/15) |

## 实施边界

- 新增链路先核对具体引擎制品，再接通向导、预检、启动、监控、恢复和操作文档，不只新增下拉选项。
- 继续保留当前 PG 新增表原槽续传、不补历史的默认语义；按需补采是独立能力。
- 不静默清空目标数据、复制槽或检查点，不以零代替未知指标。
- 不将至少一次语义宣称为端到端精确一次。涉及幂等、事务和部分失败时明确验证边界。
- 多节点能力以实际容量与故障隔离需求为前提，不提前替换现有可部署架构。
- GitHub Issues 记录验收范围与状态；未关闭的规划项不能写成已支持。
