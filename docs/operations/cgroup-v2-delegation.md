# 旧 cgroup v2 委派方案与迁移说明

最后更新：2026-10-12

## 当前运行模型

平台在一个非 root Docker/Rancher 容器内管理多个 ape-dts 子进程。任务资源是平台容量预算，不是每任务 CPU、内存硬限额；容器总体硬限额由 Docker/Kubernetes 管理，包含平台自身及全部任务进程。

当前部署不要求宿主统一 cgroup v2、systemd `Delegate`、`cgroup-parent`、host cgroup namespace 或可写 cgroup 挂载，也不需要 privileged、Docker Socket 或启动任务容器的权限。宿主采用 cgroup v1 并不因此阻断任务；Docker 能否执行容器总体限额仍应在实际宿主验证。

现行启动命令、环境变量、卷权限和验证流程见 [Linux 服务器 Docker 部署](./docker-server-deployment.md)。容量语义见 [容量包络](./capacity-envelope.md)。

## 旧部署迁移

旧方案通过 `SCS_RUNTIME_CGROUP_ROOT`、`SCS_RUNTIME_RUN_CPU_MAX`、`SCS_RUNTIME_RUN_MEMORY_MAX_BYTES` 为每个 run 设置子 cgroup。该方案不再是当前任务启动前提；上述变量不应继续作为当前资源额度配置。不要把旧的每 run 4C/8GiB 文案理解成共享容器仍提供同样的任务级硬隔离。

升级时：

1. 在旧平台中正常停止全部同步任务，确认运行进程已经退出；保存运行记录，备份平台数据库及配套 `/data/storage`。
2. 停止旧平台容器。保留已有持久化卷、数据库、Slot、Publication 和 Kafka Topic，不能为迁移而删除这些业务资源。
3. 按现行部署文档重建容器，移除三个旧 cgroup 环境变量，以及 `cgroup: host`、`cgroup_parent`、`--cgroupns=host`、`--cgroup-parent` 和 run 子树挂载。明确设置容器总体 CPU、内存限额。
4. 确认 `READY`、数据源连接、任务预检、启停、日志与指标，再逐步恢复任务；已有任务的容量预算保留，不会变成操作系统限额。

旧的 `scs-platform-cgroup.service`、slice 和宿主准备脚本不是新部署所需组件。**不要在任务运行时停止这些旧服务，也不要直接删除宿主 cgroup 目录。** 如需清理，先核实旧容器、所有残留任务/helper 进程及其他工作负载均不再依赖它，再由部署管理员单独安排；平台升级不会擅自清理宿主资源。

## 风险边界

所有任务共享容器额度。CPU 争用会影响吞吐与延迟；某个任务内存增长可能触发容器 OOM，影响其他任务乃至平台进程。容量预约只限制准入数量，不强制限制单任务实际 CPU/内存消耗。

若客户要求任务级硬隔离，需要重新确定独立容器/Pod 的部署边界与受限编排权限，不能靠扩大当前容器权限或静默恢复旧委派方案实现。当前版本不提供自动切换隔离模式。
