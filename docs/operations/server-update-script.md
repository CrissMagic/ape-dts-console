# 服务器一键更新（直接 Docker 部署）

脚本：`tools/deployment/update-server.sh`。这是独立 Bash 脚本，不依赖 Python。适用于 Linux 本机 Docker、现有 master 代码仓库、单个平台容器和 bind 挂载；不用于 Rancher 或 Compose 管理的工作负载。

## 使用

首次使用先获取包含脚本的代码。在仓库根目录执行：

```sh
git pull --ff-only origin master
sudo bash tools/deployment/update-server.sh
```

后续更新只需最后一条命令。依赖 Bash 4+、Git、Docker、GNU tar、flock 和本机 `/var/run/docker.sock`；不需要安装 jq、Python、Node、pnpm 或宿主 pg_dump，也不需要修改 CentOS 软件源。JSON 解析使用 `node:24.19.0-bookworm-slim` 中的 Node.js，辅助容器结束后自动删除。脚本要求 root，且代码目录无未提交改动、无本地未推送提交。

包含登录与会话配置的版本需要执行数据库迁移 28，仍使用本更新脚本，无新增 env 或挂载参数。升级后管理员可在“系统管理 → 登录与会话”调整时长；退出并重新登录后采用新策略，已有会话不会因升级被延长。

配置导入导出功能需要迁移 29，用于标记导入后尚未配置密码的数据源；普通数据源的密码约束保持不变。仍使用本更新脚本完成备份、迁移和重启，不新增 env 或挂载参数。功能范围和迁移步骤见[配置导入导出](config-transfer.md)。

任务组拖拽排序需要迁移 30，将既有任务组按原显示顺序初始化排序值。分组重命名、删除不改同步配置或任务运行状态；删除仅解除归组，任务继续保留。迁移仍由本脚本执行。

若本次版本包含更新脚本自身的修改，应先手动执行 `git pull --ff-only origin master`，再执行脚本。脚本内部拉取代码不会替换当前 Bash 进程已经加载的函数；先拉取才能让本次部署直接使用新版诊断和失败处理。

Git 调用通过子 Shell 切换到仓库目录执行，不依赖旧版 Git 不支持的 `git -C`。Git 命令执行失败会单独报错，不会误判为分支错误或工作区干净；拉取后使用本次 `FETCH_HEAD` 核对版本，不依赖旧版 Git 对远端跟踪引用的更新行为。

默认平台容器名 `scs-data-sync-platform`，备份目录 `/srv/scs-data-sync-platform/backups`。平台数据库为 PG12 时无需增加参数；若平台数据库不是 PG12，务必指定适配的客户端镜像，例如：

```sh
sudo bash tools/deployment/update-server.sh \
  --repo /opt/scs-data-sync-platform \
  --container scs-data-sync-platform \
  --backup-dir /srv/scs-data-sync-platform/backups \
  --pg-client-image postgres:16-bookworm \
  --ready-timeout 300
```

默认继承旧容器端口。需要将平台 HTTP 宿主端口改成 18088 时执行 `sudo bash tools/deployment/update-server.sh --http-port 18088`；原监听 IP 和其他端口映射保持不变，后续更新无需重复传入。未发布过 HTTP 端口的容器默认只发布到 `127.0.0.1`。该选项不适用于 host/none 网络模式；目标宿主端口需事先确认未占用。

客户端版本不能低于平台数据库服务端版本。首次运行会拉取客户端镜像；离线部署需事先导入 `node:24.19.0-bookworm-slim` 和对应 PostgreSQL 客户端镜像。

更新成功进入 READY 后，默认执行 `docker image rm` 清理本项目过期镜像。需要保留旧镜像供离线恢复时，加 `--keep-old-images`；普通更新及 `--resume-backup` 续跑都支持该参数。

## 执行顺序

1. 校验部署边界，拉取 master（仅 fast-forward），使用提交 SHA 构建新镜像；此时旧容器仍在运行。
2. 停止原平台容器，关闭其自动重启，备份持久化 storage 和整个**平台数据库**。
3. 使用原容器网络及持久化配置中的数据库凭据运行 pg_dump；校验归档可读取。不会备份业务 PG 或 Kafka。
4. 保留已停止的旧容器，使用新镜像和原环境、挂载、网络、CPU/内存限额执行 `upgrade --backup-confirmed`。升级命令先获取数据库控制锁；旧连接暂未释放时，每秒重试，等待窗口为 60 秒（数据库连接和查询自身超时另计）。成功获取后才检查账号并执行升级。
5. 重建同名平台容器；继承新镜像入口、命令及健康检查，更新 `SCS_PLATFORM_VERSION`。
6. 在容器内检查 `/api/health/ready`，不依赖 SSH 隧道。成功后移除本次旧平台容器；完整备份已完成时，默认清理符合下述边界的旧镜像。维护及 PG 客户端容器自动清理。

镜像清理只处理精确仓库名 `scs-data-sync-platform` 的镜像，以及更新脚本构建并标记 `io.scs-data-sync-platform.image=true` 的镜像。构建前记录本项目镜像 ID 并随备份保留，覆盖同 tag 重建后变为 dangling 的旧镜像；续跑也读取这份记录。旧备份没有该记录时，仍识别当前本项目标签、归属标记及备份原容器使用的本项目镜像。无法证明归属的历史 dangling 镜像保留。

当前活动镜像、被任意运行中或已停止容器引用的镜像，以及带其他仓库 tag 或 digest 的镜像都会保留。脚本只移除本次成功更新替换的旧容器，不批量移除其他 `previous` 容器；被保留容器引用的旧镜像不会强删，以免影响失败更新的诊断和续跑。删除镜像时带 `--no-prune`，避免连带删除未打 tag 的父镜像；不执行全局 prune、强制镜像删除、卷删除或业务数据清理。Docker 清理失败会报告警告，已经 READY 的更新仍视为成功。

为保留原 HostConfig，脚本通过短生命周期 Node 辅助容器访问本机 Docker Socket，复用新镜像中的 Node，无需安装新版 curl。只有创建容器的辅助进程以 root 挂载 Docker Socket，并在退出后自动删除；纯 JSON 解析辅助容器不挂载 Socket 或宿主配置目录，通过标准输入读取 JSON，使用 `none` 日志驱动，避免将含凭据的输出写入 Docker 日志。正式平台容器仍保留原用户身份，不挂载 Socket。不要使用 `bash -x` 输出敏感部署信息。

有持续运行的任务时，升级会中断任务进程，重启后按平台既有流程恢复；READY 仅代表平台可用，不代表所有任务恢复成功。若不希望自动恢复，更新前先在页面正常停止任务。维护窗口内不要运行第二个平台实例或其他数据库迁移工具。

Docker `json-file` / `local` 日志统一设置为每文件 10 MiB、最多 3 个；远程日志驱动保持不变，应由对应日志系统管理保留策略。原环境变量直接从旧容器读取，无需重新填写 env 文件；下次修改 env 文件不会自动覆盖旧容器配置，需要另行按部署命令重建。

## 备份与失败处理

每次更新创建独立的时间戳备份目录，权限 `0700`，文件默认 `0600`：

- `platform.dump`：平台数据库 custom-format 备份（不含 owner/ACL；恢复时需使用原数据库所有者）。
- `storage.tar.gz`：storage 完整备份，包含平台密钥、制品和任务日志。
- `old-container.json`：旧镜像 ID、环境变量、挂载、网络、资源和日志配置，含敏感信息。
- `new-version.txt`：新提交及镜像标签。
- `project-images.txt`：默认清理开启时记录构建前本项目镜像 ID，供成功后及续跑后的清理识别；不是镜像归档。
- `backup-error.log`、`upgrade.log`、失败时的 `startup.log`：备份、升级和启动诊断。
- `backup-complete`、`migration-attempted`、`update-complete`：已到达阶段。

尚未启动升级命令时失败：原容器重新启动，不删除数据。升级命令已启动后，只有明确的控制锁冲突可以确认尚未调用迁移；此时旧容器保持停止，提示排查锁后续跑，不自动启动可能与其他实例冲突的旧平台。其他失败（即使迁移可能已回滚）不自动连接旧镜像。新容器就绪失败时也会停止并关闭其自动重启；查阅上述日志再处理。`migration-attempted` 仅表示启动过升级命令，不证明数据库已发生变更。

需要回滚时，应停止所有平台实例，配套恢复平台数据库及 storage，再按 `old-container.json` 中的旧镜像和配置启动；不要仅启动旧镜像。脚本不自动执行数据库恢复或删除备份。默认成功清理后，旧镜像 ID 可能已不在本机，恢复时须重新导入对应旧镜像或按原版本重建；备份文件不包含镜像内容。需要保留本机旧镜像时，更新时使用 `--keep-old-images`。更新或迁移失败不会触发镜像清理，恢复所需旧镜像和停止容器继续保留。定期将确认过的备份转存并人工清理，避免备份本身持续占用空间。

### 升级前控制锁冲突后的续跑

控制锁等待超时后，终端会直接显示诊断，`upgrade.log` 同时保留 `PLATFORM_CONTROLLER_LOCK_TIMEOUT` JSON 行和 `STOP_PLATFORM_BEFORE_SECURITY_MIGRATION`。诊断列出当前元数据库这把锁的最多 10 条持有连接：PID、数据库用户、应用名、来源 IP/端口、东八区连接建立时间及状态，不输出完整 SQL 或凭据。统计不可读取时 `diagnosticsAvailable=false`，字段也可能因统计权限不足为空；不把空结果认定为连接已安全清理。脚本不会自动终止连接、修改任务状态或重置位点。

新版元数据库连接的应用名为 `scs-data-sync-platform/<主机名或容器 hostname>/<进程 PID>`，用于区分本机开发服务、正式平台容器和升级维护容器。容器内 PID 常为 1，须同时核对 hostname、来源地址及连接建立时间；来源地址可能经过 NAT，不能仅凭 IP 判断实例归属。旧连接仍显示不带后缀的应用名。

持锁专用连接每 10 秒独立探活，不依赖浏览器访问；管理接口也检查这条连接，而非只检查连接池中的其他连接。连接异常或探活失败后销毁旧连接，并通过既有元数据库降级、重新获取锁及恢复核验流程处理。正常停机会解锁；解锁失败的连接同样销毁，不放回连接池。

平台同时为新建的元数据库连接设置会话级 TCP 检测参数：`tcp_keepalives_idle=10`、`tcp_keepalives_interval=5`、`tcp_keepalives_count=3`、`tcp_user_timeout=15000`（毫秒）。这些参数用于缩短容器断网、断电后 PG 保留失联连接的时间，通常为几十秒；实际回收时间受操作系统和网络影响，不承诺严格在 60 秒内完成。不需要修改 `postgresql.conf`、Docker 权限或额外环境变量。参数只作用于平台新连接，不改变业务数据库、其他服务的连接或现有旧会话。

健康实例会持续持锁，即使 `pg_stat_activity.state` 显示 `idle`，也不会因闲置而被升级脚本踢下线。这类冲突仍需先停止实际持锁的平台实例，不能用取消锁校验或延长等待代替。旧版本留下的失联会话也不会被新版自动终止；首次升级仍可能需要数据库管理员精确核对后清理一次。

新格式的上述两行日志，以及旧版本仅含 `STOP_PLATFORM_BEFORE_SECURITY_MIGRATION` 的日志，都能确认本次尚未调用迁移。其他错误或混合日志不放行。先核实持锁来源、停止其他实际运行的平台；如确认残留连接，交由数据库管理员按 PID 和连接建立时间精确核对后处理，再运行：

```sh
git pull --ff-only origin master
sudo bash tools/deployment/update-server.sh \
  --resume-backup /srv/scs-data-sync-platform/backups/<本次备份目录> \
  --http-port 18088
```

续跑只复用该目录记录的已构建镜像和完整备份，不重新启动旧版本、不重新构建镜像或覆盖原备份。脚本核对原容器 ID 和停止状态，要求目标容器尚不存在，并保留上次升级日志；重新执行时仍须获取控制锁。成功进入 READY 后才移除旧容器。若上次错误不是这个明确的升级前诊断，或更新已经完成，脚本拒绝续跑，不猜测数据库状态。

等待和诊断逻辑位于新版镜像的升级命令中；续跑旧备份不会替换备份绑定的旧镜像，因此旧镜像仍可能立即报锁冲突。不要手工修改备份版本文件来换镜像。

storage 必须是可写 bind 挂载；缓存不备份，可以重新填充。脚本会拒绝 named/anonymous volume、额外可写持久化目录、多网络、固定容器 IP、覆盖镜像启动命令及共享其他容器网络等未覆盖场景，不静默丢失配置。自定义只读挂载、网络解析、限额和安全配置保持原值；Docker Engine 必须支持原来的 HostConfig。

无法捕获断电或 SIGKILL；重新执行前先检查备份阶段、容器状态和数据库版本。脚本提供可恢复材料，不替代备份恢复演练。
