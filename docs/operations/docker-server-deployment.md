# Linux 服务器 Docker 部署

最后更新：2026-10-15

本文用于在 Linux 服务器上以单个 Docker 容器部署平台。PostgreSQL、Kafka 为平台外部既有资源，不由本部署创建或纳入平台容器资源限额。

## 1. 部署边界

- 镜像以固定非 root 身份 `10001:10001` 运行。
- 平台 HTTP 服务默认监听容器内 `3000`，示例映射为宿主 `18080`。
- `/data/storage` 必须持久化，其中包含平台本地密钥配置、上传制品和本地日志对象。数据库备份与该目录必须配套保管。
- `/var/cache/ape-dts-platform/artifacts` 建议持久化，避免重启或重建后重复填充运行制品缓存。
- 同步任务使用容器内独立子进程，不启动任务容器。所有任务与平台主进程共享容器 CPU、内存限额；本文 `4 CPU / 8 GiB` 仅为容器总额示例，包含平台开销。
- 任务资源是容量预算，不是任务级硬隔离；系统管理的运行容量按实际压测设置。单任务资源增长可能影响其他任务或触发容器 OOM。
- 不要求 cgroup v2 委派、host cgroup 挂载、privileged 或 Docker Socket。Metrics 端口配置缺失时容器仍可启动，但任务预检或启动必须明确失败。
- 当前 PostgreSQL 12 路径已有测试覆盖；Kafka Metadata 使用 Kafka 2.1 支持的 v4 协议且不依赖 Topic UUID，但公司 `2.1-IV2` 环境仍应执行本地连接、元数据、Topic 创建和真实制品验证。

## 2. 前置条件

服务器应满足：

1. 64 位 Linux，Docker Engine 与 Docker Compose v2 可用。
2. Docker 能为普通非 root 容器执行总体 CPU、内存限额；无需宿主切换 cgroup 版本。旧内核和旧 Docker 的镜像兼容性及资源控制效果应在实际服务器验证，不为本平台盲目修改 GRUB。
3. 已准备独立的平台 PostgreSQL 数据库。数据库账号应拥有该数据库，并能创建、升级 `ape_dts_platform` schema。不要让两个平台容器同时连接同一平台数据库。
4. 容器能够访问业务 PostgreSQL 和 Kafka。若外部资源位于其他 Docker 容器，可将平台加入同一用户网络并使用容器 DNS 名；若使用服务器或局域网地址，应先从容器网络验证路由和防火墙。
5. 容器限额必须覆盖平台自身和全部获准任务。不能根据 4C/8GiB 示例推导安全并发数；先小规模测试，再按 [容量包络](./capacity-envelope.md) 设置运行容量。

平台数据库可按以下方式准备；`CREATE DATABASE` 需在事务外执行，密码应替换为受管凭据：

```sql
CREATE ROLE scs_platform LOGIN PASSWORD 'replace-with-managed-secret';
CREATE DATABASE scs_data_sync_platform
  OWNER scs_platform
  ENCODING 'UTF8'
  TEMPLATE template0;
```

业务 PostgreSQL 若用于 CDC，还必须按 ape-dts 要求启用逻辑复制，例如 PostgreSQL 12 的 `wal_level=logical`，并授予连接账号读取源表及管理所选 Slot、Publication 所需的实际权限。平台数据库与业务源库应分开理解。

## 3. 构建镜像

在仓库根目录执行。推荐把镜像标签和 `SCS_PLATFORM_VERSION` 固定为同一个不可变版本，便于定位运行错误。

```sh
git rev-parse --short HEAD
docker image inspect node:24.19.0-bookworm-slim >/dev/null
docker build \
  --build-arg NODE_IMAGE=node:24.19.0-bookworm-slim \
  --tag scs-data-sync-platform:<版本> \
  .
docker image inspect scs-data-sync-platform:<版本> \
  --format '{{.Id}} {{.RepoTags}}'
```

离线服务器应提前导入 `node:24.19.0-bookworm-slim` 及平台镜像所需层；不要临时改用未经验证的 Node 大版本。

## 4. 准备持久化目录与环境文件

以下路径仅为示例。目录必须属于容器运行身份，且不能被其他普通用户读取：

```sh
sudo install -d -o 10001 -g 10001 -m 0700 \
  /srv/scs-data-sync-platform/storage \
  /srv/scs-data-sync-platform/artifact-cache
sudo install -d -o root -g root -m 0700 /etc/scs-data-sync-platform
sudo touch /etc/scs-data-sync-platform/platform.env
sudo chown root:root /etc/scs-data-sync-platform/platform.env
sudo chmod 0600 /etc/scs-data-sync-platform/platform.env
```

首次部署的 `/etc/scs-data-sync-platform/platform.env` 示例：

```dotenv
SCS_PLATFORM_VERSION=<与镜像一致的版本>
SCS_PLATFORM_CONFIG_PATH=/data/storage/.platform/config.json
SCS_PLATFORM_DATABASE_CONFIG={"host":"<平台PG地址>","port":5432,"database":"scs_data_sync_platform","username":"scs_platform","password":"<数据库密码>","tls":{"mode":"DISABLE"}}

SECURITY_ARGON2_MEMORY_KIB=65536
SECURITY_ARGON2_PARALLELISM=2
SECURITY_ARGON2_PASSES=3
SECURITY_ARGON2_TAG_LENGTH=32

SCS_RUNTIME_METRICS_PORT_MIN=36001
SCS_RUNTIME_METRICS_PORT_MAX=36100
```

注意：

- Docker `--env-file` 中的 JSON 不要再套一层 shell 引号；密码包含换行或难以安全表示的字符时，应先换用适合 env 文件的受管密码或由部署系统注入。
- `SECURITY_ARGON2_*` 四项必须同时配置。示例为部署起点，正式参数应结合服务器性能和安全基线评估。
- `SCS_RUNTIME_METRICS_PORT_MIN/MAX` 是容器网络命名空间内的专用端口池，不需要发布到宿主，也不应被容器内其他进程占用。范围容量至少覆盖全部并发运行单元。
- TLS 生产配置可使用 `REQUIRE`，或在 `VERIFY_FULL` 中提供 CA；不要在不可信网络直接照搬 `DISABLE`。
- 首次初始化成功后，连接信息和数据加密密钥会写入 `/data/storage/.platform/config.json`。核对备份后，可从后续容器环境中移除 `SCS_PLATFORM_DATABASE_CONFIG`，平台会优先使用持久化配置。

## 5. Docker Compose 或 Docker 启动

在服务器部署目录创建 `compose.yml`。此示例仅包含平台，连接第 4 节配置的外部 PostgreSQL：

```yaml
name: scs-data-sync-platform

services:
  platform:
    image: scs-data-sync-platform:<版本>
    container_name: scs-data-sync-platform
    user: "10001:10001"
    cpus: 4
    mem_limit: 8g
    restart: unless-stopped
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    read_only: true
    env_file:
      - /etc/scs-data-sync-platform/platform.env
    ports:
      - "127.0.0.1:18080:3000"
    volumes:
      - /srv/scs-data-sync-platform/storage:/data/storage:rw
      - /srv/scs-data-sync-platform/artifact-cache:/var/cache/ape-dts-platform/artifacts:rw
    tmpfs:
      - /run/ape-dts:rw,nosuid,nodev,noexec,mode=0700,uid=10001,gid=10001
      - /tmp:rw,nosuid,nodev,noexec,mode=1777
    stop_grace_period: 30s
```

如需从公司内网其他电脑访问，将端口监听地址改为服务器受控的内网地址，并配置防火墙和访问策略；不要无条件发布到 `0.0.0.0`。当前平台支持 HTTP 直连，不要求配置 TLS 反向代理。例如端口已发布为 `192.0.2.10:18088:3000` 时，可直接访问 `http://192.0.2.10:18088/` 并登录。

登录 Cookie 按 HTTP 策略签发，无需新增环境变量；HttpOnly、SameSite、CSRF 和会话过期校验保持生效。HTTP 不加密账号密码和会话，必须限制在受控内网，不能直接暴露到公网；HTTPS 按后续需求另行实现。本次更新不涉及数据库迁移或账号密码重置。

先验证配置，再启动：

```sh
docker compose -f compose.yml config
docker compose -f compose.yml up -d
docker compose -f compose.yml ps
docker compose -f compose.yml logs --tail=200 platform
```

不使用 Compose 时执行等价命令（替换镜像版本）：

```sh
docker run -d --name scs-data-sync-platform \
  --user 10001:10001 --read-only \
  --cpus=4 --memory=8g --restart=unless-stopped \
  --log-driver=json-file --log-opt=max-size=10m --log-opt=max-file=3 \
  --env-file /etc/scs-data-sync-platform/platform.env \
  -p 127.0.0.1:18080:3000 \
  --mount type=bind,src=/srv/scs-data-sync-platform/storage,dst=/data/storage \
  --mount type=bind,src=/srv/scs-data-sync-platform/artifact-cache,dst=/var/cache/ape-dts-platform/artifacts \
  --tmpfs /run/ape-dts:rw,nosuid,nodev,noexec,mode=0700,uid=10001,gid=10001 \
  --tmpfs /tmp:rw,nosuid,nodev,noexec,mode=1777 \
  --stop-timeout 30 scs-data-sync-platform:<版本>
```

两种方式都只设置容器总体限额。容器重启会结束其中全部任务进程；恢复由平台现有恢复流程判断，不应在入口脚本直接启动未知历史进程。检查 Docker 实际限额与生产身份：

```sh
docker inspect scs-data-sync-platform \
  --format 'CPU={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}} User={{.Config.User}}'
docker exec --user 10001:10001 scs-data-sync-platform \
  sh -ceu 'id; test -w /data/storage; test -w /var/cache/ape-dts-platform/artifacts; test -w /run/ape-dts'
```

示例应显示 `CPU=4000000000 Memory=8589934592 User=10001:10001`。这证明配置已传入 Docker，不替代实际负载下的限额验证。已有委派部署的迁移注意事项见 [旧 cgroup v2 委派方案](./cgroup-v2-delegation.md)。

## 6. Rancher 私有化部署

云效流水线使用 Dockerfile 构建并推送不可变镜像，Rancher 拉取该镜像维护单个平台工作负载。无需让平台控制 Rancher、Docker 或创建额外 Worker/任务容器。

Rancher 中配置：

- 副本数固定为 1，禁止两个平台实例同时连接同一平台数据库；更新时确保旧实例完全退出，再启动新实例。
- 容器运行身份为 UID/GID `10001:10001`，不启用 privileged，不挂载 Docker Socket 或宿主 cgroup。
- 将第 4 节环境变量通过容器启动参数注入；数据库密码用部署侧 Secret 管理。镜像标签与 `SCS_PLATFORM_VERSION` 一致。
- 设置工作负载总体 CPU、内存 limits，例如 `4` CPU、`8Gi` 内存；requests 按节点容量和压测配置，不把总额度展示为每任务额度。
- 将持久化卷挂载到 `/data/storage` 和 Artifact cache；存储驱动应确保 UID `10001` 可写，不能假定 `fsGroup` 对所有驱动均生效。
- 若启用只读根文件系统，为 `/run/ape-dts`、`/tmp` 提供可写临时卷，并确认运行身份可写。临时运行目录不替代持久化日志和检查点备份。
- HTTP 端口为 `3000`，由受控 Service/Ingress 暴露。Metrics 端口池仅用于容器内 loopback，不创建外部 Service/hostPort。
- 存活检查使用 `/api/health/live`，就绪检查使用 `/api/health/ready`；为初始化和恢复提供启动等待时间，避免用存活检查反复重启尚在初始化的实例。

数据库与业务 PG/Kafka 均连接客户既有资源。服务发现、凭据、TLS、网络策略和存储备份由客户部署侧维护。

## 7. 首次启动与管理员登录

首次初始化完成后，容器日志只输出一次固定账号 `admin` 的随机临时密码：

```sh
docker compose -f compose.yml logs platform
```

查找事件 `bootstrap.administrator_created`，安全保存临时密码，访问 `http://127.0.0.1:18080/`，使用 `admin` 登录并按要求立即修改密码。日志是有效凭据来源，应限制读取权限；正常重启不会再次打印密码。

分别检查存活与就绪：

```sh
curl -fsS http://127.0.0.1:18080/api/health/live
curl -fsS http://127.0.0.1:18080/api/health/ready
```

- `live` 返回 HTTP 200 只表示 HTTP 进程存活。
- `ready` 返回 HTTP 200 且 `phase=READY` 表示平台数据库、密钥和控制器租约已就绪，可以登录管理；不表示每个同步任务均已恢复。任务恢复在后台执行，单个任务恢复失败或暂停不阻塞平台入口。
- `LOCAL_STARTUP_FAILED` 应检查平台数据库连接、`/data/storage/.platform/config.json` 的完整性与权限，以及数据库和密钥文件是否来自同一套备份。不要删除密钥文件反复初始化。

初始化成功并备份 `/data/storage` 后，建议删除 env 文件中的 `SCS_PLATFORM_DATABASE_CONFIG`，再执行一次 `docker compose up -d --force-recreate`，确认仅凭持久化配置仍能进入 `READY`。

## 8. 部署后功能验证

建议按以下顺序测试：

1. 在「数据源」登记并测试业务 PostgreSQL 与 Kafka。Kafka Topic 不存在时可以先保存任务；选择自动创建后，由管理员在预检流程中显式准备。
2. 在「运行制品」上传 Linux ELF 或安装包并登记。上传文件和已登记副本位于 `/data/storage`；详见 [文件上传与容器文件制品登记](./artifact-file-registration.md)。
3. 任务页面直接选择已登记的上传包，无需执行场景认证、手动校验或手动启用。文件完整性、架构及安全路径检查仍保留；包不兼容或无法执行时，在启动记录和运行日志中查看错误。
   PostgreSQL → Kafka 可在「目标 Sink」配置 Avro / JSON 及 CloudCanal 模板；JSON 必须由所选制品实现。配置与切换注意事项见 [Kafka 消息格式](./kafka-message-format.md)。
4. 创建测试任务，先运行预检。平台管理模式下若 Slot 或 Publication 缺失，只应出现一条 PostgreSQL 资源失败项；管理员准备资源后重新预检。
5. 启动任务并确认状态从 `STARTING` 进入 `RUNNING`；检查任务详情中的日志和性能指标，停止后确认进程退出、容量释放，再验证可重新启动。通过 `docker stats --no-stream scs-data-sync-platform` 观察整个容器的资源使用，不将其归属为单任务数据。
6. 旧任务若已经有失败运行记录且外部复制资源被清理，不允许重新走首次资源准备；应复制为新任务，并使用新的 Slot 和 Publication 名称。

Kafka `192.0.2.10:30015` 等实际地址应在页面数据源中配置，不写入平台部署环境文件。Kafka 2.1 没有 Topic UUID，平台按集群、Topic 名和分区事实核验，不应把获取不到 Topic UUID 当作阻断条件。

业务 PostgreSQL 数据源使用 `VERIFY_FULL` 时，在页面保存 CA 证书即可；平台会在每个运行单元目录内以 `0600` 权限生成 `postgres-ca.pem`，通过连接 URL 的 `sslrootcert` 同时供全量和 CDC 连接使用，退出或启动失败时清理。无需额外挂载证书目录或修改容器 env；不会自动降级为不校验证书的模式，制品仍须支持对应 PostgreSQL TLS 参数。

## 9. 升级与回滚

直接 Docker 部署可使用 [服务器更新脚本](./server-update-script.md)，自动拉取 master、构建、停机备份、迁移、重建及就绪检查；脚本会为本地 Docker 日志配置轮转。

告警及制品删除版本新增迁移 21、22；已有部署必须先按本节执行数据库升级，不能仅替换镜像。功能配置与验证见 [告警与消费进度](./alerts-and-consumer-progress.md)。环境变量、挂载及容器总体资源配置不变。

普通代码更新也必须重建容器，单纯 `docker restart` 不会替换镜像：停止同步任务并确认退出，备份后构建/拉取新镜像，更新 Compose 或 Rancher 镜像版本，再执行 `docker compose -f compose.yml up -d --force-recreate` 或 Rancher 工作负载更新。不要删除存储目录、复制资源或平台数据库。

直接使用 `docker run` 的环境：停止任务并备份后执行 `docker stop scs-data-sync-platform`、`docker rm scs-data-sync-platform`，再按第 5 节新镜像命令创建同名容器；这只移除容器，保留已挂载宿主目录。不要追加删除卷参数。

升级前：

1. 停止平台容器，确认没有第二个控制实例。
2. 对平台 PostgreSQL 做一致性备份。
3. 备份 `/srv/scs-data-sync-platform/storage`，特别是 `.platform/config.json`；数据库与该目录必须来自同一恢复点。
4. 构建或导入新镜像，核对镜像摘要和版本。

数据库 schema 为旧版本时，先用新镜像执行维护命令：

```sh
docker compose -f compose.yml down
docker compose -f compose.yml run --rm --no-deps \
  --entrypoint node platform \
  dist/bootstrap/security-cli.js upgrade --backup-confirmed
docker compose -f compose.yml up -d
```

若旧环境管理员仍叫 `preview-admin`，升级命令追加 `--administrator preview-admin`，迁移后固定用户名为 `admin`，用户 ID、任务归属和审计记录保留。

启动后重新检查 `ready`、管理员登录、数据源连接、制品选择、运行目录权限及任务启停。数据库迁移完成后，不要让旧镜像直接连接新 schema；需要回滚时，应同时恢复升级前数据库和配套存储目录备份。

### 容器重启后的任务恢复

平台先开放正常管理入口，再后台恢复需要运行的任务。可定位到具体任务的制品、连续性或启动失败会转为人工处理，其他符合条件的任务继续恢复；容量不足等整体恢复条件也不封锁平台页面。数据库不可用、密钥不匹配、schema 待升级或并发平台实例仍属于平台启动阻塞，不绕过这些检查。

正常 `docker stop` 会结束平台拥有的子进程，但保留仍为“运行”的任务意图、应用配置及恢复绑定，重启后继续按连续性、制品和容量检查恢复。用户主动停止的任务不会因此自动启动。停机需要 Docker 留出足够的优雅退出时间；超时强杀或服务器断电不能视为已完成正常停机。

元数据库的持锁专用连接每 10 秒探活，异常后销毁连接并进入既有降级和恢复流程；解锁失败的连接不会被放回池中。新连接使用会话级 TCP keepalive 和 `tcp_user_timeout`，缩短容器断网、宿主机断电后的失联连接回收时间，无需新增 env、修改数据库全局参数或扩大容器权限。正常运行的另一实例仍会阻止并发启动，平台不会自动抢锁或结束其他实例。旧版本已经建立的连接不会自动应用这些参数，首次升级的残留连接处理见 [更新脚本说明](./server-update-script.md#升级前控制锁冲突后的续跑)。

异常旧运行会在任务列表显示为阻塞。管理员确认旧容器及该任务的旧进程已停止后，可在该行操作列点击“恢复任务”，仅为该任务重新执行恢复检查，无需删除任务。恢复失败会保留具体原因，平台入口不受影响；修复原因后可再次尝试。不要在旧实例仍可能运行时确认，避免重复消费或多个进程争用复制槽。

恢复沿用已有复制槽、Publication、Kafka Topic 和同步位点，不自动重建资源、不重置位点。包含全量快照的任务目前没有可验证的快照续传位点，因此仍会提示 `RECOVERY_SNAPSHOT_RESTART_REQUIRES_DECISION`，不会静默重做全量；需另行明确重做策略。复制槽失效、WAL 已丢失或外部资源身份变化也不会强行恢复。

如需结束恢复处理，可点击“放弃恢复”，将任务置为停止并释放旧容量和指标端口占用。该操作保留任务配置、制品绑定、失败历史和外部复制资源；后续重新启动仍需预检，不能将它视为保证从原位点续传的方式。当前平台仍持有进程的任务不允许通过这些入口绕过正常停止。

原平台数据库和 `/data/storage/.platform/config.json` 继续使用，无需删库、删密钥文件或重新初始化管理员。处理前应备份平台数据库和配套存储目录；不要为了通过恢复检查直接删除运行绑定或将恢复步骤伪造为成功。

## 10. 管理员密码恢复

忘记 `admin` 密码时，由有容器执行权限的部署管理员运行：

```sh
docker compose -f compose.yml exec -T platform \
  node dist/auth/reset-admin-cli.js
```

命令会输出新的随机临时密码、撤销 `admin` 旧会话，并要求下次登录改密。没有公开的免登录 Web 重置入口。详细语义见 [密码登录与管理员恢复](./password-authentication.md)。

## 11. 常见问题

| 现象 | 检查方向 |
| --- | --- |
| 页面可打开，但 `ready` 为 503 | 查看返回的 phase 和容器日志；重点检查数据库、配置文件和控制器租约 |
| 首次启动没有管理员密码 | 只有全新数据库初始化成功时才输出一次；已有环境应使用原密码或执行管理员密码恢复 |
| 上传或日志写入失败 | 核对 `/data/storage`、Artifact cache 的属主、权限、容量和只读挂载状态 |
| 预检报 Metrics 端口未配置 | 同时配置 `SCS_RUNTIME_METRICS_PORT_MIN/MAX`，保证范围合法且容量足够 |
| 预检仍报 cgroup 委派错误 | 确认镜像已更新到共享容器资源版本；新版本不要求子 cgroup，不必扩大宿主权限 |
| 平台管理却显示自行管理提示 | 确认使用包含 2026-09-30 修复的新镜像；新版本使用独立只读核验路径，不再强制套用自行管理语义 |
| PostgreSQL 资源重复报警 | 确认新镜像；资源核验失败后不会继续读取连续性，同一轮预检只保留一条 `PG_RESOURCES` |

## 12. 停止与备份

正常停止：

```sh
docker compose -f compose.yml down
```

不要在未备份时删除 `/srv/scs-data-sync-platform/storage`。最低备份集合包括：

- 平台 PostgreSQL 数据库；
- `/data/storage` 对应宿主目录；
- 当前镜像摘要、`SCS_PLATFORM_VERSION` 和部署配置；
- 已登记 ape-dts 制品的来源与文件摘要。

Artifact cache 可以重新填充，但保留它可减少恢复时间。任何恢复演练都应验证数据库与本地密钥配置配套、`ready` 可达、数据源凭据可解密，以及任务启停、容量预算和共享容器限额符合部署配置。

## 13. Docker 日志容量

本部署为 Docker 标准输出日志设置 `max-size=10m`、`max-file=3`，每个容器约保留 30 MiB（轮转存在少量开销），超限自动淘汰最旧的日志。此配置需重建容器生效，单纯 restart 不会修改旧容器的日志策略。不要直接 truncate 或删除 Docker 管理的日志文件。

任务持久化日志不属于 Docker 日志：现有平台默认日志保留 30 天、单次运行日志上限 1 GiB，按平台设置执行；超期清理针对已结束运行，每小时检查一次。指标保留 7 天，不等于任务日志保留 7 天。此次更新不修改数据库中的日志保留策略。
