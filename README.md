# Ape DTS Console

中文 | [English](README.en.md)

基于 [ape-dts](https://github.com/apecloud/ape-dts) 的自托管数据同步管理平台。通过 Web 页面配置同步任务、管理复制资源、查看进度与日志，并处理任务恢复和告警。

本项目是独立社区项目，不是 ApeCloud 官方产品。平台管理服务与 ape-dts 引擎是独立程序；引擎制品由部署者上传，不包含在本项目源码或默认镜像中。

## 当前能力

| 链路               | 全量与增量             | 平台管理                                                 |
| ------------------ | ---------------------- | -------------------------------------------------------- |
| PostgreSQL → Kafka | 可选初始全量，随后 CDC | 表到 Topic 映射、复制槽与 Publication、预检、监控和恢复  |
| MySQL → Kafka      | 可选初始全量，随后 CDC | Binlog 检查、表到 Topic 映射、检查点、恢复与显式重新全量 |
| Kafka → Kafka Raw  | 持续转发（扩展引擎）   | 多 Topic 映射、分区运行单元与 Consumer Group 进度        |

**Kafka → Kafka 持续 Raw 转发是社区维护 fork 的扩展能力，不是当前官方 ape-dts 的标准能力。** 使用该任务类型需要包含此扩展的引擎制品，不能直接以官方发行包替代。平台提供配置页面不代表任意上传包都具备对应能力。上游与扩展引擎的区别见 [引擎来源与兼容性](docs/operations/engine-compatibility.md)。

- 多行来源映射，每行可选择多个目标 Topic；任务卡片、共享分组和拖拽归组。
- 数据源分页、地址与名称搜索、类型筛选、连接测试、复制与删除；管理员和开发者权限、数据源授权及审计。
- 上传和选择运行制品；实际链路能力取决于所用 ape-dts 制品。
- 运行日志级别筛选与实时读取、历史运行与最长 7 天指标、PG 复制槽和 Kafka 积压监控。
- 企业微信机器人告警、可配置自动恢复、配置导入导出（不导出密码）。
- 中英文档、中文界面、明暗主题；页面时间采用 Asia/Shanghai。

Doris 目前支持数据源登记和元数据读取，尚未接入任务写入。其他 ape-dts 链路见 [路线图](docs/roadmap.md)。已有能力不代表任意数据库版本、引擎制品和部署环境均已验收。

## Docker 部署

需要 Linux x86_64、Docker 和独立的外部 PostgreSQL 平台库；业务源库和 Kafka 由部署者准备。平台与任务在一个容器中运行，任务为独立子进程，共享容器资源，不需要 Docker Socket、特权模式或 cgroup v2 委派。

```sh
git clone https://github.com/CrissMagic/ape-dts-console.git
cd ape-dts-console
docker build -t ape-dts-console:local .

sudo install -d -o 10001 -g 10001 -m 0700 \
  /srv/ape-dts-console/storage /srv/ape-dts-console/artifact-cache
install -m 0600 .env.example platform.env
# 编辑 platform.env，填写独立平台 PG 库的连接信息和唯一密码。

docker run -d --name ape-dts-console \
  --restart unless-stopped --stop-timeout 30 \
  --cpus 4 --memory 8g \
  --log-driver json-file --log-opt max-size=10m --log-opt max-file=3 \
  --env-file ./platform.env \
  --env SCS_PLATFORM_VERSION="$(git rev-parse --short HEAD)" \
  -p 127.0.0.1:18088:3000 \
  --mount type=bind,src=/srv/ape-dts-console/storage,dst=/data/storage \
  --mount type=bind,src=/srv/ape-dts-console/artifact-cache,dst=/var/cache/ape-dts-platform/artifacts \
  ape-dts-console:local
```

打开 `http://127.0.0.1:18088/`。首次初始化成功后，容器日志输出固定管理员 `admin` 的随机临时密码，首次登录必须修改。通过受控方式查看 `docker logs ape-dts-console`，不要公开包含临时密码的日志。

示例资源限额不是容量承诺；先小规模验证。默认只绑定本机；远程访问应绑定受控内网地址并配置访问限制。当前采用 HTTP，会话不支持公网安全部署；原生 HTTPS 能力尚未提供。

**必须配套备份平台数据库和 storage 目录。** storage 中包含解密凭据所需密钥，丢失后不能仅靠数据库恢复连接密码。同一个平台数据库只能有一个活动控制实例。

详细步骤：[Docker 部署](docs/operations/docker-server-deployment.md)、[升级与恢复](docs/operations/server-update-script.md)、[管理员密码恢复](docs/operations/password-authentication.md)。旧名称 `scs-data-sync-platform`、数据库 Schema、环境变量和容器内部路径保留兼容，不要求既有部署重命名。

## 本地开发

需要 Node.js 24 和 pnpm 10.12.4。不要将开发服务连接到生产平台数据库。

```sh
corepack enable
corepack pnpm install --frozen-lockfile
corepack pnpm build:packages
corepack pnpm dev
```

API 默认使用 3000 端口；前端以 Vite 启动，实际地址见终端。通过本地环境注入 `.env.example` 中的参数；该文件用于 Docker env-file，不会由开发服务自动加载。开发环境的配置、存储和运行目录见 [开发指南](docs/development.md)。

```sh
corepack pnpm lint
corepack pnpm typecheck
corepack pnpm test
corepack pnpm build
```

集成测试需要独立 Docker 测试资源，见 [开发指南](docs/development.md)。

## 文档与参与

- [文档目录](docs/README.md) · [路线图](docs/roadmap.md) · [变更记录](CHANGELOG.md)
- [报告问题／功能建议](https://github.com/CrissMagic/ape-dts-console/issues) · [贡献指南](CONTRIBUTING.md)
- [安全问题报告](SECURITY.md) · [社区行为准则](CODE_OF_CONDUCT.md)

Issue 默认使用中文，欢迎使用英文。修复和功能以可验证的链路为单位推进；不要提交真实凭据、数据库导出或未经脱敏的日志。

## 开源协议

平台原创代码采用 [Apache-2.0](LICENSE)。第三方代码按各自协议保留，见 [NOTICE](NOTICE) 和 [第三方说明](THIRD_PARTY_NOTICES.md)。ape-dts 引擎采用 AGPL-3.0，平台协议不改变引擎及其修改版本的义务。若分发引擎或带引擎的组合镜像，需同时处理对应版本的许可证与源码提供义务。
