# 开发与测试

## 工具链

- Node.js 24，pnpm 10.12.4（通过 Corepack）。
- 平台元数据库使用独立 PostgreSQL；不要与已运行的平台实例共用。
- 使用真实链路测试时，另准备测试源库、Kafka 与可信 ape-dts Linux 制品。

```sh
corepack enable
corepack pnpm install --frozen-lockfile
corepack pnpm build:packages
corepack pnpm dev
```

开发服务不会自动读取根目录 `.env.example`。通过 IDE 或终端设置环境变量，JSON 作为一个完整字符串传入。`SCS_PLATFORM_CONFIG_PATH` 可指向独立目录中的 `config.json`。当前本地存储、制品缓存和运行目录使用 Linux 固定路径，完整服务与引擎联调建议在 Docker 中进行，按部署文档准备可写目录；没有提供任意覆盖这些路径的环境变量。Windows 可以开发前端和运行适用的单元测试，不承诺直接运行 Linux 引擎。

首次启动需提供 `SCS_PLATFORM_DATABASE_CONFIG` 以及 `.env.example` 的 Argon2 参数和 Metrics 端口范围。持久配置建立后不应删除或随意替换密钥。API 默认 3000，前端代理默认指向本机 3000，可通过 `SCS_DEV_API_URL` 修改。Vite 的实际监听端口以终端为准。

## 检查

```sh
corepack pnpm lint
corepack pnpm typecheck
corepack pnpm test
corepack pnpm build
```

工作区为 pnpm monorepo：`apps/api` 是 NestJS 服务，`apps/web` 是 React 管理界面，`packages` 保存配置、数据库、安全、存储、观测和共享类型。只改某个模块时可使用 `corepack pnpm --filter @scs/web test` 等命令；API 和共享包的修改须覆盖实际调用方。

## 集成测试

集成测试位于 `tests/integration`，由 `vitest.integration.config.ts` 单独执行。先阅读目标测试的环境变量和资源创建逻辑，再在隔离环境中启用。测试可能创建数据库、复制槽、Topic 或容器；不要将测试连接指向生产环境。

常用入口为 `test:integration:postgres`、`test:integration:data-sources`、`test:integration:auth`、`test:integration:runtime`。部分测试需要 Linux Docker 和特定引擎制品；未运行的链路应在 PR 中明确注明，不能以单元测试通过代替真实链路验收。

## 开源快照

公开历史以清理后的 1.0.0 快照起步，不导入早期私有 Git 历史、内部试用记录、调试输出和部署截图。保留兼容所需的内部标识。平台许可证不覆盖独立引擎，见 [第三方说明](../THIRD_PARTY_NOTICES.md)。
