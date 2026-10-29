# 贡献指南 / Contributing

欢迎中文或英文的 Issue、文档和代码贡献。先查看 [路线图](docs/roadmap.md) 和现有 Issues，避免重复工作；较大的接口、状态机和数据语义变更请先在 Issue 中讨论。

## 开发与验证

1. Fork 仓库，从 `master` 创建功能分支。
2. 按 [开发指南](docs/development.md) 安装工具链，使用独立测试资源。
3. 沿用现有项目结构和 UI 组件；同时检查浅色、深色与窄屏。
4. 为实际业务分支补测试，例如权限隔离、恢复位点、并发冲突和错误传播。不要添加仅重复 DTO 或实现细节的测试。
5. 执行与改动相关的测试、lint、typecheck 和 build，明确记录未覆盖环境。
6. 提交 PR，关联 Issue，说明用户行为变化、验证结果及兼容性影响。

```sh
corepack pnpm lint
corepack pnpm typecheck
corepack pnpm test
corepack pnpm build
```

提交信息采用 `feat:`、`fix:`、`docs:`、`test:` 等 Conventional Commits 格式。不要提交 `node_modules`、构建结果、运行目录、数据库备份、个人路径、真实凭据或原始生产日志。

## 数据与恢复语义

- 不自动重置复制槽、Consumer Group Offset 或已确认检查点。
- 不因配置变化静默重新全量；影响数据范围的操作须有明确预览和确认。
- 不将进程存活等同于同步健康，不将未知指标显示成零。
- 数据库迁移须说明作用范围、备份要求和失败处理，不能以旧镜像回退代替数据库恢复。
- 数据源与任务列表必须遵守既有权限，不通过搜索、分页总数或分组泄露未授权资源。

## 授权与协作

提交贡献表示你有权提供该代码，并同意原创贡献按项目 Apache-2.0 协议提供。第三方代码须保留其原始许可证和来源。遵守 [社区行为准则](CODE_OF_CONDUCT.md)；安全漏洞按 [SECURITY.md](SECURITY.md) 私下报告。

English contributions are welcome. Open an issue before large changes, use isolated test resources, preserve recovery and authorization semantics, and include relevant validation in your PR. By contributing, you confirm your right to submit the work under the project license. Retain all third-party notices and report vulnerabilities privately.
