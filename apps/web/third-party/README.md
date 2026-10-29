# shadcn-admin 来源

首批后台外壳和业务页采用 [satnaing/shadcn-admin](https://github.com/satnaing/shadcn-admin) 的 shadcn/ui 组件与布局模式。

固定来源提交：`e16c87f213a5ba5e45964e9b67c792105ec74d26`。

复用文件：`src/components/ui/*`、`src/hooks/use-mobile.tsx`、`src/app/shadcn-theme.css`。许可证见 `shadcn-admin-LICENSE`（MIT，Copyright 2024 Sat Naing）。本地适配包括相对导入、中文无障碍文案、按钮默认类型、项目严格类型和布局约束。

未引入上游的 Clerk、演示数据或认证存储；认证和所有业务请求仍使用本项目 API。旧 Ant Design 页面保留到后续批次；本批不是整个仓库替换，也不宣称已引入 TanStack Router/Table。
