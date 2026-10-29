# OSS 日志存储部署准入

OSS 日志存储在下列证据全部由目标部署环境验证前保持 **No-Go**。代码测试或客户端 mock 不能替代环境准入。

## 必须验证的控制项

- endpoint 使用 HTTPS，并由运行平台校验证书链和主机名，不允许静默降级到明文连接。
- 日志对象写入请求显式使用 `private` ACL 和服务端加密；当前适配层要求 `AES256`。
- 运行身份只拥有所绑定 bucket/prefix 下日志对象的 `Put/Get/Head/Delete` 权限，不拥有 bucket 管理、跨 prefix 读取或 ACL 放宽权限。
- 用目标运行身份分别证明：允许访问绑定 prefix；拒绝访问同 bucket 的兄弟 prefix；拒绝设置 public ACL；写入对象的实际 ACL 与服务端加密元数据符合要求。
- profile 的 endpoint、bucket、prefix、Secret 引用在激活后不可变；已有 run 始终沿用首次绑定的 profile，不能在故障时切换驱动或 profile。
- OSS Secret 只能由 Secret Vault 在单次客户端调用所需的最短生命周期内解密，不得进入数据库明文字段、API、审计、应用日志或环境变量；调用结束后清零明文缓冲。
- 通过真实 OSS 验证 create-only 写入、重复同内容幂等、同 key 不同内容冲突、读取大小/SHA-256 校验、缺失对象以及删除权限。

## 当前未闭环项

仓库尚未选定具体 OSS 客户端实现，也未定义 OSS Secret 明文结构及其 `owner_type/purpose`。因此厂商客户端装配、Vault 解密接线和真实最小 prefix 权限证据尚未闭环；在这些事项完成并于目标环境留存证据前，不得宣称 OSS 日志存储可发布。
