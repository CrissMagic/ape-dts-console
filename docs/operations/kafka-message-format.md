# Kafka 消息格式

最后更新：2026-10-13

## 任务配置

PostgreSQL → Kafka 任务的「目标 Sink」步骤支持以下选项：

| 页面选项         | 任务配置               | ape-dts INI                |
| ---------------- | ---------------------- | -------------------------- |
| 消息格式         | `target.messageFormat` | `[sinker].message_format`  |
| JSON 模板        | `target.jsonTemplate`  | `[sinker].json_template`   |
| 字段定义（Avro） | `target.withFieldDefs` | `[sinker].with_field_defs` |

消息格式可选 Avro 或 JSON；JSON 模板可选标准 JSON 或 CloudCanal。字段定义仅影响 Avro 编码，不能把 Avro 二进制转换成 JSON 文本。

选择「制品默认」时，平台不写入对应 INI 参数；已有任务未设置这些字段时也不会被补入默认值。本次恢复 JSON 支持的 ape-dts 制品仍默认使用 Avro；JSON 默认模板为标准 JSON，Avro 默认包含字段定义。选择 JSON 时，页面预选 CloudCanal，也可改为标准 JSON 或制品默认模板。

如需与以下手写配置对应，在目标步骤选择 JSON、CloudCanal、包含字段定义，并在高级运行参数中设置批次大小 200：

```ini
[sinker]
db_type=kafka
sink_type=write
batch_size=200
url=<目标Kafka地址>
message_format=json
json_template=cloudcanal
with_field_defs=true
```

保存后重新预检、启动任务；初始快照和 CDC 使用相同的消息编码配置。JSON 模式下 `with_field_defs` 不改变 JSON 模板。

Kafka → Kafka Raw 任务不提供重新编码配置，继续保留源消息的 key、payload 和 headers，包括 null 值，只变更目标 Topic。

## 制品要求

平台生成参数不等同于运行制品实现了对应编码。用户维护的 ape-dts Git 历史中：

- `f35ee698` 添加 Kafka JSON 消息支持。
- `91bbaed1` 添加 `json_template` 和 CloudCanal 转换器；`feat/add-json-kafka` 分支保留此实现。
- `71a7c5b6` 合并上游时删除了消息格式配置、JSON 模板枚举和转换器；旧 `main` 快照 `0f38c3b4` 没有这条 JSON 编码路径。
- `db6550ea` 在合并上游 `b6985c14` 后恢复 JSON / CloudCanal 编码，并适配新版元数据接口和字段类型。本次 Linux x86_64 安装包来自本地 `main` 的 `293a87f7` 及保留的本地修改，已启用 metrics；这些引擎提交尚未推送至 fork 远端。

制品能力应按实际源码快照或安装包确认，不能仅凭 `main` 分支名判断。使用 JSON 时必须选择确实包含 JSON 实现的制品，并显式选择 JSON / CloudCanal；仅替换制品不会自动改变任务编码。平台不自动切换源码分支、不替换上传包，也不重新引入制品认证门槛。升级平台镜像和上传引擎制品是两个独立步骤，平台 Git 仓库不内置该安装包。

## 验证及切换注意事项

先停止任务，再修改编码、保存、预检和启动。建议使用独立测试 Topic 验证新消息的 Value 能解析为 JSON，并核对 CloudCanal 的 `action`、`before`、`data`、`db`、`schema`、`table` 等字段以及 INSERT / UPDATE / DELETE。

修改编码只影响后续新写入的消息，不转换或删除 Topic 中已有的 Avro 消息。继续写入同一 Topic 会产生混合编码记录，消费者需要兼容处理，或使用新的 Topic 隔离格式。平台侧配置保存和 INI 渲染测试不能替代真实制品的 CDC 输出验证。
