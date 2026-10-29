# 部署侧制品认证

仅部署管理员运行。平台容器不需要 Docker socket，也没有接受任意通过证据的 HTTP/CLI 导入入口。

先在相同版本源码上安装依赖并构建 packages 与 API；准备 Docker、Compose、独立测试镜像及当前平台运行镜像。
仅通过进程环境设置 `SCS_CERTIFICATION_DATABASE_URL`（平台数据库连接，勿放命令参数/日志）和 `SCS_PLATFORM_VERSION`（与部署值完全相同）。

```powershell
node tools/certify-artifact.mjs --artifact-id <制品UUID> --sha256 <实际ELF的SHA256> --candidate <ELF或tar.gz或tar.xz路径> --runtime-image <当前平台镜像> --suite pg
node tools/certify-artifact.mjs --artifact-id <制品UUID> --sha256 <实际ELF的SHA256> --candidate <ELF或tar.gz或tar.xz路径> --runtime-image <当前平台镜像> --suite kafka
```

工具锁定制品认证，验证固定 `admin` 为启用管理员、制品为 INACTIVE，且登记/实测/存储对象 SHA 一致。解析候选包后再次核验 ELF SHA，规范打包不会修改二进制内容。运行镜像解析为不可变本地 image ID。

每次完整执行实际镜像的 `--version`/动态库检查及所选 Phase0 业务测试。PG 组覆盖快照、CDC DML、快照到CDC衔接和点号Topic；Kafka组覆盖消息、恢复、多分区和扩容边界。所有5项实际通过且无跳过才持久化对应场景PASSED；任何失败记录FAILED，不启用制品。重复认证不会复用旧SHA证据。

认证使用独立 Compose 项目、PG12/Kafka3.8容器、4C8G运行限额，不连接业务PG/Kafka；不代表已验证公司Kafka2.1-IV2全部兼容性。正常结束清理本次测试容器/临时目录。强制终止可能留下RUNNING探测或独立测试资源，需部署管理员核查；不得手工改成PASSED。

Kafka新消费组使用latest起步。“extractor starts”日志和HTTP健康响应不代表消费位置已初始化；单分区实时场景先以独立探测消息确认端到端转发及group offset提交，再记录源/目标基线。业务样例只发送一次，仍严格核验消息数、headers、null key/value及重启恢复，不重试业务样例或放宽断言。探测消息只进入隔离认证Topic，不写入业务Topic。该认证不证明启动日志出现瞬间即具备消费就绪保证。

输出probeId、passed和白名单failureCode；SHA不一致、镜像不可用等会保留明确错误码，不输出原始异常或密码。业务测试和持久化均使用同一构建后的config-model包；修改源码后必须重新构建再认证。详细测试文件摘要、候选SHA、镜像ID及报告摘要哈希存于probe evidence_summary。认证完成后仍由管理员在制品页面执行启用。命令不支持 `--passed`、`--report`、自定义测试命令或选择性跳过测试。
