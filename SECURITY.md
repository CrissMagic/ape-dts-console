# 安全策略 / Security Policy

## 支持范围

当前维护 `1.0.x` 和默认分支；修复优先在默认分支完成。发布说明会注明回移版本。开发分支和历史测试制品不构成安全支持承诺。

## 报告漏洞

请通过仓库 [Security → Report a vulnerability](https://github.com/CrissMagic/ape-dts-console/security/advisories/new) 私下报告。若该入口不可用，在公开 Issue 中仅请求私密联系渠道，不要公开漏洞细节、利用代码、凭据或客户信息。

请附上受影响版本、必要配置条件、最小复现、影响范围和脱敏日志。确认后由维护者协调修复和披露，不承诺未经确认的响应时限。

## 部署边界

- 当前 HTTP 会话策略仅面向可信内网；原生 HTTPS 尚未提供。不要把现有服务直接暴露到公网。
- 上传的引擎是可执行程序，只接受可信来源的制品；容器与平台数据库访问权限必须受控。
- 平台数据库和 storage 中的密钥文件需要配套备份，备份本身包含敏感数据。
- 初次启动与管理员重置日志含临时密码，不应作为公开诊断日志上传。
- 不要共享生产连接配置或未经脱敏的日志。GitHub Issues 不用于保存秘密。

Report vulnerabilities privately through GitHub Security Advisories. If unavailable, request a private channel without disclosing details. The current HTTP deployment is intended for trusted private networks. Engine artifacts execute code; use trusted artifacts, protect backups and never publish temporary administrator passwords.
