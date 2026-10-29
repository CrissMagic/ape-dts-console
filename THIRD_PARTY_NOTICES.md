# 第三方代码与引擎

平台原创代码使用 Apache-2.0；下表不替代第三方许可证原文。

| 组件         | 使用方式                                   | 协议与来源                                                                                                     |
| ------------ | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| shadcn-admin | 改编 UI 组件、样式和布局                   | MIT；[来源与修改说明](apps/web/third-party/README.md)、[许可证原文](apps/web/third-party/shadcn-admin-LICENSE) |
| ape-dts      | 由用户单独上传的可执行制品，作为子进程运行 | AGPL-3.0；[上游源码与许可证](https://github.com/apecloud/ape-dts)                                              |
| npm 依赖     | 通过 pnpm-lock.yaml 固定版本安装           | 各依赖随包附带的许可证；锁文件并不授予额外权利                                                                 |

平台仓库和默认 Dockerfile 不打包 ape-dts 引擎。分发引擎、引擎修改版本或含引擎的镜像时，应提供对应版本的许可证、版权声明及所需对应源码；不能用平台 Apache-2.0 协议替代引擎协议。

新增或复制第三方源码时，贡献者必须记录上游地址、版本、许可证及本地修改，保留原版权声明。不要引入没有明确授权的代码或部署资产。

Original platform code uses Apache-2.0. Third-party components retain their original licenses. The independently uploaded AGPL-3.0 engine is not bundled or relicensed. Redistributors must handle the license and corresponding-source obligations of the exact engine build they distribute.
