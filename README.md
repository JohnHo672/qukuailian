# OKX 自动交易系统

本仓库保存当前本机部署的 Hummingbot 自动交易系统源码，不包含运行数据或交易所密钥。

## 项目结构

- `hummingbot-api/`：Hummingbot API，以及独立的 OKX 模拟盘现货与永续连接支持。
- `condor/`：Condor 网页控制台，默认简体中文并保留 English 切换。

## 当前范围

- Hummingbot Core 固定为已验证版本 `20260920`。
- 模拟盘连接器：`okx_demo`、`okx_perpetual_demo`。
- 模拟盘与实盘凭据、REST 请求和 WebSocket 地址相互隔离。
- 本仓库不包含 API Key、Secret、Passphrase、机器人实例、订单、数据库、日志、备份或 Docker/WSL 数据盘。
- 系统不承诺盈利，启用任何策略前应先完成模拟验证和风险参数审核。

## 配置

请分别参考两个子项目中的 README 和 `.env.example`。复制示例文件创建本机 `.env` 后再填写配置，切勿将真实密钥提交到 Git。

本项目基于 Hummingbot 官方开源项目进行定制，许可证与上游说明保留在各子目录中。
