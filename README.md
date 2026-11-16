# 订单监听 · Portfolio Watch

监听 Binance 合约公开带单对象的交易操作、带单余额和资金划转，提供中文管理页面、Telegram / Bark 通知。**只读，不下单、不跟单、不需要 Binance API Key。**

## 功能

- 根据昵称搜索公开带单对象，添加监听；每个对象可独立开启、暂停。
- 交易记录展示：交易对、多空操作、成交数量、均价、成交金额、已实现盈亏、成交时间。
- 区分开多 / 加多、减多 / 平多、开空 / 加空、减空 / 平空。单条记录不能可靠区分全平与部分减仓。
- 展示带单余额快照变化与资金转入 / 转出历史；余额变化可能包含浮动盈亏，不能等同于充值提现。
- 全局可调轮询频率（10～86400 秒，默认 60 秒）；余额通知可设置最小变化阈值，默认 0。
- SQLite 持久化数据、同步位置、通知队列；重启去重、故障退避、两种通知渠道独立重试。
- 单容器部署，无第三方 Python 依赖；GitHub Actions 测试并发布 amd64 / arm64 镜像到 GHCR。

## Docker 部署

```sh
git clone https://github.com/zhshengjun/order-monitor.git
cd order-monitor
cp .env.example .env
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'
```

把生成的随机口令填入 `.env` 的 `ADMIN_TOKEN`，然后：

```sh
docker compose up -d --build
```

访问 <http://localhost:8080>，使用 `ADMIN_TOKEN` 登录。搜索“熬鹰”，选择“熬鹰资本”添加。组合 ID 为 `5075281354358777856`。

默认端口仅绑定服务器回环地址。远程访问可以通过 SSH 转发：

```sh
ssh -L 8080:127.0.0.1:8080 user@your-server
```

若需要公开访问，使用 HTTPS 反向代理并设置 `COOKIE_SECURE=1`。不要把访问口令或通知密钥提交到 Git。SQLite 数据卷含通知凭证，需要保护访问权限。

### 使用自动构建镜像

`main` 分支 push 或手动运行 Actions 后会发布：

```text
ghcr.io/zhshengjun/order-monitor:latest
ghcr.io/zhshengjun/order-monitor:sha-<commit>
```

版本标签 `v*` 会生成对应镜像标签。确认 Actions 成功后，在 `.env` 设置 `MONITOR_IMAGE=ghcr.io/zhshengjun/order-monitor:latest`，执行：

```sh
docker compose pull
docker compose up -d --no-build
```

如果 GHCR 包为私有，先用具有 `read:packages` 权限的凭证执行 `docker login ghcr.io`。不要将凭证写入仓库。

## 本地运行

Python 3.9+ 可运行，Docker / CI 使用 Python 3.13。

```sh
export ADMIN_TOKEN='请替换为至少16位的随机口令'
python3 app.py
```

默认 `127.0.0.1:8080`，数据库在 `data/monitor.db`。通知在前端“通知与设置”配置，未启用任何渠道时仅在页面记录。

### Telegram

在 Telegram 的 BotFather 创建机器人，把 Bot Token 和 Chat ID 填到设置页。先与机器人发起对话，或把机器人加入目标群并允许发消息。启用开关并保存。Token 保存后不回显，输入框留空保留原值。

### Bark

填写 Bark 服务根地址（默认 `https://api.day.app`）和 App 中的 Device Key，启用并保存。使用服务端 `/push` JSON 接口，可接入自建 HTTPS Bark 服务。

## 网络配置

默认读取 `https://www.binance.com`，不带 App 登录令牌。以下是启动环境变量，不通过页面修改：

| 变量 | 默认值 | 作用 |
|---|---|---|
| `ADMIN_TOKEN` | 必填 | 管理页面访问口令，至少 16 位 |
| `BINANCE_BASE_URL` | `https://www.binance.com` | 上游 HTTPS 入口 |
| `HTTPS_PROXY` / `HTTP_PROXY` | 空 | 可选 HTTP CONNECT 代理，供 Binance 和通知请求使用 |
| `DATABASE_PATH` | 本地 `data/monitor.db` | SQLite 文件路径 |
| `HOST` / `PORT` | `127.0.0.1` / `8080` | 本地监听；容器内部为 `0.0.0.0:8080` |
| `COOKIE_SECURE` | `0` | HTTPS 部署设置为 `1` |

容器中的 `127.0.0.1` 是容器本身。代理地址需要能从容器访问，例如 Docker Desktop 上的 `http://host.docker.internal:7890`（端口以实际配置为准）。只支持 Python 标准库 HTTP 代理，不直接支持 SOCKS。程序不会修改系统代理或关闭证书校验。

## 同步与通知语义

1. 首次添加仅读取最近一页交易 / 划转记录并建立余额基线，展示为“历史基线”，**不发送旧记录通知**。不自动下载所有历年历史。
2. 后续轮询逐页回溯到已保存的时间边界，最多 50 页 / 1000 条；超过范围显示错误并保留同步位置，不静默跳过缺口。
3. 暂停对象会取消其未发送通知；恢复会补查暂停期间新增记录。首次同步失败不会把下一次成功的数据误报为新订单。
4. 交易接口没有提供订单 ID，因此使用组合、币种、方向、订单类型、毫秒订单时间作为替代身份；同一身份成交数量等字段变化记为“记录更新”。同一毫秒同属性的不同订单理论上可能冲突；不能保证交易所级别逐单精确性。
5. 余额通知阈值比较相邻成功快照的差额，不累计多个小变动。所有观察到的变化仍会保存。通知中的余额、划转可能来自同一次转账，会分别显示。
6. 每个事件与通知任务在同一数据库事务提交。渠道失败独立退避重试；已成功渠道不会因另一个失败而重发。外部服务“已接收但响应丢失”仍可能导致重试重复通知，不能承诺 exactly-once。
7. 仅给事件产生时已启用的渠道创建通知，不在启用渠道时追发历史。关闭渠道后待发送任务会取消。
8. 调度为单工作线程，对象依次采集，每次成功 / 失败后计算下次时间。频率是最短轮询间隔，网络超时或对象较多时实际周期会更长。
9. 这些是 Binance 客户端使用的非正式公开接口，字段、访问限制、数据延迟可能变化。程序无法保证实时性，也无法看到未公开持仓或撤单。

## API 对应关系

公共路径前缀 `/bapi/futures/v1/friendly/future/copy-trade/`：

| 功能 | 方法 | 路径 |
|---|---|---|
| 搜索 | POST | `home-page/query-list` |
| 详情 / 带单余额 | GET | `lead-portfolio/detail?portfolioId=...` |
| 操作记录 | POST | `lead-portfolio/order-history` |
| 转入转出历史 | POST | `lead-portfolio/transfer-history` |

前端使用本服务的 `/api/*` 接口，管理 API 均需登录；`/healthz` 仅提供进程健康探针。页面每 5 秒刷新本地数据，不直接请求 Binance。

## 验证

```sh
python3 -m unittest discover -s tests -v
node --check static/app.js
```

覆盖首次同步、跨页新增、重启去重、更新记录、余额阈值、资金划转、暂停 / 恢复、上游失败不推进状态和通知分渠道重试。测试使用假上游和假通知，不发送真实消息。

## 数据与备份

首次启动会自动建库建表，首次同步只取最近 20 条作为基线，之后按已保存记录增量拉取。无需提前导入数据或 SQL。

数据位于 Docker 命名卷 `monitor-data`。容器入口会修正 `/data` 目录及 SQLite 文件的所有者，然后降权为 UID/GID `10001` 运行服务；也支持 `./data:/data` 绑定挂载。若自行覆盖容器 `user`、使用只读挂载或不允许 chown 的网络文件系统，需要预先保证目录及文件对 `10001` 可写。不要删除数据卷来解决升级问题，否则会丢失增量同步位置。

停止容器后备份整个卷，或使用 SQLite 在线备份 API；不要在写入过程中只复制主 `.db` 文件而忽略 WAL。当前没有自动清理历史，部署时注意磁盘空间。只运行一个应用实例，不共享数据库给多个副本。
