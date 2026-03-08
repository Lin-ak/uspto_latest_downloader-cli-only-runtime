# USPTO 最新文件自动下载服务

[![Python 3.13](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Playwright](https://img.shields.io/badge/Playwright-Chromium-2EAD33?logo=playwright&logoColor=white)](https://playwright.dev/)
[![SQLite](https://img.shields.io/badge/SQLite-runtime-003B57?logo=sqlite&logoColor=white)](https://www.sqlite.org/)
[![Mode](https://img.shields.io/badge/Sync-CLI--first-6C5CE7)](./run_download_latest_once.py)

这个目录包含应用代码、运行文档和本地运维文件。当前实现已经按功能分包，根目录只保留少量运行入口。

详细接口见 [API.md](./API.md)，运行与维护规范见 [SOP.md](./SOP.md)。

## 本地运行

准备虚拟环境并安装依赖：

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/playwright install chromium
```

如需调整可选环境变量，可复制样例：

```bash
cp .env.example .env
set -a
source .env
set +a
```

路径相关环境变量：

- `USPTO_ROOT_DIR`：统一指定运行根目录；未显式设置下载/运行目录时，默认在该目录下使用 `downloads/` 和 `runtime/`
- `USPTO_DOWNLOADS_DIR`：单独指定 ZIP 下载目录
- `USPTO_RUNTIME_DIR`：单独指定 `app.db`、锁文件和运行时缓存目录

说明：

- 相对路径会以 `USPTO_ROOT_DIR` 为基准；如果未设置 `USPTO_ROOT_DIR`，则以当前项目根目录为基准
- 如果三个变量都不设置，行为与当前默认值一致

常用入口：

```bash
make test
make run
```

默认服务地址：

```text
http://127.0.0.1:8010
```

## 接口

对外只读接口：

- `GET /health/ready`
- `GET /api/v1/status`
- `GET /api/v1/files/latest/download`

其中：

- `GET /api/v1/status` 适合作为业务状态检查接口
- `GET /health/ready` 适合作为可用性探针
- `GET /api/v1/files/latest/download` 直接返回当前最新本地 ZIP，并带 `ETag` / `Last-Modified`

CLI 同步入口：

- `python3 run_download_latest_once.py`

常规定时同步请固定使用 CLI 入口，不通过 HTTP 触发。

## 定时同步

推荐把定时同步固定到：

```bash
./.venv/bin/python run_download_latest_once.py
```

### cron

示例：每 6 小时同步一次，并把标准输出与错误输出写到日志文件。

```cron
0 */6 * * * cd /opt/uspto_latest_downloader && /bin/zsh -lc 'set -a; [ -f .env ] && source .env; set +a; ./.venv/bin/python run_download_latest_once.py' >> runtime/cron.log 2>&1
```

说明：

- 把 `/opt/uspto_latest_downloader` 替换成你的实际部署目录
- 用绝对路径执行，避免 `cron` 下工作目录不确定
- 如果你不需要 `.env` 里的可选参数，可以去掉 `source .env`
- 项目内部已经有跨进程锁，不需要额外再套一层 `flock`

### systemd timer

推荐在 Linux 上优先使用 `systemd timer`，更容易观察日志和失败重试。

服务单元示例：

```ini
[Unit]
Description=USPTO latest downloader sync job
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/opt/uspto_latest_downloader
EnvironmentFile=/opt/uspto_latest_downloader/.env
ExecStart=/opt/uspto_latest_downloader/.venv/bin/python /opt/uspto_latest_downloader/run_download_latest_once.py
```

把 `/opt/uspto_latest_downloader` 替换成你的实际部署目录。

定时器单元示例：

```ini
[Unit]
Description=Run USPTO latest downloader every 6 hours

[Timer]
OnBootSec=10min
OnUnitActiveSec=6h
Persistent=true

[Install]
WantedBy=timers.target
```

启用方式：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now uspto-latest-downloader.timer
sudo systemctl status uspto-latest-downloader.timer
```

## 目录

- `app/`：HTTP 应用层，包括路由、响应模型和应用装配
- `core/`：共享契约、常量与日志工具
- `sync/`：同步主链路，包括调度、上游访问和 ZIP 处理
- `storage/`：SQLite 持久化层
- `server.py`：薄启动包装，实际应用装配在 `app/factory.py`
- `run_download_latest_once.py`：CLI 同步入口
- `Makefile`：本地统一运行与校验入口
- `.env.example`：本地环境变量样例
- `tests/`：测试目录
- `downloads/`：保存已下载 ZIP
- `runtime/app.db`：保存运行状态、下载历史和 `job_runs` 执行记录

## 运行策略

- 下载任务会对浏览器会话、官方元数据和 ZIP 下载做有限次重试与退避
- 浏览器拿到的上游 cookie 会做短期缓存；默认缓存 900 秒，可用 `USPTO_COOKIE_CACHE_TTL_SECONDS=0` 关闭
- 重试退避默认带随机抖动，避免固定节奏命中风控；抖动比例可用 `USPTO_RETRY_JITTER_RATIO` 调整
- 连续失败后会进入短期冷却窗口，避免持续撞击 USPTO；冷却秒数可用 `USPTO_FAILURE_COOLDOWN_SECONDS` 调整，设为 `0` 可关闭
- 上游 `fileDownloadURI` 会做 `https://data.uspto.gov/...` allowlist 校验，公开接口也会再次脱敏非法 URL
- 已存在的本地 ZIP 不只检查大小，还会做 ZIP 结构校验
- 最新文件解析路径只做轻量 ZIP 可读性校验，不再对历史 ZIP 重复全量 `testzip()`
- 运行日志统一走结构化 JSON logging，输出到 `stderr`
- 应用启动时会在 FastAPI lifespan 中执行运行状态修复和文件历史回补

## 包结构说明

- 项目内部实现优先从 `app/`、`core/`、`sync/`、`storage/` 导入
- 根目录只保留 `server.py`、`run_download_latest_once.py` 这 2 个运行入口
- 其余实现模块已经收敛到分包目录，不再建议新增根目录级功能文件
