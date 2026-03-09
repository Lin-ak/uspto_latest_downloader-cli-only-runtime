# USPTO 最新文件自动下载服务

当前目录只保留生产运行所需代码、依赖定义和最小运行说明。正式入口固定为 CLI：

```bash
python3 run_download_latest_once.py
```

## 安装

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/playwright install chromium
```

如需设置可选环境变量：

```bash
cp .env.example .env
set -a
source .env
set +a
```

## 环境变量

- `USPTO_ROOT_DIR`：统一指定运行根目录；未显式设置下载/运行目录时，默认在该目录下使用 `downloads/` 和 `runtime/`
- `USPTO_DOWNLOADS_DIR`：单独指定 ZIP 下载目录
- `USPTO_RUNTIME_DIR`：单独指定 `app.db`、锁文件和运行时缓存目录
- `USPTO_COOKIE_CACHE_TTL_SECONDS`：默认 `0`，不持久化第三方 cookie；大于 `0` 时才会写入 `runtime/app.db`
- `USPTO_RETRY_JITTER_RATIO`：重试退避抖动比例
- `USPTO_FAILURE_COOLDOWN_SECONDS`：连续失败后的冷却秒数；设为 `0` 可关闭

说明：

- 相对路径会以 `USPTO_ROOT_DIR` 为基准；如果未设置 `USPTO_ROOT_DIR`，则以当前项目根目录为基准
- 如果三个路径变量都不设置，行为与默认目录一致
- cookie 持久化模式只适合单用户、受信任主机

## 运行

单次同步：

```bash
./.venv/bin/python run_download_latest_once.py
```

最小上线校验：

```bash
./.venv/bin/python -m py_compile run_download_latest_once.py core/*.py sync/*.py storage/*.py
./.venv/bin/playwright install chromium
./.venv/bin/python run_download_latest_once.py
```

## 定时同步

`cron` 示例：

```cron
0 */6 * * * cd /opt/uspto_latest_downloader && /bin/zsh -lc 'set -a; [ -f .env ] && source .env; set +a; ./.venv/bin/python run_download_latest_once.py' >> runtime/cron.log 2>&1
```

`systemd` 服务示例：

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

`systemd` 定时器示例：

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

## 目录

- `core/`：共享契约、常量与日志工具
- `sync/`：同步主链路，包括调度、上游访问和 ZIP 处理
- `storage/`：SQLite 持久化层
- `run_download_latest_once.py`：CLI 同步入口
- `requirements.txt`：生产依赖
- `.env.example`：部署环境变量样例
- `downloads/`：保存已下载 ZIP
- `runtime/`：保存数据库、锁文件和运行时缓存

## 运行特性

- 下载任务会对浏览器会话、官方元数据和 ZIP 下载做有限次重试与退避
- 浏览器拿到的上游 cookie 默认不持久化；只有当 `USPTO_COOKIE_CACHE_TTL_SECONDS > 0` 时才会写入 `runtime/app.db`
- 重试退避默认带随机抖动，避免固定节奏命中风控
- 连续失败后会进入短期冷却窗口，避免持续撞击 USPTO
- 上游 `fileDownloadURI` 会做 `https://data.uspto.gov/...` allowlist 校验
- 已存在的本地 ZIP 不只检查大小，还会做 ZIP 结构校验
- 运行日志统一走结构化 JSON logging，输出到 `stderr`
- CLI 每次执行都会基于 SQLite 和磁盘状态继续同步流程
- 运行时会主动把 `runtime/` 收紧到 `0700`，并把 `runtime/app.db`、其 `-wal/-shm` 文件和 `.download.lock` 收紧到 `0600`
