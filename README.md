# USPTO 最新文件自动下载服务

这个目录包含应用代码、运行文档和本地运维文件。

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

## 目录

- `Makefile`：本地统一运行与校验入口
- `.env.example`：本地环境变量样例
- `api_schemas.py`：FastAPI 响应模型
- `api_routes_public.py`：公开页面和公开只读接口
- `app_factory.py`：FastAPI 应用装配与启动入口
- `downloader_service.py`：下载任务编排与重试逻辑
- `downloader_storage.py`：SQLite 状态、历史与 `job_runs` 持久化
- `downloader_upstream.py`：USPTO 上游浏览器与元数据访问
- `downloader_zip.py`：ZIP 文件校验、落盘与本地文件回补
- `run_download_latest_once.py`：独立下载任务入口，适合 `cron` / `systemd timer`
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
