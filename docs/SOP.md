# USPTO 最新文件自动下载服务 SOP

## 1. 目的

统一当前项目的本地运行、定时同步、排障和交付方式。

## 2. 当前运行模式

当前项目只保留：

- CLI 同步入口 `run_download_latest_once.py`

常规定时同步一律通过 CLI 入口执行。

实现目录按功能分为：

- `core/`：共享契约、常量、日志
- `sync/`：同步主链路
- `storage/`：SQLite 持久化

根目录脚本只保留运行入口；`run_download_latest_once.py` 是正式 CLI 入口。

## 3. 首次部署

```bash
cd /opt/uspto_latest_downloader
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/playwright install chromium
cp .env.example .env
```

把 `/opt/uspto_latest_downloader` 替换成你的实际部署目录。

如果要修改下载目录或运行目录，优先改 `.env`：

- `USPTO_ROOT_DIR`
- `USPTO_DOWNLOADS_DIR`
- `USPTO_RUNTIME_DIR`

相对路径默认以 `USPTO_ROOT_DIR` 为基准；未设置时以项目根目录为基准。

Cookie 缓存说明：

- 默认 `USPTO_COOKIE_CACHE_TTL_SECONDS=0`，不上盘第三方 cookie
- 只有显式设置为大于 `0` 时，才会把 USPTO 会话 cookie 写入 `runtime/app.db`
- 这种模式只适合单用户、受信任主机

## 4. 启动与停止

启动：

```bash
set -a
source .env
set +a
make run
```

停止：

- 前台运行：直接中断
- 后台运行：`kill <pid>`

校验：

```bash
make test
make pycompile
```

## 5. CLI 同步

单次同步入口：

```bash
./.venv/bin/python run_download_latest_once.py
```

执行过程：

1. 抢跨进程锁，避免并发同步
2. 读取或复用缓存 cookie
3. 默认直接启动 headless Chromium 获取 cookie；只有开启 cookie TTL 时才会跨进程复用
4. 请求 USPTO 元数据
5. 选出最新 ZIP
6. 判断本地是否已有有效文件
7. 必要时下载并校验 ZIP
8. 写回 SQLite 状态、文件历史、任务历史

## 6. 排障

优先检查：

1. `stderr` 结构化日志
2. `runtime/app.db`
3. `downloads/`
4. `make test`

SQLite 常看表：

- `service_state`
- `download_history`
- `job_runs`
- `runtime_cache`

## 7. 持久化资产

- `downloads/*.zip`
- `runtime/app.db`

建议定期备份这两类数据。
运行时会主动把 `runtime/` 收紧到 `0700`，并把 `runtime/app.db`、其 `-wal/-shm` 文件和 `.download.lock` 收紧到 `0600`。

## 8. 变更原则

- 文档变更必须同步 `README.md` 或 `docs/SOP.md`
- 不把运行产物提交到源码目录
