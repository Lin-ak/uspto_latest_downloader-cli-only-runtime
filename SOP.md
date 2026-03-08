# USPTO 最新文件自动下载服务 SOP

## 1. 目的

统一当前项目的本地运行、定时同步、排障和交付方式。

## 2. 当前运行模式

当前项目只保留：

- 公开只读 HTTP 接口
- CLI 同步入口 `run_download_latest_once.py`

常规定时同步一律通过 CLI 入口执行，不通过 HTTP 触发。

实现目录按功能分为：

- `app/`：HTTP 应用层
- `core/`：共享契约、常量、日志
- `sync/`：同步主链路
- `storage/`：SQLite 持久化

根目录脚本只保留运行入口；其中 `server.py` 是薄启动包装，`run_download_latest_once.py` 是正式 CLI 入口。

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
```

## 5. 公开接口

- `GET /health/ready`
- `GET /api/v1/status`
- `GET /api/v1/files/latest/download`

## 6. CLI 同步

单次同步入口：

```bash
./.venv/bin/python run_download_latest_once.py
```

执行过程：

1. 抢跨进程锁，避免并发同步
2. 读取或复用缓存 cookie
3. 必要时启动 headless Chromium 获取 cookie
4. 请求 USPTO 元数据
5. 选出最新 ZIP
6. 判断本地是否已有有效文件
7. 必要时下载并校验 ZIP
8. 写回 SQLite 状态、文件历史、任务历史

## 7. 排障

优先检查：

1. `GET /health/ready`
2. `stderr` 结构化日志
3. `runtime/app.db`
4. `downloads/`

SQLite 常看表：

- `service_state`
- `download_history`
- `job_runs`
- `runtime_cache`

## 8. 持久化资产

- `downloads/*.zip`
- `runtime/app.db`

建议定期备份这两类数据。

## 9. 变更原则

- 新公开接口必须补测试
- 文档变更必须同步 `README.md` 或 `API.md`
- 不把运行产物提交到源码目录
