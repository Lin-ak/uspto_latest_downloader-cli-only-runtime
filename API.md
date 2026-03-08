# API 文档

服务默认地址：

```text
http://127.0.0.1:8010
```

## 约定

- 编码：`UTF-8`
- JSON 响应默认带 `Cache-Control: no-store`
- 运行状态默认保存在 `runtime/app.db`
- 运行日志使用结构化 JSON logging，输出到 `stderr`
- 代码实现按功能分包到 `app/`、`core/`、`sync/`、`storage/`，根目录脚本主要保留启动与兼容入口

## 统一返回结构

成功：

```json
{
  "ok": true,
  "data": {},
  "meta": {}
}
```

失败：

```json
{
  "ok": false,
  "error": {
    "code": "invalid_request",
    "message": "limit 必须是正整数。"
  }
}
```

## 对象结构

### PublicFile

```json
{
  "file_name": "apc260305.zip",
  "official_data_date": "2026-03-05",
  "release_date_raw": "2026-03-06 00:54:52",
  "file_size_bytes": 32167494,
  "download_url": "https://data.uspto.gov/ui/datasets/products/files/TRTDXFAP/apc260305.zip",
  "downloaded_at": "2026-03-06T20:54:26+08:00",
  "status": "downloaded"
}
```

### PublicStatus

```json
{
  "running": false,
  "last_checked_at": "2026-03-06T20:54:01+08:00",
  "last_action": "downloaded",
  "latest_remote": null,
  "last_download": null,
  "last_error": null,
  "last_success_at": "2026-03-06T20:54:26+08:00",
  "last_success_age_seconds": 3600,
  "last_success_outcome": "downloaded",
  "last_run_summary": "cli 触发：已下载 apc260305.zip",
  "consecutive_failure_count": 0,
  "failure_cooldown_until": null,
  "failure_cooldown_remaining_seconds": null
}
```

## 公开接口

### `GET /health/ready`

用途：回答服务是否完成初始化且运行目录 / SQLite 可用。

失败时返回：

```json
{
  "ok": false,
  "error": {
    "code": "service_not_ready",
    "message": "服务尚未就绪。"
  }
}
```

### `GET /api/v1/status`

用途：公开状态查询，适合看板与业务状态检查。

说明：公开接口带最小限度的内存级限流；同一来源在短时间内请求过于频繁时会返回 `429 Too Many Requests`。

限流响应示例：

```json
{
  "ok": false,
  "error": {
    "code": "rate_limited",
    "message": "请求过于频繁，请稍后重试。"
  }
}
```

### `GET /api/v1/files/latest/download`

用途：直接下载当前最新的本地 ZIP 文件。

成功响应：`200 OK`

响应体为 ZIP 二进制流，不是 JSON。关键响应头：

```text
Content-Type: application/zip
Content-Disposition: attachment; filename="apc260305.zip"
Cache-Control: public, max-age=0, must-revalidate
ETag: "189a41b90bc55bfa-1ead646"
Last-Modified: Fri, 06 Mar 2026 12:54:26 GMT
X-Content-Type-Options: nosniff
```

支持条件请求：

- `If-None-Match`
- `If-Modified-Since`

命中时返回：`304 Not Modified`

如果同一来源在短时间内请求过于频繁，会返回：`429 Too Many Requests`

无可用文件：

```json
{
  "ok": false,
  "error": {
    "code": "latest_file_not_found",
    "message": "当前没有可下载的最新本地文件。"
  }
}
```

## CLI 契约

### `python3 run_download_latest_once.py`

用途：在不启动 HTTP 服务的情况下执行一次最新文件同步，适合 `cron`、`systemd timer` 或外部调度器。

返回约定：

- 成功时退出码为 `0`
- 失败时退出码为 `1`
- 标准输出打印 JSON envelope
- 结构化运行日志输出到标准错误

成功输出示例：

```json
{
  "ok": true,
  "data": {
    "operation": "sync_latest_file",
    "resource": "files/latest",
    "outcome": "skipped",
    "status": {
      "running": false
    },
    "latest_remote": null,
    "last_download": null,
    "summary": "cli 触发：本地已是最新，跳过最新 ZIP",
    "trigger_policy": {
      "recommended_entrypoint": "run_download_latest_once.py",
      "recommended_mode": "scheduled_cli",
      "note": "常规定时同步请固定调用 run_download_latest_once.py。"
    }
  }
}
```
