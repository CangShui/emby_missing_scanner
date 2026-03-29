# Emby Missing Scanner

一个用于扫描 Emby 动漫库缺失季/集的 Python 3 工具。  
它会对比 Emby 现有数据与 TMDB 官方元数据，输出“缺失作品”，并可生成本地 Web 页面进行可视化查看。

## 功能概览

- 从 Emby 读取指定媒体库的全部剧集（默认 `Anime`）
- 从 TMDB 获取季/集基准数据
- 检测并输出：
  - 缺失整季
  - 缺失单集
- 支持跳过指定作品（按名称/Emby ID）
- 支持进度条、详细日志
- 支持本地 Web 界面（海报墙 + 缺失集详情 + Emby/TMDB 跳转按钮）
- 支持 TMDB API 缓存与图片缓存（可独立 TTL）
- 支持中英双语（`zh-CN` / `en-US`）

---

## 运行环境

- Windows x86_64（开发和验证环境）
- Python 3.11+
- 运行时依赖：仅 Python 标准库（`requirements.txt` 无第三方包）

## 目录结构

- `emby_missing_scanner.py`: 主程序
- `tests/test_emby_missing_scanner.py`: 单元测试
- `requirements.txt`: 依赖说明（标准库）

---

## 快速开始

```powershell
python .\emby_missing_scanner.py
```

首次运行会自动创建配置文件：

`C:\Users\Public\emby_scan.json`

首次语言默认策略：

- 系统语言为中文 -> `zh-CN`
- 否则 -> `en-US`

---

## 命令行参数

```powershell
python .\emby_missing_scanner.py [options]
```

| 参数 | 说明 |
|---|---|
| `--config` | 配置文件路径（默认 `C:\Users\Public\emby_scan.json`） |
| `--timeout` | 临时覆盖配置中的请求超时（秒） |
| `--max-series` | 临时限制扫描作品数量（调试用） |
| `--max-lookup-errors` | 临时限制 TMDB 错误上限（达到后提前停止） |
| `--include-specials` | 临时包含 Season 0（特典） |
| `--log-file` | 临时覆盖日志文件路径（空字符串可关闭） |
| `--no-progress` | 临时关闭进度条 |
| `--web-port` | 临时覆盖 Web 端口 |
| `--no-web` | 本次不启动 Web 服务 |

---

## 配置文件说明（`C:\Users\Public\emby_scan.json`）

> 建议优先改配置文件，CLI 参数仅用于临时覆盖。

| 键 | 类型 | 说明 |
|---|---|---|
| `emby_url` | string | Emby 地址 |
| `emby_api_key` | string | Emby API Key |
| `library_name` | string | Emby 库名 |
| `tmdb_bearer` | string | TMDB Read Access Token |
| `tmdb_api_key` | string | TMDB API Key（Bearer 失败时可回退） |
| `language` | string | `zh-CN` 或 `en-US` |
| `timeout` | number | HTTP 超时（秒） |
| `include_specials` | bool | 是否包含 Season 0 |
| `include_unaired` | bool | 是否包含未播出内容 |
| `max_series` | number/null | 扫描上限（`null` 为全量） |
| `max_lookup_errors` | number/null | 错误上限（`null` 为不限） |
| `show_progress` | bool | 是否显示进度条 |
| `log_file` | string | 日志文件路径 |
| `skip_series_names` | string[] | 按名称跳过 |
| `skip_series_ids` | string[] | 按 Emby ID 跳过 |
| `concurrency_workers` | number | 并发扫描线程数 |
| `tmdb_max_retries` | number | TMDB 请求最大重试次数 |
| `tmdb_retry_delay` | number | TMDB 请求重试间隔（秒） |
| `cache_dir` | string | 缓存与 Web 文件根目录 |
| `tmdb_api_cache_ttl_hours` | number | API 缓存 TTL（小时） |
| `tmdb_image_cache_ttl_hours` | number | 图片缓存 TTL（小时） |
| `cache_images` | bool | 是否缓存图片到本地 |
| `web_enabled` | bool | 扫描完成后是否启动 Web |
| `web_host` | string | Web 绑定地址（默认 `127.0.0.1`） |
| `web_port` | number | Web 端口 |

---

## 匹配与缺失判定逻辑

### 1) 作品匹配

- 优先使用 Emby `ProviderIds.Tmdb`
- 若没有 TMDB ID，则用标题 + 年份搜索 TMDB TV

### 2) 季有效性判定

- 若 `include_specials=false`，默认跳过 Season 0
- 若 `include_unaired=false`：
  - 首集无 `air_date` -> 整季跳过
  - 首集 `air_date` 晚于今天 -> 整季跳过

### 3) 集有效性判定（核心）

程序会优先使用配置语言（如 `zh-CN`）数据。  
仅当本地语言信息不足时，才按需拉取 `en-US` 回退数据。

某集会被视为“有效存在”并参与缺失比对，当它满足：

- 已播出（`air_date <= 今天`）
- 且不是“占位/无效条目”

占位/无效条目（会被跳过）包括：

- 中英都无有效简介，且没有 `still_path`
- 或弱占位特征命中：
  - 简介为空
  - 标题为泛化命名（如 `Episode 13` / `第13集`）
  - `runtime` 为空或 0
  - `vote_count` 为 0

> 注意：若 `zh-CN` 简介缺失但 `en-US` 简介有效，会自动回退使用 `en-US`，并记日志。

### 4) 缺失判定

- 缺失季：TMDB 有该季，Emby 没有该季
- 缺失集：同季下 TMDB 有该集，Emby 没有该集

---

## 排除逻辑

### 用户显式排除

- `skip_series_names` 命中 -> 该作品不扫描
- `skip_series_ids` 命中 -> 该作品不扫描

### 系统自动排除

- 未播出季（或首集无播出日期）
- 占位/无效集（见上文）

---

## 缓存机制

缓存目录（默认）：

`C:\Users\Public\emby_scan\cache`

- API 缓存：`cache/api/*.json`
- 图片缓存：`cache/images/*`

### TTL 行为

- `tmdb_api_cache_ttl_hours > 0`: 命中有效缓存则直接读取
- `tmdb_image_cache_ttl_hours > 0`: 图片按 TTL 命中复用

### TTL 为 0 的清理机制

当以下值为 `0` 时：

- `tmdb_api_cache_ttl_hours = 0`
- `tmdb_image_cache_ttl_hours = 0`

程序启动时会自动清空对应缓存目录，并且本次运行不再写入该类缓存。

### `cache_images=false`

- 不写本地图片缓存
- Web 页面直接使用 TMDB 图片 URL

---

## 日志说明

默认日志文件：`emby_scan.log`

典型日志类型：

- `Scan started / Scan completed`
- `Missing detected`
- `Skipped by user config`
- `Ignored unaired/unconfirmed season`
- `Ignored TMDB placeholder episode`
- `Used TMDB en-US fallback overview`
- `TMDB cache hit`

---

## Web 界面

默认地址：

`http://127.0.0.1:8765`

页面结构：

- 首页：仅展示“存在缺失”的作品
- 二级页：展示该作品缺失集详情（图、简介、播出日期）
- 悬浮按钮：
  - 打开 Emby（`/web/index.html#!/details?id=...&serverId=...`）
  - 打开 TMDB 对应集页面

---

## 测试与构建

```powershell
python -m compileall .
python -m unittest discover -s tests -v
```

---

## 安全建议

- 不要把生产环境 `emby_api_key` / `tmdb_bearer` 提交到公开仓库
- 推荐在发布到 GitHub 前，把配置示例中的密钥替换为占位符
