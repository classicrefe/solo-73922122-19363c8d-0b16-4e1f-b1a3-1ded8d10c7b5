# HLS 直播连续录音采集

电台语料采集工具：把直播 HLS 流录成连续录音包，交给离线转写。
FastAPI + SQLite 采集网页，输入媒体清单地址即开始追随录制；停止后把应收片段
全部落盘，产出可离线播放的 ZIP（本地 M3U8 + 原始 TS 片段，不引用远端）。
完成录音还可在网页上按秒数区间遮蔽敏感内容，流式解码 TS 音轨并生成连续的
消音 WAV 供下载。

## 运行

```bash
docker compose up --build
# 网页:  http://localhost:8000
# 直播源: http://localhost:8080
```

网页中可开始录制、停止、查看进度、下载 ZIP；对**已完成**的录音还可填写
按录音起点计的秒数区间（如 `12.5-18, 30-33.2`），生成并下载消音 WAV。
直播源地址示例（网页内也有提示）：

| 用途 | 地址 |
|---|---|
| 普通滑窗直播 | `http://livesource:8080/live.m3u8` |
| 慢下载（每段延迟 1.5s） | `http://livesource:8080/live.m3u8?delay_ms=1500` |
| 缺段（指定序号返回 404） | `http://livesource:8080/live.m3u8?missing=12,13` |
| 窗口跳号（越过未发现序号） | `http://livesource:8080/live.m3u8?jump_at=10&jump_by=8` |
| VOD（带 ENDLIST） | `http://livesource:8080/vod.m3u8` |
| 非法样例（master/加密/不连续） | `/master.m3u8` `/enc.m3u8` `/disc.m3u8` |

## 行为规则（与需求对应）

- **接受的清单**：仅单层媒体清单（拒绝 master）、未加密（拒绝非 NONE 的
  `EXT-X-KEY`）、无不连续标记、MPEG-TS（片段须为 `.ts`，拒绝 `EXT-X-MAP`），
  片段地址在清单内与整个录制任务内均不得复用。
- **追随滑窗**：从首份清单首段（其 `EXT-X-MEDIA-SEQUENCE`）开始，按媒体序号
  判定连续性；新清单起点越过 `last_seq+1` 即判失败并列出缺段序号。
- **停止**：受理时把边界固定为最后已接收清单的尾段并立即持久化，此后不再
  延长范围（拉取途中的清单若晚于停止到达会被丢弃）；重启后边界不变。
- **完成**：边界内全部片段保存后才生成并开放 ZIP；ZIP 内含本地
  `playlist.m3u8`（`EXT-X-ENDLIST`，仅引用包内文件）与原始片段。
- **失败**：应收片段 404、窗口跳号、源违反约束、持续拉取/下载失败均判失败，
  失败任务列出缺段且不提供下载，不会跳过后报成功。
- **持久化**：片段与 ZIP 存持久卷（`/data`，compose 命名卷），SQLite 只存
  进度（任务状态、边界、每段状态）。落盘用临时文件 + 原子 rename；
  重启时对账磁盘与 DB——已落盘未登记的片段收养为完成，登记完成但文件丢失
  的重新下载；已保存片段即使滑出清单也保留并入包。
- **并发**：每路录制为独立的异步轮询 + 下载任务（asyncio/httpx），支持两路
  （及以上）同时录制；慢下载不阻塞另一路、停止受理与查询。消音构建在线程池
  内执行（ffmpeg 子进程 + 分块管道），不阻塞事件循环与网页响应。
- **消音 WAV**：仅对 `done` 录音开放。按 ZIP 内 `playlist.m3u8` 的清单顺序
  逐段用 ffmpeg 解码 TS 音轨为 44.1kHz/16bit/单声道 PCM，按**累计 PCM 样本
  位置**把请求区间（自录音起点计的秒数，可跨片段、可重叠、可超出结尾）置零，
  未遮蔽样本逐字节保持原值，产出一个连续 WAV。全程分块流式处理（64KB 管道
  + 固定大小缓冲），长录音不会一次载入内存。临时文件写满并 fsync 后才原子
  rename 发布；源包缺段、片段为空、ffmpeg 解码失败，或构建期间源 ZIP 被改写
  （大小/mtime/inode 快照变化）一律判失败：不发布音频、删除半成品、请求记为
  `failed` 且不可下载，原 ZIP 下载不受影响。处理中重启会把遗留请求判失败并
  清理 `.part` 文件。

## API

- `POST /api/jobs` `{url}` 开始录制
- `POST /api/jobs/{id}/stop` 停止（固定边界）
- `GET /api/jobs` / `GET /api/jobs/{id}` 进度（状态、窗口、缺段、错误）
- `GET /api/jobs/{id}/download` 下载 ZIP（未完成返回 409）
- `POST /api/jobs/{id}/mute` `{intervals:[{start,end},...]}` 提交消音（秒，
  仅完成录音；返回 201 与消音任务，状态 `processing → ready/failed`）
- `GET /api/jobs/{id}/mutes` 该录音的全部消音请求
- `GET /api/mutes/{mid}` 单个消音请求状态（含总/置零样本数、截断提示）
- `GET /api/mutes/{mid}/download` 下载消音 WAV（未就绪/失败返回 409）

任务状态：`recording` →（停止）→ `finalizing` → `done` / `failed`。
消音状态：`processing` → `ready` / `failed`（失败原因在 `error` 字段）。

## 目录

```
app/          采集网页与录制引擎（FastAPI）
  src/playlist.py     严格清单解析与校验
  src/recorder.py     轮询、下载、定界、打包、重启恢复
  src/mute.py         流式 TS→PCM 解码、按累计样本位置置零、WAV 打包与篡改检测
  src/mute_manager.py 消音线程池、原子发布、失败清理与重启对账
  src/db.py           SQLite 进度存储（含 mute_jobs 表）
livesource/   本地伪直播源（滑窗/延迟/缺段/跳号/非法清单样例）
docker-compose.yml
```

## 测试

本仓库代码已在本地以与容器相同的命令（`uvicorn src.main:app`）完成端到端
自测：基本录制-停止-打包、ZIP 内容校验（本地清单/连续序号/无远端引用/
片段字节一致/ffmpeg 可解码）、两路并发与慢源不阻塞、缺段 404 失败、窗口
跳号失败、非法清单拒绝、ENDLIST 自动定界、崩溃（`os._exit`）后重启续录、
停止边界重启不变、落盘与登记之间崩溃的收养恢复（26 项），片段地址复用与
ZIP ffmpeg 解码（3 项），以及消音功能 46 项——请求校验、单/多/跨片段/超尾
区间逐样本置零、未遮蔽样本与源 PCM 逐字节一致、长录音（40 段）流式构建且
Python 峰值内存 <4MB、源包缺段/空段/损坏 TS 解码失败、构建中 ZIP 被改写
检测、失败后无半成品且原 ZIP 照常下载、并发消音、重启清理与 ffmpeg 可播放，
全部通过。

测试钩子（仅 `RECORDER_TEST_HOOKS=1` 时挂载，compose 不启用）：
`POST /api/_test/crash` 立即崩溃；`POST /api/_test/arm_after_write/{job}/{seq}`
在指定片段落盘后、登记前崩溃；`POST /api/_test/arm_mute_tamper/{job}`
让下一次消音构建中途截断源 ZIP；`POST /api/_test/restore_zip/{job}` 恢复。

## 已知限制

- 单进程部署（任务状态在内存中，重启时从 SQLite + 磁盘恢复）；不支持多
  worker 水平扩展。
- 片段按字节原样保存，不做码流级完整性校验。
- 非 404 的持续性下载错误重试 5 次后判失败；清单持续拉取失败 15 次后判失败。
- 本地直播源各片段内容相同（种子片段循环），时间戳每段重置；ffmpeg 解码
  校验通过，拼接播放时个别播放器可能提示时间戳不连续。
