# Diting Meeting Service

一个使用 Axum、SQLite 和 Tokio 后台任务实现的会议处理服务。音频保存在本地文件系统，任务状态、Rolling Summary 和 Meeting Board 保存在 SQLite。

## 启动

```bash
cargo run
```

服务默认监听 `http://127.0.0.1:3000`，数据库默认为 `data/meeting.db`，可通过 `DITING_DATABASE_URL` 修改：

```bash
DITING_DATABASE_URL=sqlite://data/custom.db cargo run
```

监听地址可通过 `DITING_ADDR` 修改，例如 `DITING_ADDR=127.0.0.1:3001 cargo run`。

音频请求体默认限制为 100 MiB，可以通过 `DITING_MAX_UPLOAD_BYTES` 调整。

## 接入真实模型

服务支持 OpenAI 兼容接口。ASR 需要提供音频转写接口，LLM 需要提供 Chat Completions 接口；以下配置完整时自动启用远程 provider，未完整配置时使用本地 provider：

```bash
export DITING_ASR_BASE_URL=https://api.example.com/v1
export DITING_ASR_API_KEY=...
export DITING_ASR_MODEL=whisper-1
export DITING_LLM_BASE_URL=https://api.example.com/v1
export DITING_LLM_API_KEY=...
export DITING_LLM_MODEL=your-json-capable-model
cargo run
```

ASR 请求为 `POST {base_url}/audio/transcriptions` multipart，LLM 请求为 `POST {base_url}/chat/completions`。LLM 必须返回 `choices[0].message.content` 中的 `SummaryDocument` JSON。上传时显式提供 `transcript` 会跳过 ASR 请求。

## API

```text
POST /api/v1/meetings
GET  /api/v1/meetings/{id}
DELETE /api/v1/meetings/{id}
POST /api/v1/meetings/{id}/end
POST /api/v1/meetings/{id}/speakers
GET  /api/v1/meetings/{id}/speakers
POST /api/v1/meetings/{id}/segments
GET  /api/v1/meetings/{id}/segments
GET  /api/v1/meetings/{id}/summaries
GET  /api/v1/meetings/{id}/board
GET  /api/v1/meetings/{id}/board/versions
GET  /api/v1/jobs?meeting_id={id}&status=failed
POST /api/v1/jobs/{id}/retry
GET  /health

Swagger UI：`/docs/`，OpenAPI JSON：`/api-docs/openapi.json`。
```

创建会议：

```bash
curl -X POST http://127.0.0.1:3000/api/v1/meetings \
  -H 'content-type: application/json' \
  -d '{"title":"产品周会"}'
```

上传音频分段：

```bash
curl -X POST http://127.0.0.1:3000/api/v1/meetings/$MEETING_ID/segments \
  -F speaker_id=$SPEAKER_ID \
  -F sequence_no=1 \
  -F start_ms=0 \
  -F end_ms=300000 \
  -F transcript='Alice: 本周完成登录模块' \
  -F audio=@sample.wav
```

## 当前处理器

接口、SQLite 任务队列、5 分钟窗口和 Board 版本处理已经打通。上传时可以通过 `transcript` 字段直接提供转写文本；未提供时，当前转写处理器写入 `[transcript provider not configured]` 作为占位文本。`Transcriber` 和 `Summarizer` trait 已经注入 `AppState`，接入实际 ASR 和 LLM 时实现这两个 trait 并替换启动时的 provider 即可，不需要改 Worker 或 API。Summary 使用固定 JSON 结构，Board 会对主题、关键点和行动项去重合并。

Worker 仅消费已经到达 `available_at` 的任务，失败任务最多自动执行三次。服务重启时会自动恢复中断的 `running` 任务；最终失败的任务可以通过 jobs 接口查询并手动重试。

同一窗口内仍有音频正在转写时，Summary 会等待这些转写完成。若已经生成 Summary 后又补传较早时间段的音频，Worker 会创建 `rebuild` 任务，从最早受影响窗口开始重新生成后续 Summary 和 Meeting Board。

同一个受影响窗口的多个迟到分段会合并为一个 `rebuild` 任务；Summary 内容会清理空项、重复项和不支持的行动项状态后再写入 Board。

删除会议会在 SQLite 事务中删除会议、说话人、音频分段、Summary、Board 历史和 jobs，事务成功后删除该会议的本地音频目录。音频目录删除失败不会恢复数据库删除，但会写入错误日志。

转写重试耗尽后分段会标记为 `failed` 且不再阻塞摘要：Worker 会为已完成的部分继续排入后续 Summary；已结束的会议会生成最终 Summary。手动重试任务后该分段会重新参与摘要计算。

## 测试

```bash
cargo test
```

测试不依赖真实 ASR/LLM 服务：provider 层的用例会启动一个返回固定响应的本地 HTTP 服务来模拟 OpenAI 兼容接口；流水线层的用例通过 `FixedTranscriber`/`FixedSummarizer` 桩返回固定文本与固定摘要文档。
