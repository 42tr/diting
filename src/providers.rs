//! ASR / LLM provider 抽象与 OpenAI 兼容实现。Worker 只依赖 trait，测试可注入桩实现。
use crate::summary::SummaryDocument;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tracing::{info, warn};

/// 允许的分段语义类型（与上游 CoAssist 的语义标签枚举保持一致）。
pub(crate) const SEMANTIC_TYPES: [&str; 5] = ["decision", "report", "question", "action", "other"];

/// 日志中打印请求/响应体的最大字符数，超出部分截断。
const MAX_LOG_BODY_CHARS: usize = 2000;

#[async_trait]
pub(crate) trait Transcriber: Send + Sync {
    async fn transcribe(&self, file_path: &str, existing: Option<&str>) -> Result<String, String>;
}

#[async_trait]
pub(crate) trait Summarizer: Send + Sync {
    async fn summarize(
        &self,
        start_ms: i64,
        end_ms: i64,
        transcript: &str,
    ) -> Result<SummaryDocument, String>;
}

#[async_trait]
pub(crate) trait Classifier: Send + Sync {
    /// 判断转写片段的语义类型，返回 SEMANTIC_TYPES 之一。
    async fn classify(&self, transcript: &str) -> Result<String, String>;
}

#[async_trait]
pub(crate) trait Corrector: Send + Sync {
    /// 校对转写文本（同音错字、重复词、标点），不改写内容。
    async fn correct(&self, text: &str) -> Result<String, String>;
}

/// 读取环境变量；未设置或只含空白都视为未配置。
pub(crate) fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

pub(crate) struct Providers {
    pub transcriber: Arc<dyn Transcriber>,
    pub summarizer: Arc<dyn Summarizer>,
    pub classifier: Arc<dyn Classifier>,
    /// 仅在 DITING_CORRECTION_ENABLED 打开且 LLM 已配置时存在。
    pub corrector: Option<Arc<dyn Corrector>>,
}

impl Providers {
    pub fn from_env(client: reqwest::Client) -> Self {
        let transcriber: Arc<dyn Transcriber> = match (
            env_nonempty("DITING_ASR_BASE_URL"),
            env_nonempty("DITING_ASR_API_KEY"),
            env_nonempty("DITING_ASR_MODEL"),
        ) {
            (Some(base_url), Some(api_key), Some(model)) => Arc::new(OpenAiTranscriber {
                client: client.clone(),
                base_url,
                api_key,
                model,
            }),
            _ => {
                warn!("DITING_ASR_BASE_URL/DITING_ASR_API_KEY/DITING_ASR_MODEL not fully set: transcription will output placeholder text '[transcript provider not configured]'");
                Arc::new(LocalTranscriber)
            }
        };
        let chat = match (
            env_nonempty("DITING_LLM_BASE_URL"),
            env_nonempty("DITING_LLM_API_KEY"),
            env_nonempty("DITING_LLM_MODEL"),
        ) {
            (Some(base_url), Some(api_key), Some(model)) => Some(ChatClient {
                client,
                base_url,
                api_key,
                model,
                timeout: Duration::from_secs(120),
            }),
            _ => {
                warn!("DITING_LLM_BASE_URL/DITING_LLM_API_KEY/DITING_LLM_MODEL not fully set: summaries will use local placeholder, segment classification will output 'other'");
                None
            }
        };
        let correction_requested =
            env_nonempty("DITING_CORRECTION_ENABLED").is_some_and(|v| v == "true" || v == "1");
        if correction_requested && chat.is_none() {
            warn!("DITING_CORRECTION_ENABLED is set but the LLM provider is not configured: correction disabled");
        }
        info!(
            asr_provider = if env_nonempty("DITING_ASR_BASE_URL").is_some() {
                "openai-compatible"
            } else {
                "local"
            },
            summarizer_provider = if chat.is_some() {
                "openai-compatible"
            } else {
                "local"
            },
            correction = correction_requested && chat.is_some(),
            "processing providers configured"
        );
        match chat {
            Some(chat) => Self {
                transcriber,
                summarizer: Arc::new(OpenAiSummarizer(chat.clone())),
                classifier: Arc::new(OpenAiClassifier(chat.clone())),
                corrector: correction_requested.then(|| {
                    Arc::new(OpenAiCorrector(ChatClient {
                        timeout: Duration::from_secs(30),
                        ..chat
                    })) as Arc<dyn Corrector>
                }),
            },
            None => Self {
                transcriber,
                summarizer: Arc::new(LocalSummarizer),
                classifier: Arc::new(LocalClassifier),
                corrector: None,
            },
        }
    }
}

#[derive(Clone)]
pub(crate) struct OpenAiTranscriber {
    pub client: reqwest::Client,
    pub base_url: String,
    pub api_key: String,
    pub model: String,
}

#[async_trait]
impl Transcriber for OpenAiTranscriber {
    async fn transcribe(&self, file_path: &str, existing: Option<&str>) -> Result<String, String> {
        if let Some(text) = existing.map(str::trim).filter(|text| !text.is_empty()) {
            return Ok(text.to_string());
        }
        let bytes = tokio::fs::read(file_path)
            .await
            .map_err(|e| e.to_string())?;
        let filename = std::path::Path::new(file_path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("audio.bin")
            .to_string();
        let part = reqwest::multipart::Part::bytes(bytes).file_name(filename.clone());
        let form = reqwest::multipart::Form::new()
            .text("model", self.model.clone())
            .part("file", part);
        let response = self
            .client
            .post(format!(
                "{}/audio/transcriptions",
                self.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.api_key)
            .multipart(form)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = response.status();
        let body: Value = response.json().await.map_err(|e| e.to_string())?;
        info!(
            status = %status,
            model = %self.model,
            file = %filename,
            response = %truncate_for_log(&body.to_string(), MAX_LOG_BODY_CHARS),
            "ASR provider response"
        );
        if !status.is_success() {
            return Err(format!("ASR provider returned {}: {}", status, body));
        }
        body.get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| "ASR response does not contain text".into())
    }
}

/// OpenAI 兼容的 Chat Completions 客户端，Summary / 分类 / 校对共用。
#[derive(Clone)]
pub(crate) struct ChatClient {
    pub client: reqwest::Client,
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub timeout: Duration,
}

impl ChatClient {
    /// 返回 `choices[0].message.content`；`json_mode` 时要求模型输出 JSON 对象。
    async fn complete(&self, system: &str, user: &str, json_mode: bool) -> Result<String, String> {
        let mut request = json!({
            "model": self.model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user}
            ]
        });
        if json_mode {
            request["response_format"] = json!({"type": "json_object"});
        }
        let response = self
            .client
            .post(format!(
                "{}/chat/completions",
                self.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.api_key)
            .timeout(self.timeout)
            .json(&request)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = response.status();
        let body: Value = response.json().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("LLM provider returned {}: {}", status, body));
        }
        body.pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| "LLM response does not contain choices[0].message.content".to_string())
    }
}

/// 去掉模型偶尔包裹的 Markdown 代码块（```json … ``` 或 ``` … ```）。
pub(crate) fn strip_code_fence(content: &str) -> &str {
    let trimmed = content.trim();
    let Some(rest) = trimmed.strip_prefix("```") else {
        return trimmed;
    };
    let Some(body) = rest.strip_suffix("```") else {
        return trimmed;
    };
    // 首行是可选的语言标记（json、JSON 等）。
    match body.split_once('\n') {
        Some((lang, inner)) if !lang.trim().contains(['{', '[']) => inner.trim(),
        _ => body.trim(),
    }
}

pub(crate) struct OpenAiSummarizer(pub ChatClient);

#[async_trait]
impl Summarizer for OpenAiSummarizer {
    async fn summarize(
        &self,
        start_ms: i64,
        end_ms: i64,
        transcript: &str,
    ) -> Result<SummaryDocument, String> {
        let system = "Return only valid JSON matching this schema: {\"topics\":[],\"decisions\":[],\"action_items\":[{\"content\":\"\",\"owner\":null,\"due_date\":null,\"status\":\"open\"}],\"open_questions\":[],\"risks\":[],\"key_points\":[]}. Extract facts from the meeting transcript. Do not invent details. 无论会议转写使用何种语言，摘要内容必须使用中文，包括讨论主题、决策、行动项内容、待解答问题、风险和关键点。人名、专有名词可保留原文；JSON 字段名、status 枚举值和日期格式保持上述 schema 的要求。";
        let user = format!("Meeting window {start_ms}-{end_ms} ms.\nTranscript:\n{transcript}");
        let content = self.0.complete(system, &user, true).await?;
        serde_json::from_str(strip_code_fence(&content))
            .map_err(|e| format!("invalid SummaryDocument JSON: {e}"))
    }
}

pub(crate) struct OpenAiClassifier(pub ChatClient);

#[async_trait]
impl Classifier for OpenAiClassifier {
    async fn classify(&self, transcript: &str) -> Result<String, String> {
        let system = "判断会议发言片段的语义类型，只返回 JSON：{\"semantic_type\":\"...\"}。取值：decision=达成结论或决定，report=汇报进展或情况，question=提问或待解答，action=指派任务或后续行动，other=其他。不要输出多余内容。";
        let content = self.0.complete(system, transcript, true).await?;
        let parsed: Value = serde_json::from_str(strip_code_fence(&content))
            .map_err(|e| format!("invalid classifier JSON: {e}"))?;
        Ok(parsed
            .get("semantic_type")
            .and_then(Value::as_str)
            .map(str::to_lowercase)
            .filter(|value| SEMANTIC_TYPES.contains(&value.as_str()))
            .unwrap_or_else(|| "other".to_string()))
    }
}

pub(crate) struct OpenAiCorrector(pub ChatClient);

#[async_trait]
impl Corrector for OpenAiCorrector {
    async fn correct(&self, text: &str) -> Result<String, String> {
        let system = "你是会议转写校对器。只修正明确的同音错字、重复词和标点；保持原意、数字、专有名词与事实。不得总结、增补、改写或添加说话人。只输出校对后的正文。";
        let content = self.0.complete(system, text, false).await?;
        Some(content.trim())
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| "empty correction".into())
    }
}

pub(crate) struct LocalTranscriber;

#[async_trait]
impl Transcriber for LocalTranscriber {
    async fn transcribe(&self, _file_path: &str, existing: Option<&str>) -> Result<String, String> {
        match existing.map(str::trim).filter(|text| !text.is_empty()) {
            Some(text) => Ok(text.to_string()),
            None => Ok("[transcript provider not configured]".to_string()),
        }
    }
}

pub(crate) struct LocalSummarizer;

#[async_trait]
impl Summarizer for LocalSummarizer {
    async fn summarize(
        &self,
        _start_ms: i64,
        _end_ms: i64,
        transcript: &str,
    ) -> Result<SummaryDocument, String> {
        let points: Vec<String> = transcript
            .split(['。', '.', '\n'])
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        Ok(SummaryDocument {
            key_points: points,
            ..SummaryDocument::default()
        })
    }
}

pub(crate) struct LocalClassifier;

#[async_trait]
impl Classifier for LocalClassifier {
    async fn classify(&self, _transcript: &str) -> Result<String, String> {
        Ok("other".to_string())
    }
}

/// 截断过长文本用于日志输出。
pub(crate) fn truncate_for_log(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max_chars).collect();
    out.push_str("...(truncated)");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_code_fence_handles_common_wrappers() {
        assert_eq!(strip_code_fence("```json\n{\"a\":1}\n```"), "{\"a\":1}");
        assert_eq!(strip_code_fence("```\n{\"a\":1}\n```"), "{\"a\":1}");
        assert_eq!(strip_code_fence("```{\"a\":1}```"), "{\"a\":1}");
        assert_eq!(strip_code_fence("  {\"a\":1} "), "{\"a\":1}");
    }
}
