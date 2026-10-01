//! Summary 文档结构、规范化与 Meeting Board 合并。
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Serialize, Deserialize, Clone, Default, utoipa::ToSchema)]
pub(crate) struct SummaryDocument {
    /// 讨论主题
    #[serde(default)]
    pub topics: Vec<String>,
    /// 已确认的决策
    #[serde(default)]
    pub decisions: Vec<String>,
    /// 待执行的行动项
    #[serde(default)]
    pub action_items: Vec<ActionItem>,
    /// 尚未解决的问题
    #[serde(default)]
    pub open_questions: Vec<String>,
    /// 会议风险
    #[serde(default)]
    pub risks: Vec<String>,
    /// 关键点
    #[serde(default)]
    pub key_points: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, utoipa::ToSchema)]
pub(crate) struct ActionItem {
    /// 行动项内容
    pub content: String,
    /// 负责人
    #[serde(default)]
    pub owner: Option<String>,
    /// 截止日期，通常为 ISO 日期
    #[serde(default)]
    pub due_date: Option<String>,
    /// open、in_progress、done 或 blocked
    #[serde(default = "default_action_status")]
    pub status: String,
}

pub(crate) fn default_action_status() -> String {
    "open".into()
}

pub(crate) fn empty_board() -> Value {
    json!({"topics":[],"decisions":[],"action_items":[],"open_questions":[],"risks":[],"key_points":[]})
}

pub(crate) fn merge_board(board: &mut Value, summary: &SummaryDocument) {
    for (field, values) in [
        ("topics", &summary.topics),
        ("decisions", &summary.decisions),
        ("open_questions", &summary.open_questions),
        ("risks", &summary.risks),
        ("key_points", &summary.key_points),
    ] {
        let target = array_field(board, field);
        for value in values {
            if !target.iter().any(|item| item.as_str() == Some(value)) {
                target.push(Value::String(value.clone()));
            }
        }
    }
    let action_items = array_field(board, "action_items");
    for item in &summary.action_items {
        let exists = action_items.iter().any(|existing| {
            existing.get("content").and_then(Value::as_str) == Some(item.content.as_str())
        });
        if !exists {
            if let Ok(value) = serde_json::to_value(item) {
                action_items.push(value);
            }
        }
    }
}

fn array_field<'a>(board: &'a mut Value, field: &str) -> &'a mut Vec<Value> {
    if !board.get(field).is_some_and(Value::is_array) {
        board[field] = json!([]);
    }
    board
        .get_mut(field)
        .and_then(Value::as_array_mut)
        .expect("field was normalized to an array")
}

pub(crate) fn normalize_summary(mut summary: SummaryDocument) -> SummaryDocument {
    fn normalize_list(values: &mut Vec<String>) {
        let mut normalized = Vec::new();
        for value in values.drain(..) {
            let value = value.trim();
            if !value.is_empty()
                && !normalized
                    .iter()
                    .any(|existing: &String| existing.eq_ignore_ascii_case(value))
            {
                normalized.push(value.to_string());
            }
            if normalized.len() == 100 {
                break;
            }
        }
        *values = normalized;
    }
    for values in [
        &mut summary.topics,
        &mut summary.decisions,
        &mut summary.open_questions,
        &mut summary.risks,
        &mut summary.key_points,
    ] {
        normalize_list(values);
    }
    let mut actions = Vec::new();
    for mut item in summary.action_items.drain(..) {
        item.content = item.content.trim().to_string();
        if item.content.is_empty()
            || actions
                .iter()
                .any(|existing: &ActionItem| existing.content.eq_ignore_ascii_case(&item.content))
        {
            continue;
        }
        item.owner = item.owner.and_then(trimmed_option);
        item.due_date = item.due_date.and_then(trimmed_option);
        item.status = item.status.trim().to_string();
        if !matches!(
            item.status.as_str(),
            "open" | "in_progress" | "done" | "blocked"
        ) {
            item.status = default_action_status();
        }
        actions.push(item);
        if actions.len() == 100 {
            break;
        }
    }
    summary.action_items = actions;
    summary
}

fn trimmed_option(value: String) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}
