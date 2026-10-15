use super::*;

/// Where a human-input request stands. Only a pending request can be
/// answered; the others are final.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InputRequestStatus {
    Pending,
    Answered,
    /// The deadline passed unanswered, after any escalation.
    Expired,
    /// The waiting program call was withdrawn.
    Cancelled,
}

impl InputRequestStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Answered => "answered",
            Self::Expired => "expired",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        [
            Self::Pending,
            Self::Answered,
            Self::Expired,
            Self::Cancelled,
        ]
        .into_iter()
        .find(|status| status.as_str() == value)
    }
}

/// The recorded answer to a request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InputRequestAnswer {
    pub approved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub answered_by: Attribution,
    pub answered_at_ms: u64,
}

/// A question a program run asked people, as the universe's inbox shows it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InputRequestView {
    pub request_id: String,
    /// The program session whose run is waiting.
    pub session_id: String,
    pub run_id: u64,
    pub ask: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// JSON Schema an approval's `value` must satisfy. Without one, an
    /// answer is an approval or a rejection with an optional comment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_schema: Option<Value>,
    pub assignees: Vec<String>,
    /// Who takes the request over, once, if its deadline passes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escalate_to: Vec<String>,
    /// Whether the request has been escalated to `escalateTo`.
    pub escalated: bool,
    pub status: InputRequestStatus,
    pub created_at_ms: u64,
    pub deadline_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remind_after_ms: Option<u64>,
    pub reminder_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reminded_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<InputRequestAnswer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InboxRequestListParams {
    /// Only requests in this state; every state when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<InputRequestStatus>,
    /// Only requests from this session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    /// At most this many requests, newest first. Defaults to 50.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 200))]
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InboxRequestListResponse {
    pub requests: Vec<InputRequestView>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InboxRequestReadParams {
    pub request_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InboxRequestResponse {
    pub request: InputRequestView,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InboxRequestAnswerParams {
    pub request_id: String,
    /// Approve or reject the request.
    pub approved: bool,
    /// An approval's value: required exactly when the request has a
    /// response schema, and it must satisfy it. A rejection has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}
