//! Human input for programs: `input.request` asks people a question and
//! completes with their answer. It is a system workflow-tool binding with a
//! joined reply: the input-request workflow records the request in the
//! universe's inbox, waits durably for an answer or its deadline, and
//! resolves the call. Only program sessions bind it; no model presents it.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{ToolError, ToolResult};

pub const INPUT_REQUEST_TOOL_ID: &str = "input.request";
pub const INPUT_REQUEST_TOOL_NAME: &str = "input_request";
pub const INPUT_REQUEST_WORKFLOW_TOOL_ID: &str = "input-request";
pub const INPUT_REQUEST_WORKFLOW_SEMANTIC_TYPE: &str = "lightspeed.input.request.v1";
pub const INPUT_REQUEST_WORKFLOW_TYPE: &str = "InputRequestWorkflow";

const DAY_MS: u64 = 24 * 60 * 60 * 1_000;
/// How long a request waits when neither the call nor the program sets a
/// deadline.
pub const DEFAULT_INPUT_DEADLINE_MS: u64 = 7 * DAY_MS;
/// The longest deadline a request may ask for, so a two-week absence fits.
pub const MAX_INPUT_DEADLINE_MS: u64 = 30 * DAY_MS;
pub const MIN_INPUT_DEADLINE_MS: u64 = 1_000;
pub const MIN_REMIND_AFTER_MS: u64 = 60 * 1_000;
/// The binding's joined ceiling: a request may wait out its own deadline
/// and, once escalated, a fresh one.
pub const INPUT_REQUEST_DEADLINE_CEILING_MS: u64 = 2 * MAX_INPUT_DEADLINE_MS + DAY_MS;

pub const MAX_ASK_BYTES: usize = 4 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_RESPONSE_SCHEMA_BYTES: usize = 64 * 1024;
pub const MAX_ASSIGNEES: usize = 32;
pub const MAX_ASSIGNEE_BYTES: usize = 200;
pub const MAX_COMMENT_BYTES: usize = 16 * 1024;
pub const MAX_ANSWER_VALUE_BYTES: usize = 256 * 1024;

/// The failure code of a request whose deadline passed unanswered. A
/// program sees it as the thrown error's `kind`.
pub const DEADLINE_EXCEEDED_CODE: &str = "deadline_exceeded";
/// The failure code of a request withdrawn before it was answered.
pub const INPUT_CANCELLED_CODE: &str = "input_cancelled";

/// What a program asks for.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputRequestArgs {
    /// The question, shown to the assignees.
    pub ask: String,
    /// Context shown with the question: any JSON value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// JSON Schema an approving answer's `value` must satisfy. Without one,
    /// an answer is an approval or a rejection with an optional comment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_schema: Option<Value>,
    /// Who should answer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assignees: Vec<String>,
    /// How long to wait for an answer, in milliseconds. Defaults to the
    /// program's input deadline, at most 30 days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1000, max = 2592000000_u64))]
    pub deadline_ms: Option<u64>,
    /// Remind the assignees on this interval until the request is answered
    /// or its deadline passes. At least one minute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 60000))]
    pub remind_after_ms: Option<u64>,
    /// Who takes the request over, once and with a fresh deadline, when the
    /// deadline passes unanswered. Without it the call fails with
    /// `deadline_exceeded`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escalate_to: Vec<String>,
}

impl InputRequestArgs {
    pub fn validate(&self) -> ToolResult<()> {
        if self.ask.trim().is_empty() || self.ask.len() > MAX_ASK_BYTES {
            return Err(invalid(format!(
                "ask must be non-empty and at most {MAX_ASK_BYTES} bytes"
            )));
        }
        if let Some(payload) = &self.payload
            && json_len(payload) > MAX_PAYLOAD_BYTES
        {
            return Err(invalid(format!(
                "payload must be at most {MAX_PAYLOAD_BYTES} bytes of JSON"
            )));
        }
        if let Some(schema) = &self.response_schema {
            if json_len(schema) > MAX_RESPONSE_SCHEMA_BYTES {
                return Err(invalid(format!(
                    "response_schema must be at most {MAX_RESPONSE_SCHEMA_BYTES} bytes of JSON"
                )));
            }
            jsonschema::validator_for(schema).map_err(|error| {
                invalid(format!("response_schema is not a JSON Schema: {error}"))
            })?;
        }
        for (field, people) in [
            ("assignees", &self.assignees),
            ("escalate_to", &self.escalate_to),
        ] {
            validate_people(field, people)?;
        }
        if let Some(deadline) = self.deadline_ms
            && !(MIN_INPUT_DEADLINE_MS..=MAX_INPUT_DEADLINE_MS).contains(&deadline)
        {
            return Err(invalid(format!(
                "deadline_ms must be between {MIN_INPUT_DEADLINE_MS} and {MAX_INPUT_DEADLINE_MS}"
            )));
        }
        if self
            .remind_after_ms
            .is_some_and(|interval| interval < MIN_REMIND_AFTER_MS)
        {
            return Err(invalid(format!(
                "remind_after_ms must be at least {MIN_REMIND_AFTER_MS}"
            )));
        }
        Ok(())
    }

    /// The request's wait: its own deadline, else the program's default,
    /// never beyond the cap.
    pub fn deadline_after_ms(&self, program_default: Option<u64>) -> u64 {
        self.deadline_ms
            .or(program_default)
            .unwrap_or(DEFAULT_INPUT_DEADLINE_MS)
            .clamp(MIN_INPUT_DEADLINE_MS, MAX_INPUT_DEADLINE_MS)
    }
}

/// What the call returns once someone answers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InputAnswer {
    /// Whether the request was approved; a rejection is an answer too.
    pub approved: bool,
    /// An approving answer's value, present when the request has a
    /// response schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Who answered, as the inbox recorded them.
    pub answered_by: String,
    /// When the answer was recorded, in Unix milliseconds.
    pub answered_at_ms: u64,
}

/// Check a decision against its request: an approval carries a value
/// exactly when the request has a response schema, and that value must
/// satisfy it; a rejection carries none.
pub fn validate_decision(
    response_schema: Option<&Value>,
    approved: bool,
    value: Option<&Value>,
    comment: Option<&str>,
) -> ToolResult<()> {
    if comment.is_some_and(|comment| comment.len() > MAX_COMMENT_BYTES) {
        return Err(invalid(format!(
            "comment must be at most {MAX_COMMENT_BYTES} bytes"
        )));
    }
    match (approved, response_schema, value) {
        (true, Some(schema), Some(value)) => {
            if json_len(value) > MAX_ANSWER_VALUE_BYTES {
                return Err(invalid(format!(
                    "value must be at most {MAX_ANSWER_VALUE_BYTES} bytes of JSON"
                )));
            }
            let validator = jsonschema::validator_for(schema).map_err(|error| {
                invalid(format!("request has an invalid response schema: {error}"))
            })?;
            validator.validate(value).map_err(|error| {
                invalid(format!("value does not match the response schema: {error}"))
            })
        }
        (true, Some(_), None) => Err(invalid(
            "an approval of this request must carry a value matching its response schema",
        )),
        (true, None, Some(_)) => Err(invalid(
            "this request has no response schema; approve it without a value",
        )),
        (false, _, Some(_)) => Err(invalid("a rejection carries no value")),
        (_, _, None) => Ok(()),
    }
}

/// A failed request's error document. The program runtime raises it with
/// `code` as the error's kind.
pub fn failure_document(code: &str, message: &str) -> Value {
    serde_json::json!({ "code": code, "message": message })
}

/// The `input.request` definition: its arguments and the answer it returns.
pub fn input_request_definition() -> crate::runtime::FunctionDefinition {
    crate::runtime::FunctionDefinition::new(
        INPUT_REQUEST_TOOL_NAME,
        "Ask people a question and wait durably for their answer. The call completes with the answer, or fails with kind deadline_exceeded once the deadline (and any escalation) passes unanswered.",
        crate::definitions::input_schema_for::<InputRequestArgs>(),
    )
    .with_output_schema(crate::definitions::output_schema_for::<InputAnswer>())
}

/// The session tool behind the `input.request` binding.
pub fn input_request_tool_spec() -> harness::ToolSpec {
    crate::definitions::register(
        INPUT_REQUEST_TOOL_ID,
        Default::default(),
        harness::ToolParallelism::ParallelSafe,
        Default::default(),
    )
}

pub fn is_input_request_binding(tool_id: &str, semantic_type: &str) -> bool {
    tool_id == INPUT_REQUEST_WORKFLOW_TOOL_ID
        && semantic_type == INPUT_REQUEST_WORKFLOW_SEMANTIC_TYPE
}

fn validate_people(field: &str, people: &[String]) -> ToolResult<()> {
    if people.len() > MAX_ASSIGNEES {
        return Err(invalid(format!(
            "{field} may name at most {MAX_ASSIGNEES} people"
        )));
    }
    if people.iter().any(|person| {
        person.trim().is_empty() || person.trim() != person || person.len() > MAX_ASSIGNEE_BYTES
    }) {
        return Err(invalid(format!(
            "{field} entries must be 1..={MAX_ASSIGNEE_BYTES} bytes without surrounding whitespace"
        )));
    }
    Ok(())
}

fn json_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |bytes| bytes.len())
}

fn invalid(message: impl Into<String>) -> ToolError {
    ToolError::InvalidRequest {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn args() -> InputRequestArgs {
        InputRequestArgs {
            ask: "Merge?".into(),
            payload: Some(json!({"pr": 7})),
            response_schema: None,
            assignees: vec!["alice".into()],
            deadline_ms: None,
            remind_after_ms: None,
            escalate_to: Vec::new(),
        }
    }

    #[test]
    fn requests_are_bounded() {
        args().validate().unwrap();
        for broken in [
            InputRequestArgs {
                ask: " ".into(),
                ..args()
            },
            InputRequestArgs {
                assignees: vec![" alice".into()],
                ..args()
            },
            InputRequestArgs {
                deadline_ms: Some(MAX_INPUT_DEADLINE_MS + 1),
                ..args()
            },
            InputRequestArgs {
                remind_after_ms: Some(1_000),
                ..args()
            },
            InputRequestArgs {
                response_schema: Some(json!({"type": 7})),
                ..args()
            },
        ] {
            assert!(broken.validate().is_err(), "{broken:?}");
        }
    }

    #[test]
    fn the_call_deadline_wins_then_the_program_default_then_seven_days() {
        assert_eq!(
            InputRequestArgs {
                deadline_ms: Some(5_000),
                ..args()
            }
            .deadline_after_ms(Some(9_000)),
            5_000
        );
        assert_eq!(args().deadline_after_ms(Some(9_000)), 9_000);
        assert_eq!(args().deadline_after_ms(None), DEFAULT_INPUT_DEADLINE_MS);
        assert_eq!(
            args().deadline_after_ms(Some(u64::MAX)),
            MAX_INPUT_DEADLINE_MS
        );
    }

    #[test]
    fn decisions_match_the_request_shape() {
        let schema = json!({"type": "object", "required": ["n"]});
        validate_decision(None, true, None, Some("ok")).unwrap();
        validate_decision(None, false, None, None).unwrap();
        validate_decision(Some(&schema), true, Some(&json!({"n": 1})), None).unwrap();
        validate_decision(Some(&schema), false, None, None).unwrap();
        assert!(validate_decision(Some(&schema), true, None, None).is_err());
        assert!(validate_decision(Some(&schema), true, Some(&json!({})), None).is_err());
        assert!(validate_decision(None, true, Some(&json!(1)), None).is_err());
        assert!(validate_decision(None, false, Some(&json!(1)), None).is_err());
    }
}
