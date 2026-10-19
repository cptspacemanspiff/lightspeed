//! Human input for programs: `input.request` asks people a question and
//! completes with their answer. It is a system workflow-tool binding with a
//! joined reply: the input-request workflow records the request in the
//! universe's inbox, waits durably for an answer or its deadline, and
//! resolves the call. Only program sessions bind it; no model presents it.
//!
//! `input.open` starts the same workflow but returns at once with the
//! request's promise, which the program awaits for the answer or cancels
//! to withdraw the request. `input.remind` and `input.reassign` act on an
//! open request through its inbox row while it waits. `input.handle` takes
//! back a request an earlier run detached, by its execution id.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{ToolError, ToolResult};

pub const INPUT_REQUEST_TOOL_ID: &str = "input.request";
pub const INPUT_REQUEST_TOOL_NAME: &str = "input_request";
pub const INPUT_REQUEST_WORKFLOW_TOOL_ID: &str = "input-request";
pub const INPUT_REQUEST_WORKFLOW_SEMANTIC_TYPE: &str = "lightspeed.input.request.v1";
pub const INPUT_REQUEST_WORKFLOW_TYPE: &str = "InputRequestWorkflow";
pub const INPUT_OPEN_TOOL_ID: &str = "input.open";
pub const INPUT_OPEN_TOOL_NAME: &str = "input_open";
pub const INPUT_OPEN_WORKFLOW_TOOL_ID: &str = "input-open";
pub const INPUT_OPEN_WORKFLOW_SEMANTIC_TYPE: &str = "lightspeed.input.open.v1";
pub const INPUT_REMIND_TOOL_ID: &str = "input.remind";
pub const INPUT_REMIND_TOOL_NAME: &str = "input_remind";
pub const INPUT_REASSIGN_TOOL_ID: &str = "input.reassign";
pub const INPUT_REASSIGN_TOOL_NAME: &str = "input_reassign";
pub const INPUT_HANDLE_TOOL_ID: &str = "input.handle";
pub const INPUT_HANDLE_TOOL_NAME: &str = "input_handle";
pub use harness::{
    APPROVAL_TOOL_NAME as APPROVAL_TOOL_ID, APPROVAL_WORKFLOW_SEMANTIC_TYPE,
    APPROVAL_WORKFLOW_TOOL_ID,
};
pub const APPROVAL_TOOL_NAME: &str = "input_approval";

const DAY_MS: u64 = 24 * 60 * 60 * 1_000;
/// How long a request waits when neither the call nor the program sets a
/// deadline.
pub const DEFAULT_INPUT_DEADLINE_MS: u64 = 7 * DAY_MS;
/// The longest deadline a request may ask for, so a two-week absence fits.
pub const MAX_INPUT_DEADLINE_MS: u64 = 30 * DAY_MS;
pub const MIN_INPUT_DEADLINE_MS: u64 = 1_000;
/// Reminders at most hourly keep a 30-day wait's workflow history small.
pub const MIN_REMIND_AFTER_MS: u64 = 60 * 60 * 1_000;

pub const INPUT_DEFAULT_DEADLINE_ENV: &str = "LIGHTSPEED_INPUT_DEFAULT_DEADLINE_MS";
pub const INPUT_MIN_DEADLINE_ENV: &str = "LIGHTSPEED_INPUT_MIN_DEADLINE_MS";
pub const INPUT_MAX_DEADLINE_ENV: &str = "LIGHTSPEED_INPUT_MAX_DEADLINE_MS";
pub const INPUT_MIN_REMIND_AFTER_ENV: &str = "LIGHTSPEED_INPUT_MIN_REMIND_AFTER_MS";

/// The deployment's bounds on how long requests wait and how often they
/// remind. The defaults suit production; each can be overridden from the
/// environment, e.g. for tests that need a request to expire in seconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputTimeouts {
    pub default_deadline_ms: u64,
    pub min_deadline_ms: u64,
    pub max_deadline_ms: u64,
    pub min_remind_after_ms: u64,
}

impl Default for InputTimeouts {
    fn default() -> Self {
        Self {
            default_deadline_ms: DEFAULT_INPUT_DEADLINE_MS,
            min_deadline_ms: MIN_INPUT_DEADLINE_MS,
            max_deadline_ms: MAX_INPUT_DEADLINE_MS,
            min_remind_after_ms: MIN_REMIND_AFTER_MS,
        }
    }
}

impl InputTimeouts {
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// The defaults with every override `lookup` supplies; an empty value
    /// keeps the default.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let read = |name: &str, default: u64| -> Result<u64, String> {
            match lookup(name).filter(|value| !value.trim().is_empty()) {
                None => Ok(default),
                Some(value) => value
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .filter(|millis| *millis > 0)
                    .ok_or_else(|| format!("{name} must be a positive number of milliseconds")),
            }
        };
        let defaults = Self::default();
        let timeouts = Self {
            default_deadline_ms: read(INPUT_DEFAULT_DEADLINE_ENV, defaults.default_deadline_ms)?,
            min_deadline_ms: read(INPUT_MIN_DEADLINE_ENV, defaults.min_deadline_ms)?,
            max_deadline_ms: read(INPUT_MAX_DEADLINE_ENV, defaults.max_deadline_ms)?,
            min_remind_after_ms: read(INPUT_MIN_REMIND_AFTER_ENV, defaults.min_remind_after_ms)?,
        };
        if !(timeouts.min_deadline_ms..=timeouts.max_deadline_ms)
            .contains(&timeouts.default_deadline_ms)
        {
            return Err(format!(
                "{INPUT_DEFAULT_DEADLINE_ENV} ({}) must lie between {INPUT_MIN_DEADLINE_ENV} ({}) and {INPUT_MAX_DEADLINE_ENV} ({})",
                timeouts.default_deadline_ms, timeouts.min_deadline_ms, timeouts.max_deadline_ms
            ));
        }
        Ok(timeouts)
    }

    pub fn allows_deadline(&self, deadline_ms: u64) -> bool {
        (self.min_deadline_ms..=self.max_deadline_ms).contains(&deadline_ms)
    }

    /// The binding's joined ceiling: a request may wait out its own deadline
    /// and, once escalated, a fresh one.
    pub fn deadline_ceiling_ms(&self) -> u64 {
        self.max_deadline_ms
            .saturating_mul(2)
            .saturating_add(DAY_MS)
    }
}

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
/// The failure code of a tool call whose approval was rejected.
pub const APPROVAL_REJECTED_CODE: &str = "approval_rejected";

/// Who decides a program's approval-required tool calls and how long they
/// wait, from the program's pin and the run's defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ApprovalSettings {
    pub assignees: Vec<String>,
    pub deadline_ms: Option<u64>,
    pub remind_after_ms: Option<u64>,
    pub escalate_to: Vec<String>,
}

impl ApprovalSettings {
    /// A program's settings, with what the run's starter (a bot, say) chose
    /// in their place: its approvers decide, and its escalation list takes
    /// over.
    pub fn from_program(
        program: Option<&harness::ProgramApprovals>,
        run: Option<&harness::ProgramInputDefaults>,
    ) -> Self {
        let program = program.cloned().unwrap_or_default();
        let pick = |run: Vec<String>, program: Vec<String>| {
            if run.is_empty() { program } else { run }
        };
        Self {
            assignees: pick(
                run.map(|run| run.approvers.clone()).unwrap_or_default(),
                program.assignees,
            ),
            deadline_ms: program.deadline_ms,
            remind_after_ms: program.remind_after_ms,
            escalate_to: pick(
                run.map(|run| run.escalate_to.clone()).unwrap_or_default(),
                program.escalate_to,
            ),
        }
    }

    pub fn validate(&self, timeouts: &InputTimeouts) -> ToolResult<()> {
        validate_people("assignees", &self.assignees)?;
        validate_people("escalateTo", &self.escalate_to)?;
        if let Some(deadline) = self.deadline_ms
            && !timeouts.allows_deadline(deadline)
        {
            return Err(invalid(format!(
                "deadlineMs must be between {} and {}",
                timeouts.min_deadline_ms, timeouts.max_deadline_ms
            )));
        }
        if self
            .remind_after_ms
            .is_some_and(|interval| interval < timeouts.min_remind_after_ms)
        {
            return Err(invalid(format!(
                "remindAfterMs must be at least {}",
                timeouts.min_remind_after_ms
            )));
        }
        Ok(())
    }

    /// The approval's wait: its own deadline, else the deployment's, never
    /// outside the deployment's bounds.
    pub fn deadline_after_ms(&self, timeouts: &InputTimeouts) -> u64 {
        self.deadline_ms
            .unwrap_or(timeouts.default_deadline_ms)
            .clamp(timeouts.min_deadline_ms, timeouts.max_deadline_ms)
    }
}

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
    /// program's input deadline; the deployment bounds it, by default to 1
    /// second through 30 days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub deadline_ms: Option<u64>,
    /// Remind the assignees on this interval until the request is answered
    /// or its deadline passes. The deployment sets the shortest interval,
    /// by default one hour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub remind_after_ms: Option<u64>,
    /// Who takes the request over, once and with a fresh deadline, when the
    /// deadline passes unanswered. Without it the call fails with
    /// `deadline_exceeded`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escalate_to: Vec<String>,
}

impl InputRequestArgs {
    pub fn validate(&self, timeouts: &InputTimeouts) -> ToolResult<()> {
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
            && !timeouts.allows_deadline(deadline)
        {
            return Err(invalid(format!(
                "deadline_ms must be between {} and {}",
                timeouts.min_deadline_ms, timeouts.max_deadline_ms
            )));
        }
        if self
            .remind_after_ms
            .is_some_and(|interval| interval < timeouts.min_remind_after_ms)
        {
            return Err(invalid(format!(
                "remind_after_ms must be at least {}",
                timeouts.min_remind_after_ms
            )));
        }
        Ok(())
    }

    /// The request's wait: its own deadline, else the program's default,
    /// else the deployment's, never outside the deployment's bounds.
    pub fn deadline_after_ms(&self, program_default: Option<u64>, timeouts: &InputTimeouts) -> u64 {
        self.deadline_ms
            .or(program_default)
            .unwrap_or(timeouts.default_deadline_ms)
            .clamp(timeouts.min_deadline_ms, timeouts.max_deadline_ms)
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

/// Whether a workflow invocation is a program's `input.request` or
/// `input.open`, or a parked call's approval: all start the input-request
/// workflow.
pub fn is_input_request_binding(tool_id: &str, semantic_type: &str) -> bool {
    (tool_id == INPUT_REQUEST_WORKFLOW_TOOL_ID
        && semantic_type == INPUT_REQUEST_WORKFLOW_SEMANTIC_TYPE)
        || (tool_id == INPUT_OPEN_WORKFLOW_TOOL_ID
            && semantic_type == INPUT_OPEN_WORKFLOW_SEMANTIC_TYPE)
        || is_approval_binding(tool_id, semantic_type)
}

pub fn is_approval_binding(tool_id: &str, semantic_type: &str) -> bool {
    tool_id == APPROVAL_WORKFLOW_TOOL_ID && semantic_type == APPROVAL_WORKFLOW_SEMANTIC_TYPE
}

/// The approval binding's tool. No program calls it: a call its scope
/// parks emits it with the parked call's own arguments, whatever they are.
pub fn approval_definition() -> crate::runtime::FunctionDefinition {
    crate::runtime::FunctionDefinition::new(
        APPROVAL_TOOL_NAME,
        "Wait for approval of a parked tool call, then run it. Emitted by the runtime in place of an approval-required call; never called directly.",
        serde_json::json!({ "type": "object" }),
    )
}

/// The session tool behind the approval binding.
pub fn approval_tool_spec() -> harness::ToolSpec {
    crate::definitions::register(
        APPROVAL_TOOL_ID,
        Default::default(),
        harness::ToolParallelism::ParallelSafe,
        Default::default(),
    )
}

/// The `input.open` definition: the arguments of `input.request`, and the
/// acknowledgement that carries the request's promise. Its output schema
/// is the one the binding's promise completion gives the call, so the
/// contract's digest is the same at pinning and in the scope.
pub fn input_open_definition() -> crate::runtime::FunctionDefinition {
    let mut definition = crate::runtime::FunctionDefinition::new(
        INPUT_OPEN_TOOL_NAME,
        "Ask people a question without waiting. Returns at once with the request's execution id and the promise that resolves with the answer; await the promise for the answer (it fails with kind deadline_exceeded once the deadline and any escalation pass) or cancel it to withdraw the request.",
        crate::definitions::input_schema_for::<InputRequestArgs>(),
    );
    if let Some(schema) = crate::workflow_tool::acknowledgement_result_schema(
        &input_open_completion(None, 1),
        true,
    ) {
        definition = definition.with_output_schema(schema);
    }
    definition
}

/// The completion of the `input.open` binding: one model-owned promise
/// under the reply key.
pub fn input_open_completion(
    reply_schema_ref: Option<harness::BlobRef>,
    deadline_after_ms: u64,
) -> harness::WorkflowToolCompletion {
    harness::WorkflowToolCompletion::Promises {
        reply_schema_ref,
        deadline_after_ms: Some(deadline_after_ms),
        max_promises: 1,
        key_source: harness::WorkflowToolCompletionKeySource::Reply,
    }
}

/// The session tool behind the `input.open` binding.
pub fn input_open_tool_spec() -> harness::ToolSpec {
    crate::definitions::register(
        INPUT_OPEN_TOOL_ID,
        Default::default(),
        harness::ToolParallelism::ParallelSafe,
        Default::default(),
    )
}

/// An open request, named by the execution id `input.open` returned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputRemindArgs {
    /// The request's execution id, from `input.open`.
    pub id: String,
}

/// An open request and the people who should answer it instead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputReassignArgs {
    /// The request's execution id, from `input.open`.
    pub id: String,
    /// Who should answer from now on; replaces the assignees.
    pub to: Vec<String>,
}

impl InputReassignArgs {
    pub fn validate(&self) -> ToolResult<()> {
        if self.to.is_empty() {
            return Err(invalid("to must name at least one person"));
        }
        validate_people("to", &self.to)
    }
}

/// A request an earlier run of this session opened and detached, named by
/// the execution id `input.open` returned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputHandleArgs {
    /// The request's execution id, from `input.open`.
    pub id: String,
}

/// The request's handle parts, as `input.open` returned them: its execution
/// id and the promise that settles with its answer, now the calling run's.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InputHandleOutput {
    pub execution_id: String,
    pub promise: String,
    /// pending, resolved, failed or cancelled.
    pub status: String,
}

/// The promise of the `input.open` request with this execution id, if this
/// session opened one.
pub fn opened_request_promise<'a>(
    state: &'a harness::CoreAgentState,
    execution_id: &str,
) -> Option<&'a harness::Promise> {
    crate::workflow_tool::started_call_promise(state, INPUT_OPEN_WORKFLOW_TOOL_ID, execution_id)
}

pub fn input_handle_definition() -> crate::runtime::FunctionDefinition {
    crate::runtime::FunctionDefinition::new(
        INPUT_HANDLE_TOOL_NAME,
        "Take back an input request an earlier run of this session opened and detached. The calling run owns it again: it is withdrawn at the run's exit unless detached again.",
        crate::definitions::input_schema_for::<InputHandleArgs>(),
    )
    .with_output_schema(crate::definitions::output_schema_for::<InputHandleOutput>())
}

/// Where a request stands after a reminder or a reassignment. Acting on a
/// request that is no longer pending changes nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InputControlOutput {
    /// The request's inbox id.
    pub request_id: String,
    /// pending, answered, expired or cancelled.
    pub status: String,
    pub assignees: Vec<String>,
    pub reminder_count: u32,
}

pub fn input_remind_definition() -> crate::runtime::FunctionDefinition {
    crate::runtime::FunctionDefinition::new(
        INPUT_REMIND_TOOL_NAME,
        "Remind the assignees of an open input request. A request that is no longer pending is left as it is.",
        crate::definitions::input_schema_for::<InputRemindArgs>(),
    )
    .with_output_schema(crate::definitions::output_schema_for::<InputControlOutput>())
}

pub fn input_reassign_definition() -> crate::runtime::FunctionDefinition {
    crate::runtime::FunctionDefinition::new(
        INPUT_REASSIGN_TOOL_NAME,
        "Replace the assignees of an open input request. A request that is no longer pending is left as it is.",
        crate::definitions::input_schema_for::<InputReassignArgs>(),
    )
    .with_output_schema(crate::definitions::output_schema_for::<InputControlOutput>())
}

/// Check a list of people a request is asked of or escalates to.
pub fn validate_people(field: &str, people: &[String]) -> ToolResult<()> {
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
        args().validate(&InputTimeouts::default()).unwrap();
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
                remind_after_ms: Some(MIN_REMIND_AFTER_MS - 1),
                ..args()
            },
            InputRequestArgs {
                response_schema: Some(json!({"type": 7})),
                ..args()
            },
        ] {
            assert!(
                broken.validate(&InputTimeouts::default()).is_err(),
                "{broken:?}"
            );
        }
    }

    #[test]
    fn the_call_deadline_wins_then_the_program_default_then_seven_days() {
        assert_eq!(
            InputRequestArgs {
                deadline_ms: Some(5_000),
                ..args()
            }
            .deadline_after_ms(Some(9_000), &InputTimeouts::default()),
            5_000
        );
        assert_eq!(
            args().deadline_after_ms(Some(9_000), &InputTimeouts::default()),
            9_000
        );
        assert_eq!(
            args().deadline_after_ms(None, &InputTimeouts::default()),
            DEFAULT_INPUT_DEADLINE_MS
        );
        assert_eq!(
            args().deadline_after_ms(Some(u64::MAX), &InputTimeouts::default()),
            MAX_INPUT_DEADLINE_MS
        );
    }

    fn timeouts_from(vars: &[(&str, &str)]) -> Result<InputTimeouts, String> {
        InputTimeouts::from_lookup(|name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        })
    }

    #[test]
    fn timeouts_default_without_overrides() {
        assert_eq!(timeouts_from(&[]).unwrap(), InputTimeouts::default());
        assert_eq!(
            timeouts_from(&[(INPUT_MIN_REMIND_AFTER_ENV, " ")]).unwrap(),
            InputTimeouts::default()
        );
    }

    #[test]
    fn the_environment_overrides_every_timeout() {
        let timeouts = timeouts_from(&[
            (INPUT_DEFAULT_DEADLINE_ENV, "2000"),
            (INPUT_MIN_DEADLINE_ENV, "100"),
            (INPUT_MAX_DEADLINE_ENV, "60000"),
            (INPUT_MIN_REMIND_AFTER_ENV, "250"),
        ])
        .unwrap();
        assert_eq!(
            timeouts,
            InputTimeouts {
                default_deadline_ms: 2_000,
                min_deadline_ms: 100,
                max_deadline_ms: 60_000,
                min_remind_after_ms: 250,
            }
        );
        let short = InputRequestArgs {
            deadline_ms: Some(100),
            remind_after_ms: Some(250),
            ..args()
        };
        short.validate(&timeouts).unwrap();
        assert!(short.validate(&InputTimeouts::default()).is_err());
        assert!(
            InputRequestArgs {
                deadline_ms: Some(60_001),
                ..args()
            }
            .validate(&timeouts)
            .is_err()
        );
        assert_eq!(args().deadline_after_ms(None, &timeouts), 2_000);
        assert_eq!(args().deadline_after_ms(Some(u64::MAX), &timeouts), 60_000);
        assert_eq!(timeouts.deadline_ceiling_ms(), 120_000 + DAY_MS);
    }

    #[test]
    fn invalid_timeout_overrides_are_refused() {
        for vars in [
            &[(INPUT_MIN_REMIND_AFTER_ENV, "soon")][..],
            &[(INPUT_MIN_DEADLINE_ENV, "0")][..],
            &[(INPUT_MAX_DEADLINE_ENV, "-5")][..],
            // The default must lie within the bounds.
            &[(INPUT_MAX_DEADLINE_ENV, "1000")][..],
            &[(INPUT_DEFAULT_DEADLINE_ENV, "500")][..],
        ] {
            assert!(timeouts_from(vars).is_err(), "{vars:?}");
        }
    }

    #[test]
    fn a_runs_approvers_and_escalation_replace_the_programs() {
        let program = harness::ProgramApprovals {
            assignees: vec!["alice".into()],
            deadline_ms: Some(5_000),
            remind_after_ms: None,
            escalate_to: vec!["lead".into()],
        };
        assert_eq!(
            ApprovalSettings::from_program(Some(&program), None),
            ApprovalSettings {
                assignees: vec!["alice".into()],
                deadline_ms: Some(5_000),
                remind_after_ms: None,
                escalate_to: vec!["lead".into()],
            }
        );
        let run = harness::ProgramInputDefaults {
            escalate_to: Vec::new(),
            approvers: vec!["bob".into()],
        };
        let merged = ApprovalSettings::from_program(Some(&program), Some(&run));
        assert_eq!(
            (merged.assignees, merged.escalate_to),
            (vec!["bob".to_owned()], vec!["lead".to_owned()])
        );
        let timeouts = InputTimeouts::default();
        assert_eq!(
            ApprovalSettings::default().deadline_after_ms(&timeouts),
            timeouts.default_deadline_ms
        );
        assert!(
            ApprovalSettings {
                remind_after_ms: Some(1),
                ..Default::default()
            }
            .validate(&timeouts)
            .is_err()
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
