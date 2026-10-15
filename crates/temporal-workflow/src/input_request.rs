//! Durable inputs of a human-input request: one program call waiting for
//! people to answer. The inbox row is the record of truth; the workflow
//! only schedules its deadline, escalation and reminders.

use harness::BlobRef;
use serde::{Deserialize, Serialize};

/// The stable workflow type the input-request binding's recipe starts.
pub const INPUT_REQUEST_WORKFLOW_TYPE: &str = "InputRequestWorkflow";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputRequestOpenActivityRequest {
    pub start: crate::WorkflowToolStartArgs,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InputRequestOpenActivityResult {
    /// The request is in the inbox, waiting until `deadline_at_ms`.
    Opened {
        request_id: String,
        deadline_at_ms: u64,
        /// The wait an escalation restarts.
        deadline_after_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        remind_after_ms: Option<u64>,
        /// Whether an escalation list takes over once the deadline passes.
        escalates: bool,
    },
    /// The arguments were invalid; the call fails with this error.
    Rejected { error_ref: BlobRef },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputRequestStepActivityRequest {
    pub universe_id: uuid::Uuid,
    pub execution_id: String,
    pub step: InputRequestStep,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InputRequestStep {
    /// Read the request: has it been answered?
    Check,
    /// Record the `count`th reminder.
    Remind { count: u32, at_ms: u64 },
    /// The deadline passed: escalate once with a fresh deadline when
    /// `escalate_deadline_at_ms` is set, else expire.
    Deadline {
        at_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        escalate_deadline_at_ms: Option<u64>,
    },
    /// The waiting call was withdrawn.
    Cancel { at_ms: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InputRequestStepResult {
    /// Still waiting, now until `deadline_at_ms`.
    Pending {
        deadline_at_ms: u64,
        escalated: bool,
    },
    /// Answered, expired or cancelled: the call's reply.
    Settled {
        resolution: harness::PromiseResolution,
    },
}

/// Queryable state of a human-input request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputRequestSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at_ms: Option<u64>,
    pub escalated: bool,
    pub reminders: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<harness::PromiseResolution>,
}

/// When the workflow next wakes on its own: the deadline, or the next
/// reminder if one falls before it.
pub fn next_wake_at_ms(
    deadline_at_ms: u64,
    remind_after_ms: Option<u64>,
    last_reminder_at_ms: u64,
) -> u64 {
    remind_after_ms
        .map(|interval| last_reminder_at_ms.saturating_add(interval))
        .filter(|remind_at| *remind_at < deadline_at_ms)
        .unwrap_or(deadline_at_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reminders_wake_before_the_deadline_only() {
        assert_eq!(next_wake_at_ms(1_000, None, 0), 1_000);
        assert_eq!(next_wake_at_ms(1_000, Some(300), 0), 300);
        assert_eq!(next_wake_at_ms(1_000, Some(300), 900), 1_000);
        assert_eq!(next_wake_at_ms(1_000, Some(u64::MAX), 900), 1_000);
    }
}
