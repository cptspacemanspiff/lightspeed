//! Hosted child-session delegation service: the activities behind
//! `SubagentExecutionWorkflow`, for child agents and child programs alike.
//! Prepare validates the pinned grant, reserves the root-scoped tree slot by
//! creating the child session row, creates the child from the pinned profile,
//! and starts its run with a notify intent back to the execution; resolve
//! builds the result (an agent's envelope, a program's output) and closes the
//! child; close force-closes a cancelled child.

use std::sync::Arc;

use api::{
    AgentApiError, AgentApiService, AgentProfile, InlineAgentProfile, InputItem, ProfileId,
    ProfileSource, RunStartSource, SessionCloseParams, SessionReadParams,
};
use async_trait::async_trait;
use harness::{
    BlobRef, PromiseResolution, RunStatus, RunTerminalNotifyIntent, SessionId, SubmissionId,
    media::MediaDescriptor,
    storage::{
        BlobGraphStore, BlobStore, CreateSession, DelegatedContract, SessionOrigin,
        SessionOriginKind, SessionStore, SessionStoreError,
    },
};
use temporal_workflow::{
    SubagentChildRef, SubagentPrepareActivityResult, SubagentTerminal, WorkflowToolStartArgs,
};
use tools::{
    attachments::{Attachment, linked_attachments},
    concurrency::{AwaitArgs, AwaitModeArg},
    subagents::{
        AgentCallArgs, ProgramCallArgs, ProgramChildFailure, SubagentExecutionContextV1,
        SubagentResultEnvelope, SubagentResultStatus, SubagentToolKind,
    },
};

use crate::gateway::GatewayAgentApi;

const MAX_SUBAGENT_OUTPUT_BYTES: usize = 512 * 1024;

/// The child-side operations the service needs from the hosted API. A
/// trait so the service can be exercised against a counting fake.
#[async_trait]
pub trait SubagentChildRuntime: Send + Sync {
    async fn read_profile(&self, profile_id: ProfileId) -> Result<AgentProfile, AgentApiError>;

    /// Open the child's workflow from `profile`. `pinned_from` names the
    /// profile revision an inline copy came from, so a program pinned from it
    /// records its program identity.
    async fn start_session(
        &self,
        session_id: &SessionId,
        profile: ProfileSource,
        pinned_from: Option<(ProfileId, u64)>,
    ) -> Result<(), AgentApiError>;

    async fn start_run(
        &self,
        session_id: &SessionId,
        source: RunStartSource,
        submission_id: SubmissionId,
        notify_on_terminal: Vec<RunTerminalNotifyIntent>,
    ) -> Result<String, AgentApiError>;

    /// The session's config as it was last put, for the grants a child may
    /// ask for. Read on behalf of the runtime, not a caller.
    async fn session_config(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<api::SessionConfig>, AgentApiError>;

    /// The contracts a program session's run holds, as its scopes bind
    /// them, each with the tool serving it: what the run may hand a child
    /// in `env`. `None` when a model drives the session.
    async fn program_contracts(
        &self,
        session_id: &SessionId,
        run_id: u64,
    ) -> Result<Option<Vec<DelegatedContract>>, AgentApiError>;

    async fn close_session(&self, session_id: &SessionId, force: bool)
    -> Result<(), AgentApiError>;

    /// The media the child's session holds in its active context: what the
    /// child was shown and may therefore hand up by handle.
    async fn session_media(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<MediaDescriptor>, AgentApiError>;
}

#[derive(Clone)]
pub struct AgentApiSubagentRuntime {
    api: Arc<GatewayAgentApi>,
}

impl AgentApiSubagentRuntime {
    pub fn new(api: Arc<GatewayAgentApi>) -> Self {
        Self { api }
    }
}

#[async_trait]
impl SubagentChildRuntime for AgentApiSubagentRuntime {
    async fn read_profile(&self, profile_id: ProfileId) -> Result<AgentProfile, AgentApiError> {
        use profiles::ProfileStore as _;
        self.api
            .store()
            .read_agent_profile(&profile_id)
            .await
            .map_err(|e| AgentApiError::internal(e.to_string()))
    }

    async fn start_session(
        &self,
        session_id: &SessionId,
        profile: ProfileSource,
        pinned_from: Option<(ProfileId, u64)>,
    ) -> Result<(), AgentApiError> {
        let authority = self.api.delegated_session_authority(session_id).await?;
        crate::gateway::service::authorization::with_controller_authority(authority, async {
            self.api
                .start_session_for_subagent(session_id, profile, pinned_from)
                .await
        })
        .await
    }

    async fn start_run(
        &self,
        session_id: &SessionId,
        source: RunStartSource,
        submission_id: SubmissionId,
        notify_on_terminal: Vec<RunTerminalNotifyIntent>,
    ) -> Result<String, AgentApiError> {
        let authority = self.api.delegated_session_authority(session_id).await?;
        crate::gateway::service::authorization::with_controller_authority(authority, async {
            self.api
                .start_run_for_subagent(session_id, source, submission_id, notify_on_terminal)
                .await
        })
        .await
    }

    async fn session_config(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<api::SessionConfig>, AgentApiError> {
        let loaded = self.api.load_session_state(session_id).await?;
        loaded
            .state
            .lifecycle
            .config
            .as_ref()
            .map(api_projection::session_config_to_api)
            .transpose()
    }

    async fn program_contracts(
        &self,
        session_id: &SessionId,
        run_id: u64,
    ) -> Result<Option<Vec<DelegatedContract>>, AgentApiError> {
        let loaded = self.api.load_session_state(session_id).await?;
        Ok(held_program_contracts(&loaded.state, run_id))
    }

    async fn close_session(
        &self,
        session_id: &SessionId,
        force: bool,
    ) -> Result<(), AgentApiError> {
        let authority = self.api.delegated_session_authority(session_id).await?;
        crate::gateway::service::authorization::with_controller_authority(authority, async {
            self.api
                .close_session(SessionCloseParams {
                    session_id: session_id.as_str().to_owned(),
                    force,
                })
                .await
                .map(|_| ())
        })
        .await
    }

    async fn session_media(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<MediaDescriptor>, AgentApiError> {
        let authority = self.api.delegated_session_authority(session_id).await?;
        crate::gateway::service::authorization::with_controller_authority(authority, async {
            let session = self
                .api
                .read_session(SessionReadParams {
                    session_id: session_id.as_str().to_owned(),
                    run_limit: Some(1),
                })
                .await?
                .result
                .session;
            Ok(session
                .active_context
                .entries
                .iter()
                .filter(|entry| entry.content.media_handle.is_some())
                .filter_map(|entry| {
                    let content_ref = BlobRef::parse(entry.content.content_ref.clone()).ok()?;
                    MediaDescriptor::new(
                        content_ref,
                        entry.content.media_type.as_deref()?,
                        harness::media::media_preview_name(entry.preview.as_deref()).as_deref(),
                    )
                })
                .collect())
        })
        .await
    }
}

#[derive(Clone)]
pub struct SubagentService {
    sessions: Arc<dyn SessionStore>,
    blobs: Arc<dyn BlobStore>,
    blob_graph: Option<Arc<dyn BlobGraphStore>>,
    runtime: Arc<dyn SubagentChildRuntime>,
}

impl SubagentService {
    pub fn new(
        sessions: Arc<dyn SessionStore>,
        blobs: Arc<dyn BlobStore>,
        runtime: Arc<dyn SubagentChildRuntime>,
    ) -> Self {
        Self {
            sessions,
            blobs,
            blob_graph: None,
            runtime,
        }
    }

    /// Retain handed-off attachments through the result envelope until the
    /// receiving session records its own attachment and context references.
    pub fn with_blob_graph(mut self, blob_graph: Option<Arc<dyn BlobGraphStore>>) -> Self {
        self.blob_graph = blob_graph;
        self
    }

    /// Step A of the execution. Every expected failure (limit, unlisted
    /// agent, missing profile) is `Rejected` so the parent's `reply`
    /// promise fails cleanly; only infrastructure failures are errors.
    pub async fn prepare(
        &self,
        start: WorkflowToolStartArgs,
        now_ms: u64,
    ) -> Result<SubagentPrepareActivityResult, AgentApiError> {
        let invocation = &start.invocation;
        let expected_holder =
            temporal_workflow::compose_workflow_id(start.universe_id, &invocation.session_id);
        if start.execution_id.is_empty()
            || start.universe_id != invocation.session_universe_id
            || start.holder_workflow_id != expected_holder
        {
            return Err(AgentApiError::invalid_request(
                "subagent workflow-tool start identity is invalid",
            ));
        }
        let Some(reply_promise_id) = invocation
            .completion_promises
            .as_ref()
            .and_then(|promises| promises.get(harness::REPLY_COMPLETION_KEY))
        else {
            return Err(AgentApiError::invalid_request(
                "subagent invocation is missing its reply completion promise",
            ));
        };
        let Some(kind) = SubagentToolKind::from_binding(
            invocation.tool_id.as_str(),
            invocation.semantic_type.as_str(),
        ) else {
            return Err(AgentApiError::invalid_request(format!(
                "unsupported subagent workflow tool {} ({})",
                invocation.tool_id, invocation.semantic_type
            )));
        };
        let noun = if kind.is_program() {
            "program"
        } else {
            "agent"
        };
        let call = if kind.is_program() {
            let args: ProgramCallArgs = self.read_json(&invocation.arguments_ref).await?;
            if let Err(error) = args.validate() {
                return self.rejected(error.to_string()).await;
            }
            ChildCall {
                profile: args.program,
                label: args.label,
                env: args.env,
                run: RunStartSource::Program {
                    input: args.input,
                    input_defaults: None,
                },
            }
        } else {
            let args: AgentCallArgs = self.read_json(&invocation.arguments_ref).await?;
            if let Err(error) = args.validate() {
                return self.rejected(error.to_string()).await;
            }
            ChildCall {
                profile: args.agent,
                label: args.label,
                env: args.env,
                run: RunStartSource::Input {
                    items: vec![InputItem::Text {
                        provenance_ref: None,
                        origin: None,
                        text: args.input,
                    }],
                },
            }
        };
        let context_ref = invocation.execution_context_ref.as_ref().ok_or_else(|| {
            AgentApiError::invalid_request("subagent invocation is missing its execution context")
        })?;
        let context: SubagentExecutionContextV1 = self.read_json(context_ref).await?;
        if context.parent_session_id != invocation.session_id.as_str() {
            return Err(AgentApiError::rejected(
                "delegation parent does not match admitted invocation",
            ));
        }
        if context.version != SubagentExecutionContextV1::VERSION {
            return Err(AgentApiError::invalid_request(format!(
                "unsupported subagent execution context version {}",
                context.version
            )));
        }
        if context.agent_profile_id != call.profile {
            return self
                .rejected(format!(
                    "{noun} {} does not match the admitted {noun} {}",
                    call.profile, context.agent_profile_id
                ))
                .await;
        }
        let parent_session_id = SessionId::try_new(context.parent_session_id.clone())
            .map_err(|error| AgentApiError::internal(format!("invalid parent id: {error}")))?;
        let parent = self
            .sessions
            .load_session(&parent_session_id)
            .await
            .map_err(api_projection::map_session_store_error)?
            .ok_or_else(|| {
                AgentApiError::not_found(format!("parent session not found: {parent_session_id}"))
            })?;
        let (root_session_id, root_run_id, depth, limits) = match parent.origin.as_ref() {
            Some(origin) => (
                origin.root_session_id.clone(),
                origin.root_run_id,
                origin.depth.saturating_add(1),
                context.grant_limits.attenuated_by(origin.limits),
            ),
            None => (
                parent_session_id.clone(),
                Some(context.parent_run_id),
                1,
                context.grant_limits,
            ),
        };
        let profile_id = match ProfileId::try_new(call.profile.clone()) {
            Ok(profile_id) => profile_id,
            Err(error) => {
                return self
                    .rejected(format!(
                        "invalid {noun} profile id {:?}: {error}",
                        call.profile
                    ))
                    .await;
            }
        };
        let profile = match self.runtime.read_profile(profile_id.clone()).await {
            Ok(profile) => profile,
            Err(error) if is_not_found(&error) => {
                return self
                    .rejected(format!("{noun} profile does not exist: {profile_id}"))
                    .await;
            }
            Err(error) => return Err(error),
        };
        // The grant lists each profile under its kind, but a profile can be
        // edited into the other kind after the grant was admitted.
        if profile.document.program.is_some() != kind.is_program() {
            return self
                .rejected(format!(
                    "{profile_id} is no longer {} profile",
                    if kind.is_program() {
                        "a program"
                    } else {
                        "an agent"
                    }
                ))
                .await;
        }
        // The profile is applied inline so the child runs the revision that
        // is pinned on its origin, not whatever the registry holds later,
        // with any `inherit` attachment resolved against the parent
        // environment captured at admission.
        let document = resolve_inherited_environment(
            profile.document.clone(),
            context.parent_active_environment_id.as_deref(),
        );
        // Checked again when the child's run starts; checking it here keeps a
        // bad input from taking a slot in the tree.
        if let (
            RunStartSource::Program { input, .. },
            Some(api::ProgramSpec {
                input_schema: Some(schema),
                ..
            }),
        ) = (&call.run, &profile.document.program)
            && let Err(error) = jsonschema::validate(schema, input)
        {
            return self
                .rejected(format!(
                    "program {profile_id} input does not satisfy its input schema: {error}"
                ))
                .await;
        }
        if kind.is_program() {
            let parent_config = self.runtime.session_config(&parent_session_id).await?;
            if let Some(excess) = crate::subagent_grants::excess_grant(
                document
                    .config
                    .as_ref()
                    .and_then(|config| config.features.as_ref()),
                parent_config
                    .as_ref()
                    .and_then(|config| config.features.as_ref()),
            ) {
                return self
                    .rejected_for(
                        "grant_exceeded",
                        format!(
                            "program {profile_id} asks for {excess}, which its caller does not hold"
                        ),
                    )
                    .await;
            }
        }
        // What the child may use: what the calling program named in `env`,
        // resolved against the contracts its run holds, or else the list
        // the parent was itself given.
        let env = match &call.env {
            Some(names) => {
                let Some(held) = self
                    .runtime
                    .program_contracts(&parent_session_id, context.parent_run_id)
                    .await?
                else {
                    return self
                        .rejected(
                            "only a program can narrow a child's capabilities with env".to_owned(),
                        )
                        .await;
                };
                let mut env = Vec::with_capacity(names.len());
                for name in names {
                    let Some(contract) = held.iter().find(|held| &held.contract == name) else {
                        return self
                            .rejected_for(
                                "outside_env",
                                format!("env names {name}, which its caller does not hold"),
                            )
                            .await;
                    };
                    env.push(contract.clone());
                }
                Some(env)
            }
            None => parent.origin.as_ref().and_then(|origin| origin.env.clone()),
        };
        // A program child can call only the contracts it requires.
        if let (Some(env), Some(program)) = (&env, &profile.document.program)
            && let Some(missing) = program.requires.iter().find(|required| {
                !env.iter()
                    .any(|granted| granted.contract == required.contract)
            })
        {
            return self
                .rejected_for(
                    "outside_env",
                    format!(
                        "program {profile_id} requires {}, which its caller did not pass in env",
                        missing.contract
                    ),
                )
                .await;
        }
        let child_session_id = child_session_id(&start.execution_id);
        let display_name = call.label.clone().or_else(|| {
            profile
                .display_name
                .clone()
                .or_else(|| Some(profile_id.as_str().to_owned()))
        });
        let origin = SessionOrigin {
            kind: if kind.is_program() {
                SessionOriginKind::Subprogram
            } else {
                SessionOriginKind::Subagent
            },
            parent_session_id: parent_session_id.clone(),
            parent_run_id: context.parent_run_id,
            root_session_id,
            depth,
            root_run_id,
            invocation_id: invocation.invocation_id.as_str().to_owned(),
            profile_id: profile_id.as_str().to_owned(),
            profile_revision: profile.revision,
            limits,
            env,
        };
        match self
            .sessions
            .create_session(CreateSession {
                session_id: child_session_id.clone(),
                display_name,
                // Copied once at spawn so a filter on the parent's campaign
                // catches its descendants; later puts do not propagate.
                metadata: parent.metadata.clone(),
                origin: Some(origin.clone()),
                delete_after_close_ms: None,
                created_at_ms: now_ms,
            })
            .await
        {
            Ok(_) => {}
            Err(SessionStoreError::SessionAlreadyExists { .. }) => {
                // Retry of this execution: the row is ours iff it names this
                // invocation; anything else is an identity collision.
                let existing = self
                    .sessions
                    .load_session(&child_session_id)
                    .await
                    .map_err(api_projection::map_session_store_error)?;
                let ours = existing.as_ref().is_some_and(|record| {
                    record
                        .origin
                        .as_ref()
                        .is_some_and(|existing| existing.invocation_id == origin.invocation_id)
                });
                if !ours {
                    return Err(AgentApiError::conflict(format!(
                        "subagent child session id collides with an unrelated session: {child_session_id}"
                    )));
                }
            }
            Err(error @ SessionStoreError::OriginLimitExceeded { .. }) => {
                return self.rejected(error.to_string()).await;
            }
            Err(error) => return Err(api_projection::map_session_store_error(error)),
        }
        // A profile that cannot be applied (a missing binding, ...) is a
        // rejected delegation the parent must see, not an activity retry: a
        // retried start finds the child workflow already running and would
        // skip the profile. A program child keeps its profile's identity.
        if let Err(error) = self
            .runtime
            .start_session(
                &child_session_id,
                ProfileSource::Inline {
                    profile: Box::new(InlineAgentProfile {
                        display_name: profile.display_name.clone(),
                        description: profile.description.clone(),
                        document,
                    }),
                },
                kind.is_program()
                    .then(|| (profile_id.clone(), profile.revision)),
            )
            .await
        {
            if is_caller_error(&error) {
                let _ = self.close(child_session_id.as_str()).await;
                return self
                    .rejected(format!("{noun} {profile_id} could not be started: {error}"))
                    .await;
            }
            return Err(error);
        }
        let submission_id = SubmissionId::new(format!(
            "subagent_run_{}",
            digest_suffix(&start.execution_id)
        ));
        // A run the child refuses (a program input outside its schema, ...)
        // is the caller's to see, like a profile that cannot be applied.
        let run_id = match self
            .runtime
            .start_run(
                &child_session_id,
                call.run,
                submission_id,
                vec![RunTerminalNotifyIntent {
                    holder_workflow_id: start.execution_id.clone(),
                    token: reply_promise_id.as_str().to_owned(),
                }],
            )
            .await
        {
            Ok(run_id) => run_id,
            Err(error) if is_caller_error(&error) => {
                let _ = self.close(child_session_id.as_str()).await;
                return self
                    .rejected(format!("{noun} {profile_id} could not run: {error}"))
                    .await;
            }
            Err(error) => return Err(error),
        };
        let run_id = parse_api_run_id(&run_id)?;
        Ok(SubagentPrepareActivityResult::Prepared {
            child: SubagentChildRef {
                session_id: child_session_id.as_str().to_owned(),
                run_id,
                agent_profile_id: profile_id.as_str().to_owned(),
                program: kind.is_program(),
            },
            deadline_ms: limits.deadline_ms,
        })
    }

    /// Resolve only attachments mentioned by the child. Media retains its
    /// active-context admission; file versions come from recorded completions,
    /// including metadata handed up by earlier children.
    async fn linked_attachments(
        &self,
        session_id: &str,
        text: &str,
    ) -> Result<Vec<Attachment>, AgentApiError> {
        let session_id = SessionId::try_new(session_id.to_owned())
            .map_err(|error| AgentApiError::internal(error.to_string()))?;
        let mut available = Vec::new();
        if text.contains("media:") {
            available.extend(
                self.runtime
                    .session_media(&session_id)
                    .await?
                    .into_iter()
                    .map(Attachment::Media),
            );
        }
        if text.contains("file:") {
            let mut after = None;
            loop {
                let page = self
                    .sessions
                    .read_after(harness::storage::ReadSessionEvents {
                        session_id: session_id.clone(),
                        after,
                        limit: 256,
                    })
                    .await
                    .map_err(api_projection::map_session_store_error)?;
                for entry in page.entries {
                    if entry.event.kind != "lightspeed.core.tool.call_completed" {
                        continue;
                    }
                    let event = harness::CoreAgentCodec
                        .decode_event(&entry.event)
                        .map_err(|error| AgentApiError::internal(error.to_string()))?;
                    if let harness::CoreAgentEvent::Tool(harness::ToolEvent::CallCompleted {
                        result,
                        ..
                    }) = event
                    {
                        available.extend(
                            result
                                .attachments
                                .into_iter()
                                .filter(|item| matches!(item, Attachment::File(_)))
                                .filter(|item| text.contains(item.handle())),
                        );
                    }
                }
                if page.complete {
                    break;
                }
                let next = page
                    .next_after
                    .filter(|next| after.is_none_or(|after| *next > after))
                    .ok_or_else(|| {
                        AgentApiError::internal("attachment history cursor did not advance")
                    })?;
                after = Some(next);
            }
        }
        Ok(linked_attachments(text, available))
    }

    /// Step C: the envelope the parent sees, then the child is closed.
    pub async fn resolve(
        &self,
        child: SubagentChildRef,
        terminal: SubagentTerminal,
    ) -> Result<PromiseResolution, AgentApiError> {
        if child.program {
            return self.resolve_program(child, terminal).await;
        }
        let (status, output, error, attachments) = match terminal {
            SubagentTerminal::Run {
                status,
                output,
                failure_message_ref,
            } => {
                let output = match output.as_ref() {
                    Some(output) => {
                        api_projection::project_content_text(self.blobs.as_ref(), output)
                            .await?
                            .map(|text| truncate_utf8(text, MAX_SUBAGENT_OUTPUT_BYTES))
                    }
                    None => None,
                };
                // Unknown handles remain text; only recorded attachments travel.
                let attachments = match output.as_deref() {
                    Some(text) if status == RunStatus::Completed => {
                        self.linked_attachments(&child.session_id, text).await?
                    }
                    _ => Vec::new(),
                };
                let failure = match failure_message_ref.as_ref() {
                    Some(failure_ref) => Some(self.read_text(failure_ref).await?),
                    None => None,
                };
                match status {
                    RunStatus::Completed => {
                        (SubagentResultStatus::Completed, output, None, attachments)
                    }
                    RunStatus::Cancelled => (
                        SubagentResultStatus::Cancelled,
                        output,
                        Some("sub-agent run was cancelled".to_owned()),
                        Vec::new(),
                    ),
                    RunStatus::Failed
                    | RunStatus::Active
                    | RunStatus::Parked
                    | RunStatus::Cancelling => (
                        SubagentResultStatus::Failed,
                        output,
                        Some(failure.unwrap_or_else(|| "sub-agent run failed".to_owned())),
                        Vec::new(),
                    ),
                }
            }
            SubagentTerminal::Deadline => (
                SubagentResultStatus::Deadline,
                None,
                Some("sub-agent run exceeded the grant deadline".to_owned()),
                Vec::new(),
            ),
        };
        let envelope = SubagentResultEnvelope {
            agent: child.agent_profile_id.clone(),
            session_id: child.session_id.clone(),
            run_id: Some(format!("run_{}", child.run_id)),
            status,
            output,
            error,
            attachments,
        };
        let payload_ref = self
            .blobs
            .put_bytes(serde_json::to_vec(&envelope).map_err(|error| {
                AgentApiError::internal(format!("encode subagent result: {error}"))
            })?)
            .await
            .map_err(|error| AgentApiError::internal(error.to_string()))?;
        harness::storage::record_contains_edges(
            self.blob_graph.as_deref(),
            &payload_ref,
            envelope.attachments.iter().flat_map(Attachment::blob_refs),
        )
        .await
        .map_err(|error| AgentApiError::internal(error.to_string()))?;
        self.close(&child.session_id).await?;
        Ok(match status {
            SubagentResultStatus::Completed => PromiseResolution::Resolved {
                payload_ref: Some(payload_ref),
            },
            SubagentResultStatus::Failed
            | SubagentResultStatus::Cancelled
            | SubagentResultStatus::Deadline => PromiseResolution::Failed {
                error_ref: Some(payload_ref),
            },
        })
    }

    /// A program child's result is its output, already checked against its
    /// output schema; otherwise the call fails with why.
    async fn resolve_program(
        &self,
        child: SubagentChildRef,
        terminal: SubagentTerminal,
    ) -> Result<PromiseResolution, AgentApiError> {
        let (status, error, failure) = match terminal {
            SubagentTerminal::Run {
                status: RunStatus::Completed,
                output: Some(output),
                ..
            } => {
                self.close(&child.session_id).await?;
                return Ok(PromiseResolution::Resolved {
                    payload_ref: Some(output.content_ref),
                });
            }
            SubagentTerminal::Run {
                status,
                failure_message_ref,
                ..
            } => {
                let failure = match failure_message_ref.as_ref() {
                    Some(failure_ref) => Some(self.read_failure(failure_ref).await?),
                    None => None,
                };
                let error = failure
                    .as_ref()
                    .and_then(|failure| failure.get("error"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                match status {
                    RunStatus::Cancelled => (
                        SubagentResultStatus::Cancelled,
                        error.or_else(|| Some("program run was cancelled".to_owned())),
                        failure,
                    ),
                    _ => (
                        SubagentResultStatus::Failed,
                        error.or_else(|| Some("program run failed".to_owned())),
                        failure,
                    ),
                }
            }
            SubagentTerminal::Deadline => (
                SubagentResultStatus::Deadline,
                Some("program run exceeded the grant deadline".to_owned()),
                None,
            ),
        };
        let failure = ProgramChildFailure {
            program: child.agent_profile_id.clone(),
            session_id: child.session_id.clone(),
            run_id: Some(format!("run_{}", child.run_id)),
            status,
            error,
            failure,
        };
        let error_ref = self
            .blobs
            .put_bytes(serde_json::to_vec(&failure).map_err(|error| {
                AgentApiError::internal(format!("encode program child failure: {error}"))
            })?)
            .await
            .map_err(|error| AgentApiError::internal(error.to_string()))?;
        self.close(&child.session_id).await?;
        Ok(PromiseResolution::Failed {
            error_ref: Some(error_ref),
        })
    }

    /// A program run's failure document, or its text when it is not JSON.
    async fn read_failure(&self, blob_ref: &BlobRef) -> Result<serde_json::Value, AgentApiError> {
        let bytes = self
            .blobs
            .read_bytes(blob_ref)
            .await
            .map_err(|error| AgentApiError::internal(error.to_string()))?;
        Ok(serde_json::from_slice(&bytes).unwrap_or_else(|_| {
            serde_json::Value::String(truncate_utf8(
                String::from_utf8_lossy(&bytes).into_owned(),
                MAX_SUBAGENT_OUTPUT_BYTES,
            ))
        }))
    }

    /// Force-close the child; an already-closed or missing child is fine.
    pub async fn close(&self, session_id: &str) -> Result<(), AgentApiError> {
        let session_id = SessionId::try_new(session_id.to_owned())
            .map_err(|error| AgentApiError::internal(format!("invalid child id: {error}")))?;
        match self.runtime.close_session(&session_id, true).await {
            Ok(()) => Ok(()),
            Err(error) if is_not_found(&error) || is_already_closed(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn rejected(
        &self,
        message: String,
    ) -> Result<SubagentPrepareActivityResult, AgentApiError> {
        self.rejection(serde_json::json!({ "error": message }))
            .await
    }

    /// A rejection the caller can tell apart by `reason`.
    async fn rejected_for(
        &self,
        reason: &str,
        message: String,
    ) -> Result<SubagentPrepareActivityResult, AgentApiError> {
        self.rejection(serde_json::json!({ "error": message, "reason": reason }))
            .await
    }

    async fn rejection(
        &self,
        document: serde_json::Value,
    ) -> Result<SubagentPrepareActivityResult, AgentApiError> {
        let error_ref = self
            .blobs
            .put_bytes(
                serde_json::to_vec(&document)
                    .map_err(|error| AgentApiError::internal(error.to_string()))?,
            )
            .await
            .map_err(|error| AgentApiError::internal(error.to_string()))?;
        Ok(SubagentPrepareActivityResult::Rejected { error_ref })
    }

    async fn read_json<T: serde::de::DeserializeOwned>(
        &self,
        blob_ref: &BlobRef,
    ) -> Result<T, AgentApiError> {
        let bytes = self
            .blobs
            .read_bytes(blob_ref)
            .await
            .map_err(|error| AgentApiError::internal(error.to_string()))?;
        serde_json::from_slice(&bytes)
            .map_err(|error| AgentApiError::invalid_request(format!("invalid JSON blob: {error}")))
    }

    /// Failure payloads may carry a JSON string or raw diagnostic text. Run
    /// output uses the content descriptor projection instead.
    async fn read_text(&self, blob_ref: &BlobRef) -> Result<String, AgentApiError> {
        let bytes = self
            .blobs
            .read_bytes(blob_ref)
            .await
            .map_err(|error| AgentApiError::internal(error.to_string()))?;
        let text = match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(serde_json::Value::String(text)) => text,
            Ok(value) => value.to_string(),
            Err(_) => String::from_utf8_lossy(&bytes).into_owned(),
        };
        Ok(truncate_utf8(text, MAX_SUBAGENT_OUTPUT_BYTES))
    }
}

/// The contracts the program run `run_id` of a session holds, from its
/// scopes' bindings, or `None` when a model drives the session. Every
/// execution of a run binds the same contracts.
fn held_program_contracts(
    state: &harness::CoreAgentState,
    run_id: u64,
) -> Option<Vec<DelegatedContract>> {
    if !state.lifecycle.driver.is_program() {
        return None;
    }
    let owner = harness::ScopeOwner::Run {
        run_id: harness::RunId::new(run_id),
    };
    let mut held = std::collections::BTreeMap::new();
    let bindings = state
        .code_tools
        .scopes
        .values()
        .filter(|scope| scope.spec.owner == owner)
        .flat_map(|scope| scope.spec.bindings.values());
    for binding in bindings {
        if let Some(contract) = &binding.contract {
            held.entry(contract.contract.clone())
                .or_insert_with(|| DelegatedContract {
                    contract: contract.contract.clone(),
                    digest: contract.digest.clone(),
                    tool_id: binding.tool_id.as_str().to_owned(),
                    tool_name: binding.tool_name.as_str().to_owned(),
                });
        }
    }
    Some(held.into_values().collect())
}

/// What a child-session call asks for, whichever tool made it.
struct ChildCall {
    profile: String,
    label: Option<String>,
    /// The contracts the calling program narrowed the child to.
    env: Option<Vec<String>>,
    run: RunStartSource,
}

/// Generic `await` argument evaluation shared by the session tool runtime.
pub fn await_spec_from_args(
    args: AwaitArgs,
    now_ms: u64,
) -> Result<harness::AwaitSpec, AgentApiError> {
    let promise_ids = args
        .validated_promise_ids()
        .map_err(|error| AgentApiError::invalid_request(error.to_string()))?;
    Ok(harness::AwaitSpec {
        promise_ids,
        mode: match args.mode {
            AwaitModeArg::All => harness::AwaitMode::All,
            AwaitModeArg::Any => harness::AwaitMode::Any,
        },
        deadline_at_ms: args
            .timeout_ms
            .map(|timeout| now_ms.saturating_add(timeout)),
    })
}

pub fn child_session_id(execution_id: &str) -> SessionId {
    SessionId::new(format!("agent_{}", digest_suffix(execution_id)))
}

fn digest_suffix(value: &str) -> String {
    let digest = BlobRef::from_bytes(value.as_bytes());
    digest
        .as_str()
        .strip_prefix("sha256:")
        .unwrap_or(digest.as_str())
        .chars()
        .take(32)
        .collect()
}

fn parse_api_run_id(run_id: &str) -> Result<u64, AgentApiError> {
    run_id
        .strip_prefix("run_")
        .and_then(|rest| rest.parse::<u64>().ok())
        .ok_or_else(|| {
            AgentApiError::internal(format!(
                "subagent runtime returned malformed run id: {run_id}"
            ))
        })
}

fn truncate_utf8(mut text: String, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text;
    }
    let mut cut = max_bytes;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    text.truncate(cut);
    text.push_str("\n…[truncated]");
    text
}

fn is_not_found(error: &AgentApiError) -> bool {
    matches!(error.kind, api::AgentApiErrorKind::NotFound)
}

/// Errors that describe the request rather than the runtime, a resource
/// the child's identity may not use included: surfaced to the parent as a
/// rejected delegation instead of retried.
fn is_caller_error(error: &AgentApiError) -> bool {
    matches!(
        error.kind,
        api::AgentApiErrorKind::NotFound
            | api::AgentApiErrorKind::InvalidRequest
            | api::AgentApiErrorKind::Rejected
            | api::AgentApiErrorKind::Conflict
            | api::AgentApiErrorKind::Forbidden
    )
}

fn is_already_closed(error: &AgentApiError) -> bool {
    matches!(error.kind, api::AgentApiErrorKind::Rejected) && error.to_string().contains("closed")
}

/// Resolve an `inherit` attachment against the parent's active environment
/// captured at admission, so the child's stored configuration names a
/// concrete machine. An explicit attachment for the same machine wins and
/// the inherit attachment is dropped; without a parent environment it is
/// dropped as well, and a `default` on it activates nothing.
pub(crate) fn resolve_inherited_environment(
    mut document: api::ProfileDocument,
    parent_environment_id: Option<&str>,
) -> api::ProfileDocument {
    let Some(environments) = document
        .config
        .as_mut()
        .and_then(|config| config.features.as_mut())
        .and_then(|features| features.environments.as_mut())
    else {
        return document;
    };
    let Some(index) = environments
        .environments
        .iter()
        .position(|attachment| attachment.inherit)
    else {
        return document;
    };
    match parent_environment_id {
        Some(parent)
            if !environments
                .environments
                .iter()
                .any(|attachment| attachment.environment_id.as_deref() == Some(parent)) =>
        {
            let attachment = &mut environments.environments[index];
            attachment.inherit = false;
            attachment.environment_id = Some(parent.to_owned());
        }
        _ => {
            environments.environments.remove(index);
        }
    }
    document
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use api::{AgentApiErrorKind, ProfileDocument};
    use harness::{
        PromiseId, RunId, SessionId, SubagentLimits, ToolBatchId, ToolCallId, TurnId,
        WorkflowToolId, WorkflowToolInvocation, WorkflowToolInvocationId,
        storage::{
            AppendSessionEvents, InMemoryBlobStore, InMemorySessionStore, SessionLifecycleStatus,
            SessionOrigin, SessionOriginKind, SessionStore,
        },
    };
    use temporal_workflow::{SubagentChildRef, SubagentTerminal};
    use tools::subagents::{
        AGENT_RUN_WORKFLOW_SEMANTIC_TYPE, AGENT_RUN_WORKFLOW_TOOL_ID,
        PROGRAM_RUN_WORKFLOW_SEMANTIC_TYPE, PROGRAM_RUN_WORKFLOW_TOOL_ID, SubagentResultEnvelope,
        SubagentResultStatus,
    };

    use super::*;

    const UNIVERSE: uuid::Uuid = uuid::Uuid::from_u128(11);

    type StartedRun = (
        String,
        RunStartSource,
        SubmissionId,
        Vec<RunTerminalNotifyIntent>,
    );

    /// Counting fake of the hosted child API.
    #[derive(Default)]
    struct FakeChildRuntime {
        profiles: Mutex<BTreeMap<String, AgentProfile>>,
        start_session_error: Mutex<Option<AgentApiError>>,
        close_error: Mutex<Option<AgentApiError>>,
        started_sessions: Mutex<Vec<(String, ProfileSource)>>,
        pinned_from: Mutex<Vec<Option<(ProfileId, u64)>>>,
        started_runs: Mutex<Vec<StartedRun>>,
        start_run_error: Mutex<Option<AgentApiError>>,
        parent_config: Mutex<Option<api::SessionConfig>>,
        /// What the parent's run holds when the parent is a program.
        parent_contracts: Mutex<Option<Vec<DelegatedContract>>>,
        closed: Mutex<Vec<(String, bool)>>,
        media: Mutex<Vec<MediaDescriptor>>,
    }

    impl FakeChildRuntime {
        fn with_profile(profile_id: &str, revision: u64) -> Arc<Self> {
            let runtime = Self::default();
            runtime.profiles.lock().unwrap().insert(
                profile_id.to_owned(),
                AgentProfile {
                    profile_id: ProfileId::new(profile_id),
                    display_name: Some("Reviewer".to_owned()),
                    description: Some("Reviews things".to_owned()),
                    revision,
                    document: ProfileDocument {
                        program: None,
                        metadata: Default::default(),
                        retention: None,
                        config: None,
                        instructions: None,
                    },
                    created_at_ms: 1,
                    updated_at_ms: 1,
                },
            );
            Arc::new(runtime)
        }

        /// A program profile whose config grants `features`.
        fn with_program(profile_id: &str, revision: u64, features: serde_json::Value) -> Arc<Self> {
            let runtime = Self::with_profile(profile_id, revision);
            let mut profiles = runtime.profiles.lock().unwrap();
            let profile = profiles.get_mut(profile_id).expect("profile");
            profile.document.program = Some(api::ProgramSpec {
                source: api::ProgramSource::Text {
                    text: "export default { run(input) { return input; } };".to_owned(),
                },
                semantics_version: 1,
                input_schema: None,
                output_schema: None,
                limits: None,
                requires: Vec::new(),
                approvals: None,
            });
            profile.document.config = Some(api::SessionConfig {
                features: Some(serde_json::from_value(features).expect("features")),
                ..Default::default()
            });
            drop(profiles);
            runtime
        }
    }

    #[async_trait]
    impl SubagentChildRuntime for FakeChildRuntime {
        async fn read_profile(&self, profile_id: ProfileId) -> Result<AgentProfile, AgentApiError> {
            self.profiles
                .lock()
                .unwrap()
                .get(profile_id.as_str())
                .cloned()
                .ok_or_else(|| AgentApiError::not_found(format!("profile {profile_id}")))
        }

        async fn start_session(
            &self,
            session_id: &SessionId,
            profile: ProfileSource,
            pinned_from: Option<(ProfileId, u64)>,
        ) -> Result<(), AgentApiError> {
            if let Some(error) = self.start_session_error.lock().unwrap().clone() {
                return Err(error);
            }
            self.started_sessions
                .lock()
                .unwrap()
                .push((session_id.as_str().to_owned(), profile));
            self.pinned_from.lock().unwrap().push(pinned_from);
            Ok(())
        }

        async fn start_run(
            &self,
            session_id: &SessionId,
            source: RunStartSource,
            submission_id: SubmissionId,
            notify_on_terminal: Vec<RunTerminalNotifyIntent>,
        ) -> Result<String, AgentApiError> {
            if let Some(error) = self.start_run_error.lock().unwrap().clone() {
                return Err(error);
            }
            self.started_runs.lock().unwrap().push((
                session_id.as_str().to_owned(),
                source,
                submission_id,
                notify_on_terminal,
            ));
            Ok("run_1".to_owned())
        }

        async fn session_config(
            &self,
            _session_id: &SessionId,
        ) -> Result<Option<api::SessionConfig>, AgentApiError> {
            Ok(self.parent_config.lock().unwrap().clone())
        }

        async fn program_contracts(
            &self,
            _session_id: &SessionId,
            _run_id: u64,
        ) -> Result<Option<Vec<DelegatedContract>>, AgentApiError> {
            Ok(self.parent_contracts.lock().unwrap().clone())
        }

        async fn close_session(
            &self,
            session_id: &SessionId,
            force: bool,
        ) -> Result<(), AgentApiError> {
            self.closed
                .lock()
                .unwrap()
                .push((session_id.as_str().to_owned(), force));
            match self.close_error.lock().unwrap().clone() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }

        async fn session_media(
            &self,
            _session_id: &SessionId,
        ) -> Result<Vec<MediaDescriptor>, AgentApiError> {
            Ok(self.media.lock().unwrap().clone())
        }
    }

    struct Harness {
        service: SubagentService,
        sessions: Arc<InMemorySessionStore>,
        blobs: Arc<InMemoryBlobStore>,
        runtime: Arc<FakeChildRuntime>,
    }

    async fn harness(runtime: Arc<FakeChildRuntime>) -> Harness {
        let sessions = Arc::new(InMemorySessionStore::new());
        let blobs = Arc::new(InMemoryBlobStore::new());
        sessions
            .create_session(CreateSession {
                metadata: Default::default(),
                session_id: SessionId::new("parent"),
                display_name: None,
                origin: None,
                delete_after_close_ms: None,
                created_at_ms: 1,
            })
            .await
            .expect("create parent");
        let service = SubagentService::new(
            sessions.clone(),
            blobs.clone(),
            runtime.clone() as Arc<dyn SubagentChildRuntime>,
        );
        Harness {
            service,
            sessions,
            blobs,
            runtime,
        }
    }

    fn reply_promise() -> PromiseId {
        PromiseId::new("promise_7")
    }

    /// A start for `agent_run` of `agent` by `parent`, admitted for
    /// `admitted_agent` with `limits`.
    async fn start_args(
        blobs: &InMemoryBlobStore,
        execution_id: &str,
        parent: &str,
        agent: &str,
        admitted_agent: &str,
        limits: SubagentLimits,
    ) -> WorkflowToolStartArgs {
        let arguments_ref = blobs
            .put_bytes(
                serde_json::to_vec(&AgentCallArgs {
                    agent: agent.to_owned(),
                    input: "review the change".to_owned(),
                    label: Some("reviewer: change".to_owned()),
                    env: None,
                })
                .unwrap(),
            )
            .await
            .expect("put arguments");
        let context_ref = blobs
            .put_bytes(
                serde_json::to_vec(&SubagentExecutionContextV1::new(
                    parent.to_owned(),
                    3,
                    admitted_agent.to_owned(),
                    limits,
                    Some("environment-parent".to_owned()),
                ))
                .unwrap(),
            )
            .await
            .expect("put context");
        let parent_id = SessionId::new(parent);
        WorkflowToolStartArgs {
            universe_id: UNIVERSE,
            holder_workflow_id: temporal_workflow::compose_workflow_id(UNIVERSE, &parent_id),
            execution_id: execution_id.to_owned(),
            invocation: WorkflowToolInvocation {
                invocation_id: WorkflowToolInvocationId::new(format!(
                    "wti:sha256:{}",
                    execution_id
                        .chars()
                        .last()
                        .unwrap_or('a')
                        .to_string()
                        .repeat(64)
                )),
                tool_id: WorkflowToolId::new(AGENT_RUN_WORKFLOW_TOOL_ID),
                semantic_type: AGENT_RUN_WORKFLOW_SEMANTIC_TYPE.to_owned(),
                schema_revision: 1,
                binding_fingerprint: "binding:v1:test".to_owned(),
                session_universe_id: UNIVERSE,
                session_id: parent_id,
                run_id: RunId::new(3),
                context: harness::CallContext::ModelBatch(harness::ModelBatchCall {
                    turn_id: TurnId::new(1),
                    tool_batch_id: ToolBatchId::new(1),
                    tool_call_id: ToolCallId::new("call_agent"),
                }),
                arguments_ref,
                execution_context_ref: Some(context_ref),
                completion_promises: Some(BTreeMap::from([(
                    harness::REPLY_COMPLETION_KEY.to_owned(),
                    reply_promise(),
                )])),
            },
        }
    }

    /// A start for `program_run` of `program` by `parent`, admitted for it.
    async fn program_start_args(
        blobs: &InMemoryBlobStore,
        execution_id: &str,
        program: &str,
    ) -> WorkflowToolStartArgs {
        let mut start = start_args(
            blobs,
            execution_id,
            "parent",
            program,
            program,
            SubagentLimits::default(),
        )
        .await;
        start.invocation.tool_id = WorkflowToolId::new(PROGRAM_RUN_WORKFLOW_TOOL_ID);
        start.invocation.semantic_type = PROGRAM_RUN_WORKFLOW_SEMANTIC_TYPE.to_owned();
        start.invocation.arguments_ref = blobs
            .put_bytes(
                serde_json::to_vec(&serde_json::json!({
                    "program": program,
                    "input": { "claim": "x" },
                }))
                .unwrap(),
            )
            .await
            .expect("put arguments");
        start
    }

    async fn rejection(
        blobs: &InMemoryBlobStore,
        result: SubagentPrepareActivityResult,
    ) -> serde_json::Value {
        match result {
            SubagentPrepareActivityResult::Rejected { error_ref } => {
                serde_json::from_slice(&blobs.read_bytes(&error_ref).await.expect("read"))
                    .expect("decode rejection")
            }
            SubagentPrepareActivityResult::Prepared { .. } => panic!("expected a rejection"),
        }
    }

    fn prepared(result: SubagentPrepareActivityResult) -> (SubagentChildRef, u64) {
        match result {
            SubagentPrepareActivityResult::Prepared { child, deadline_ms } => (child, deadline_ms),
            SubagentPrepareActivityResult::Rejected { .. } => panic!("expected a prepared child"),
        }
    }

    async fn rejection_message(
        blobs: &InMemoryBlobStore,
        result: SubagentPrepareActivityResult,
    ) -> String {
        match result {
            SubagentPrepareActivityResult::Rejected { error_ref } => {
                let value: serde_json::Value =
                    serde_json::from_slice(&blobs.read_bytes(&error_ref).await.expect("read"))
                        .expect("decode rejection");
                value["error"].as_str().expect("error text").to_owned()
            }
            SubagentPrepareActivityResult::Prepared { .. } => panic!("expected a rejection"),
        }
    }

    #[test]
    fn inherit_attachment_resolves_against_the_captured_parent_environment() {
        fn document(attachments: Vec<api::EnvironmentAttachment>) -> ProfileDocument {
            ProfileDocument {
                program: None,
                config: Some(api::SessionConfig {
                    features: Some(api::FeaturesConfig {
                        environments: Some(api::EnvironmentsFeature {
                            version: api::CURRENT_FEATURE_VERSION,
                            selection: false,
                            prompts: None,
                            skills: None,
                            environments: attachments,
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }
        }
        fn attachment(
            id: Option<&str>,
            inherit: bool,
            default: bool,
        ) -> api::EnvironmentAttachment {
            api::EnvironmentAttachment {
                environment_id: id.map(str::to_owned),
                inherit,
                default,
                access: api::EnvironmentAccess::Exec,
                working_directory: None,
            }
        }
        fn attachments(document: &ProfileDocument) -> &[api::EnvironmentAttachment] {
            &document
                .config
                .as_ref()
                .unwrap()
                .features
                .as_ref()
                .unwrap()
                .environments
                .as_ref()
                .unwrap()
                .environments
        }

        // Concrete: the inherit attachment becomes the parent's machine.
        let resolved = resolve_inherited_environment(
            document(vec![attachment(None, true, true)]),
            Some("env_parent"),
        );
        let resolved = attachments(&resolved);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].environment_id.as_deref(), Some("env_parent"));
        assert!(!resolved[0].inherit);
        assert!(resolved[0].default);

        // The explicit attachment for the same machine wins.
        let explicit = resolve_inherited_environment(
            document(vec![
                attachment(Some("env_parent"), false, false),
                attachment(None, true, true),
            ]),
            Some("env_parent"),
        );
        let explicit = attachments(&explicit);
        assert_eq!(explicit.len(), 1);
        assert_eq!(explicit[0].environment_id.as_deref(), Some("env_parent"));
        assert!(!explicit[0].default);

        // No parent environment: the inherit attachment is dropped.
        let dropped = resolve_inherited_environment(
            document(vec![
                attachment(Some("env_other"), false, false),
                attachment(None, true, true),
            ]),
            None,
        );
        assert_eq!(attachments(&dropped).len(), 1);
        assert!(attachments(&dropped)[0].environment_id.as_deref() == Some("env_other"));

        // Nothing to resolve leaves the document untouched.
        let untouched = resolve_inherited_environment(ProfileDocument::default(), Some("x"));
        assert_eq!(untouched, ProfileDocument::default());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_creates_the_child_with_its_origin_and_starts_its_run() {
        let runtime = FakeChildRuntime::with_profile("reviewer", 4);
        let h = harness(runtime).await;
        let limits = SubagentLimits {
            deadline_ms: 30_000,
            ..SubagentLimits::default()
        };
        let start = start_args(&h.blobs, "wte:a", "parent", "reviewer", "reviewer", limits).await;

        let (child, deadline_ms) = prepared(
            h.service
                .prepare(start.clone(), 100)
                .await
                .expect("prepare"),
        );
        assert_eq!(child.session_id, child_session_id("wte:a").as_str());
        assert_eq!(child.run_id, 1);
        assert_eq!(child.agent_profile_id, "reviewer");
        assert_eq!(deadline_ms, 30_000);

        let record = h
            .sessions
            .load_session(&SessionId::new(&child.session_id))
            .await
            .expect("load")
            .expect("child row reserved");
        assert_eq!(record.display_name.as_deref(), Some("reviewer: change"));
        let origin = record.origin.expect("origin");
        assert_eq!(origin.kind, SessionOriginKind::Subagent);
        assert_eq!(origin.parent_session_id.as_str(), "parent");
        assert_eq!(origin.root_session_id.as_str(), "parent");
        assert_eq!(origin.parent_run_id, 3);
        assert_eq!(origin.depth, 1);
        assert_eq!(origin.profile_id, "reviewer");
        assert_eq!(origin.profile_revision, 4);
        assert_eq!(origin.limits, limits);
        assert_eq!(
            origin.invocation_id,
            start.invocation.invocation_id.as_str()
        );

        let started = h.runtime.started_sessions.lock().unwrap().clone();
        assert_eq!(started.len(), 1);
        assert_eq!(started[0].0, child.session_id);
        assert!(matches!(started[0].1, ProfileSource::Inline { .. }));
        let runs = h.runtime.started_runs.lock().unwrap().clone();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].0, child.session_id);
        assert_eq!(
            runs[0].1,
            RunStartSource::Input {
                items: vec![InputItem::Text {
                    provenance_ref: None,
                    origin: None,
                    text: "review the change".to_owned()
                }]
            }
        );
        assert_eq!(
            runs[0].3,
            vec![RunTerminalNotifyIntent {
                holder_workflow_id: "wte:a".to_owned(),
                token: reply_promise().as_str().to_owned(),
            }],
            "the child's terminal must be addressed to the execution with the reply promise as token"
        );
        assert!(h.runtime.closed.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_retry_reuses_the_reserved_child() {
        let runtime = FakeChildRuntime::with_profile("reviewer", 1);
        let h = harness(runtime).await;
        let start = start_args(
            &h.blobs,
            "wte:a",
            "parent",
            "reviewer",
            "reviewer",
            SubagentLimits::default(),
        )
        .await;

        let first = h
            .service
            .prepare(start.clone(), 100)
            .await
            .expect("first prepare");
        let second = h
            .service
            .prepare(start, 200)
            .await
            .expect("retried prepare");
        assert_eq!(first, second);
        let listed = h
            .sessions
            .list_sessions(harness::storage::ListSessions {
                limit: 10,
                trees: vec![SessionId::new("parent")],
                subagent: Some(true),
                ..Default::default()
            })
            .await
            .expect("list")
            .sessions;
        assert_eq!(listed.len(), 1, "a retry must not reserve a second slot");
        assert_eq!(
            h.runtime.started_runs.lock().unwrap().len(),
            2,
            "the run start is re-issued with the same submission id"
        );
        let runs = h.runtime.started_runs.lock().unwrap().clone();
        assert_eq!(runs[0].2, runs[1].2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_rejects_an_agent_other_than_the_admitted_one() {
        let runtime = FakeChildRuntime::with_profile("reviewer", 1);
        let h = harness(runtime).await;
        let start = start_args(
            &h.blobs,
            "wte:a",
            "parent",
            "planner",
            "reviewer",
            SubagentLimits::default(),
        )
        .await;

        let result = h.service.prepare(start, 100).await.expect("prepare");
        let message = rejection_message(&h.blobs, result).await;
        assert!(
            message.contains("does not match the admitted agent"),
            "{message}"
        );
        assert!(h.runtime.started_sessions.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_rejects_a_missing_profile_without_reserving() {
        let runtime = Arc::new(FakeChildRuntime::default());
        let h = harness(runtime).await;
        let start = start_args(
            &h.blobs,
            "wte:a",
            "parent",
            "reviewer",
            "reviewer",
            SubagentLimits::default(),
        )
        .await;

        let result = h.service.prepare(start, 100).await.expect("prepare");
        let message = rejection_message(&h.blobs, result).await;
        assert!(
            message.contains("agent profile does not exist"),
            "{message}"
        );
        assert!(
            h.sessions
                .load_session(&child_session_id("wte:a"))
                .await
                .expect("load")
                .is_none(),
            "no child row without a profile"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_rejects_when_the_root_limit_is_exceeded() {
        let runtime = FakeChildRuntime::with_profile("reviewer", 1);
        let h = harness(runtime).await;
        let limits = SubagentLimits {
            max_descendants: Some(1),
            max_per_run: 1,
            ..SubagentLimits::default()
        };
        let first = start_args(&h.blobs, "wte:a", "parent", "reviewer", "reviewer", limits).await;
        let second = start_args(&h.blobs, "wte:b", "parent", "reviewer", "reviewer", limits).await;

        assert!(matches!(
            h.service.prepare(first, 100).await.expect("first prepare"),
            SubagentPrepareActivityResult::Prepared { .. }
        ));
        let result = h
            .service
            .prepare(second, 101)
            .await
            .expect("second prepare");
        let message = rejection_message(&h.blobs, result).await;
        assert!(message.contains("maxDescendants"), "{message}");
        assert_eq!(h.runtime.started_sessions.lock().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_counts_the_children_of_one_run_of_the_root() {
        let runtime = FakeChildRuntime::with_profile("reviewer", 1);
        let h = harness(runtime).await;
        let limits = SubagentLimits {
            max_per_run: 1,
            ..SubagentLimits::default()
        };
        let first = start_args(&h.blobs, "wte:a", "parent", "reviewer", "reviewer", limits).await;
        let second = start_args(&h.blobs, "wte:b", "parent", "reviewer", "reviewer", limits).await;

        let (child, _) = prepared(h.service.prepare(first, 100).await.expect("first prepare"));
        let origin = h
            .sessions
            .load_session(&SessionId::new(&child.session_id))
            .await
            .expect("load")
            .expect("child")
            .origin
            .expect("origin");
        assert_eq!(
            origin.root_run_id,
            Some(origin.parent_run_id),
            "the root's own child is made under the root's run"
        );
        let result = h
            .service
            .prepare(second, 101)
            .await
            .expect("second prepare");
        let message = rejection_message(&h.blobs, result).await;
        assert!(message.contains("maxPerRun"), "{message}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_attenuates_the_grant_by_the_parents_origin() {
        let runtime = FakeChildRuntime::with_profile("reviewer", 1);
        let h = harness(runtime).await;
        // `mid` is itself a depth-1 child of `root` with tighter limits; it
        // counts as one open descendant of `root` itself.
        let parent_limits = SubagentLimits {
            max_depth: 3,
            max_descendants: Some(2),
            max_per_run: 2,
            max_concurrent: 2,
            deadline_ms: 5_000,
        };
        h.sessions
            .create_session(CreateSession {
                metadata: Default::default(),
                session_id: SessionId::new("root"),
                display_name: None,
                origin: None,
                delete_after_close_ms: None,
                created_at_ms: 1,
            })
            .await
            .expect("create root");
        h.sessions
            .create_session(CreateSession {
                metadata: Default::default(),
                session_id: SessionId::new("mid"),
                display_name: None,
                origin: Some(SessionOrigin {
                    kind: SessionOriginKind::Subagent,
                    parent_session_id: SessionId::new("root"),
                    parent_run_id: 1,
                    root_session_id: SessionId::new("root"),
                    depth: 1,
                    root_run_id: Some(7),
                    invocation_id: format!("wti:sha256:{}", "c".repeat(64)),
                    profile_id: "planner".to_owned(),
                    profile_revision: 1,
                    limits: parent_limits,
                    env: None,
                }),
                delete_after_close_ms: None,
                created_at_ms: 2,
            })
            .await
            .expect("create mid");
        let start = start_args(
            &h.blobs,
            "wte:a",
            "mid",
            "reviewer",
            "reviewer",
            SubagentLimits::default(),
        )
        .await;

        let (child, deadline_ms) = prepared(h.service.prepare(start, 100).await.expect("prepare"));
        assert_eq!(
            deadline_ms, 5_000,
            "deadline attenuated by the parent's origin"
        );
        let origin = h
            .sessions
            .load_session(&SessionId::new(&child.session_id))
            .await
            .expect("load")
            .expect("child")
            .origin
            .expect("origin");
        assert_eq!(origin.root_session_id.as_str(), "root");
        assert_eq!(origin.parent_session_id.as_str(), "mid");
        assert_eq!(origin.depth, 2);
        assert_eq!(
            origin.root_run_id,
            Some(7),
            "a grandchild counts against the root's run its parent was made under"
        );
        assert_eq!(
            origin.limits,
            SubagentLimits {
                max_depth: 2,
                max_descendants: Some(2),
                max_per_run: 2,
                max_concurrent: 2,
                deadline_ms: 5_000,
            }
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_turns_a_caller_error_on_child_start_into_a_rejection_and_closes_the_child() {
        let runtime = FakeChildRuntime::with_profile("reviewer", 1);
        *runtime.start_session_error.lock().unwrap() = Some(AgentApiError::rejected(
            "inherit requires a parent environment",
        ));
        let h = harness(runtime).await;
        let start = start_args(
            &h.blobs,
            "wte:a",
            "parent",
            "reviewer",
            "reviewer",
            SubagentLimits::default(),
        )
        .await;

        let result = h.service.prepare(start, 100).await.expect("prepare");
        let message = rejection_message(&h.blobs, result).await;
        assert!(message.contains("could not be started"), "{message}");
        assert!(
            message.contains("inherit requires a parent environment"),
            "{message}"
        );
        assert_eq!(
            h.runtime.closed.lock().unwrap().clone(),
            vec![(child_session_id("wte:a").as_str().to_owned(), true)],
            "the half-made child is force-closed"
        );
        assert!(h.runtime.started_runs.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_surfaces_an_infrastructure_error_on_child_start() {
        let runtime = FakeChildRuntime::with_profile("reviewer", 1);
        *runtime.start_session_error.lock().unwrap() =
            Some(AgentApiError::internal("temporal unavailable"));
        let h = harness(runtime).await;
        let start = start_args(
            &h.blobs,
            "wte:a",
            "parent",
            "reviewer",
            "reviewer",
            SubagentLimits::default(),
        )
        .await;

        let error = h
            .service
            .prepare(start, 100)
            .await
            .expect_err("infrastructure error");
        assert_eq!(error.kind, AgentApiErrorKind::Internal);
        assert!(
            h.runtime.closed.lock().unwrap().is_empty(),
            "a retryable error keeps the child for the retry"
        );
    }

    async fn envelope_of(
        blobs: &InMemoryBlobStore,
        resolution: &PromiseResolution,
    ) -> SubagentResultEnvelope {
        let payload_ref = match resolution {
            PromiseResolution::Resolved { payload_ref } => payload_ref.clone(),
            PromiseResolution::Failed { error_ref } => error_ref.clone(),
            other => panic!("unexpected resolution {other:?}"),
        }
        .expect("payload ref");
        serde_json::from_slice(&blobs.read_bytes(&payload_ref).await.expect("read"))
            .expect("decode envelope")
    }

    fn child_ref() -> SubagentChildRef {
        SubagentChildRef {
            session_id: "agent_child".to_owned(),
            run_id: 5,
            agent_profile_id: "reviewer".to_owned(),
            program: false,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_program_child_is_pinned_to_its_profile_and_runs_its_json_input() {
        let runtime =
            FakeChildRuntime::with_program("checker", 4, serde_json::json!({ "timers": {} }));
        *runtime.parent_config.lock().unwrap() = Some(api::SessionConfig {
            features: Some(
                serde_json::from_value(serde_json::json!({ "timers": {}, "web": { "fetch": {} } }))
                    .unwrap(),
            ),
            ..Default::default()
        });
        let h = harness(runtime).await;
        let start = program_start_args(&h.blobs, "wte:c", "checker").await;
        let (child, _) = prepared(h.service.prepare(start, 10).await.expect("prepare"));
        assert!(child.program);
        assert_eq!(child.agent_profile_id, "checker");
        assert_eq!(
            *h.runtime.pinned_from.lock().unwrap(),
            [Some((ProfileId::new("checker"), 4))],
            "the child program records the profile revision it was copied from"
        );
        let record = h
            .sessions
            .load_session(&SessionId::new(&child.session_id))
            .await
            .expect("load")
            .expect("child row reserved");
        assert_eq!(
            record.origin.expect("origin").kind,
            SessionOriginKind::Subprogram,
            "a program child says a program, not an agent, was asked for"
        );
        let runs = h.runtime.started_runs.lock().unwrap().clone();
        assert_eq!(
            runs[0].1,
            RunStartSource::Program {
                input: serde_json::json!({ "claim": "x" }),
                input_defaults: None,
            }
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_program_child_that_asks_for_more_than_its_caller_holds_is_never_created() {
        let runtime =
            FakeChildRuntime::with_program("checker", 1, serde_json::json!({ "timers": {} }));
        let h = harness(runtime).await;
        let start = program_start_args(&h.blobs, "wte:c", "checker").await;
        let refused = rejection(
            &h.blobs,
            h.service.prepare(start, 10).await.expect("prepare"),
        )
        .await;
        assert_eq!(refused["reason"], "grant_exceeded");
        assert!(
            refused["error"]
                .as_str()
                .is_some_and(|error| error.contains("the timers feature")),
            "{refused}"
        );
        assert!(h.runtime.started_sessions.lock().unwrap().is_empty());
        let children = h
            .sessions
            .load_session(&child_session_id("wte:c"))
            .await
            .expect("load");
        assert!(children.is_none(), "no tree slot was taken");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_program_call_naming_an_agent_or_refused_by_its_child_is_rejected() {
        // The listed program was edited into an agent profile.
        let runtime = FakeChildRuntime::with_profile("checker", 1);
        let h = harness(runtime).await;
        let start = program_start_args(&h.blobs, "wte:c", "checker").await;
        let message = rejection_message(
            &h.blobs,
            h.service.prepare(start, 10).await.expect("prepare"),
        )
        .await;
        assert!(
            message.contains("is no longer a program profile"),
            "{message}"
        );

        // The child refuses the input (outside its input schema, ...).
        let runtime = FakeChildRuntime::with_program("checker", 1, serde_json::json!({}));
        *runtime.start_run_error.lock().unwrap() = Some(AgentApiError::invalid_request(
            "program input does not satisfy the program's input schema",
        ));
        let h = harness(runtime).await;
        let start = program_start_args(&h.blobs, "wte:d", "checker").await;
        let message = rejection_message(
            &h.blobs,
            h.service.prepare(start, 10).await.expect("prepare"),
        )
        .await;
        assert!(message.contains("could not run"), "{message}");
        assert_eq!(
            h.runtime.closed.lock().unwrap().as_slice(),
            [(child_session_id("wte:d").as_str().to_owned(), true)]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_program_input_outside_its_schema_is_refused_before_a_child_exists() {
        let runtime = FakeChildRuntime::with_program("checker", 1, serde_json::json!({}));
        runtime
            .profiles
            .lock()
            .unwrap()
            .get_mut("checker")
            .and_then(|profile| profile.document.program.as_mut())
            .expect("program")
            .input_schema = Some(serde_json::json!({
            "type": "object",
            "required": ["claim"],
            "properties": { "claim": { "type": "integer" } },
        }));
        let h = harness(runtime).await;
        let start = program_start_args(&h.blobs, "wte:c", "checker").await;
        let message = rejection_message(
            &h.blobs,
            h.service.prepare(start, 10).await.expect("prepare"),
        )
        .await;
        assert!(
            message.contains("does not satisfy its input schema"),
            "{message}"
        );
        assert!(h.runtime.started_sessions.lock().unwrap().is_empty());
        assert!(
            h.sessions
                .load_session(&child_session_id("wte:c"))
                .await
                .expect("load")
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_program_child_resolves_to_its_output_or_fails_with_its_failure() {
        let runtime = Arc::new(FakeChildRuntime::default());
        let h = harness(runtime).await;
        let child = SubagentChildRef {
            program: true,
            agent_profile_id: "checker".to_owned(),
            ..child_ref()
        };
        let output_ref = h
            .blobs
            .put_bytes(br#"{"verdict":"ok"}"#.to_vec())
            .await
            .unwrap();
        let resolved = h
            .service
            .resolve(
                child.clone(),
                SubagentTerminal::Run {
                    status: RunStatus::Completed,
                    output: Some(harness::ContentRef {
                        content_ref: output_ref.clone(),
                        media_type: Some("application/json".to_owned()),
                        provider_kind: None,
                    }),
                    failure_message_ref: None,
                },
            )
            .await
            .expect("resolve");
        assert_eq!(
            resolved,
            PromiseResolution::Resolved {
                payload_ref: Some(output_ref)
            }
        );

        let failure_ref = h
            .blobs
            .put_bytes(
                br#"{"status":"threw","error":"no sources","report_ref":"sha256:r"}"#.to_vec(),
            )
            .await
            .unwrap();
        let failed = h
            .service
            .resolve(
                child,
                SubagentTerminal::Run {
                    status: RunStatus::Failed,
                    output: None,
                    failure_message_ref: Some(failure_ref),
                },
            )
            .await
            .expect("resolve");
        let PromiseResolution::Failed {
            error_ref: Some(error_ref),
        } = failed
        else {
            panic!("expected a failure: {failed:?}");
        };
        let failure: ProgramChildFailure =
            serde_json::from_slice(&h.blobs.read_bytes(&error_ref).await.unwrap()).unwrap();
        assert_eq!(failure.status, SubagentResultStatus::Failed);
        assert_eq!(failure.error.as_deref(), Some("no sources"));
        assert_eq!(failure.failure.as_ref().unwrap()["status"], "threw");
        assert_eq!(h.runtime.closed.lock().unwrap().len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_maps_a_completed_run_to_a_resolved_envelope_and_closes_the_child() {
        let runtime = Arc::new(FakeChildRuntime::default());
        let h = harness(runtime).await;
        let output = h
            .blobs
            .put_bytes(b"looks good".to_vec())
            .await
            .expect("put output");

        let resolution = h
            .service
            .resolve(
                child_ref(),
                SubagentTerminal::Run {
                    status: RunStatus::Completed,
                    output: Some(harness::ContentRef::text(output)),
                    failure_message_ref: None,
                },
            )
            .await
            .expect("resolve");
        assert!(matches!(resolution, PromiseResolution::Resolved { .. }));
        let envelope = envelope_of(&h.blobs, &resolution).await;
        assert_eq!(envelope.status, SubagentResultStatus::Completed);
        assert_eq!(envelope.agent, "reviewer");
        assert_eq!(envelope.session_id, "agent_child");
        assert_eq!(envelope.run_id.as_deref(), Some("run_5"));
        assert_eq!(envelope.output.as_deref(), Some("looks good"));
        assert_eq!(envelope.error, None);
        assert_eq!(
            h.runtime.closed.lock().unwrap().clone(),
            vec![("agent_child".to_owned(), true)]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_hands_up_the_media_the_child_linked_by_handle_in_link_order() {
        let runtime = Arc::new(FakeChildRuntime::default());
        let png = MediaDescriptor::new(
            BlobRef::from_bytes(b"png bytes"),
            "image/png",
            Some("render.png"),
        )
        .expect("png");
        let pdf = MediaDescriptor::new(BlobRef::from_bytes(b"%PDF"), "application/pdf", None)
            .expect("pdf");
        let unseen = MediaDescriptor::new(BlobRef::from_bytes(b"other"), "image/jpeg", None)
            .expect("unseen");
        *runtime.media.lock().unwrap() = vec![png.clone(), pdf.clone()];
        let h = harness(runtime).await;
        let text = format!(
            "The brief is [here]({}), the render ![r]({}) and again {}; not ours: {}",
            pdf.handle, png.handle, png.handle, unseen.handle
        );
        let output = h.blobs.put_bytes(text.into_bytes()).await.expect("output");

        let resolution = h
            .service
            .resolve(
                child_ref(),
                SubagentTerminal::Run {
                    status: RunStatus::Completed,
                    output: Some(harness::ContentRef::text(output.clone())),
                    failure_message_ref: None,
                },
            )
            .await
            .expect("resolve");
        let envelope = envelope_of(&h.blobs, &resolution).await;
        assert_eq!(
            envelope.attachments,
            vec![
                Attachment::Media(pdf.clone()),
                Attachment::Media(png.clone())
            ]
        );

        // A failed run hands nothing up even when its text links media.
        let failed = h
            .service
            .resolve(
                child_ref(),
                SubagentTerminal::Run {
                    status: RunStatus::Failed,
                    output: Some(harness::ContentRef::text(output)),
                    failure_message_ref: None,
                },
            )
            .await
            .expect("resolve failed");
        let envelope = envelope_of(&h.blobs, &failed).await;
        assert!(envelope.attachments.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn file_handoff_uses_recorded_versions_and_retains_them_for_nested_results() {
        use harness::storage::{AppendSessionEvents, BlobEdge, UncommittedStoredEvent};
        let mut h = harness(Arc::new(FakeChildRuntime::default())).await;
        h.service = h.service.with_blob_graph(Some(h.blobs.clone()));
        let original = h
            .blobs
            .put_bytes(b"original report".to_vec())
            .await
            .unwrap();
        let reference = harness::FileAttachment::new(original.clone(), "report.txt".into(), None);
        let unwanted = harness::FileAttachment::new(
            h.blobs.put_bytes(b"intermediate".to_vec()).await.unwrap(),
            "intermediate.txt".into(),
            None,
        );
        let child_id = SessionId::new("agent_child");
        h.sessions
            .create_session(CreateSession {
                session_id: child_id.clone(),
                metadata: Default::default(),
                display_name: None,
                origin: None,
                delete_after_close_ms: None,
                created_at_ms: 1,
            })
            .await
            .unwrap();
        let completion = |attachments| {
            harness::CoreAgentEvent::Tool(harness::ToolEvent::CallCompleted {
                run_id: harness::RunId::new(5),
                turn_id: harness::TurnId::new(1),
                batch_id: harness::ToolBatchId::new(1),
                result: harness::ToolCallResult {
                    attachments,
                    call_id: harness::ToolCallId::new("call"),
                    status: harness::ToolCallStatus::Succeeded,
                    output_ref: None,
                    model_visible_context_entries: vec![],
                    error_ref: None,
                    effects: Vec::new(),
                    duration_ms: None,
                    output_bytes: None,
                    truncated: false,
                },
            })
        };
        h.sessions
            .append(AppendSessionEvents {
                session_id: child_id.clone(),
                expected_head: None,
                events: vec![UncommittedStoredEvent {
                    observed_at_ms: 1,
                    joins: Default::default(),
                    event: harness::CoreAgentCodec
                        .encode_event(&completion(vec![
                            Attachment::File(reference.clone()),
                            Attachment::File(unwanted.clone()),
                        ]))
                        .unwrap(),
                }],
            })
            .await
            .unwrap();
        let text = format!(
            "[report]({}) unknown file:000000000000000000000000",
            reference.handle
        );
        let output = h.blobs.put_bytes(text.into_bytes()).await.unwrap();
        let resolution = h
            .service
            .resolve(
                child_ref(),
                SubagentTerminal::Run {
                    status: RunStatus::Completed,
                    output: Some(harness::ContentRef::text(output.clone())),
                    failure_message_ref: None,
                },
            )
            .await
            .unwrap();
        let envelope = envelope_of(&h.blobs, &resolution).await;
        assert_eq!(
            envelope.attachments,
            vec![Attachment::File(reference.clone())]
        );
        let PromiseResolution::Resolved {
            payload_ref: Some(payload),
        } = resolution
        else {
            panic!("resolved payload");
        };
        assert!(
            h.blobs
                .edges()
                .contains(&BlobEdge::contains(payload, original.clone()))
        );
        // A parent completion carries the same facts, so another level can forward
        // them without the original workspace, environment, or child context.
        h.sessions
            .append(AppendSessionEvents {
                session_id: SessionId::new("parent"),
                expected_head: None,
                events: vec![UncommittedStoredEvent {
                    observed_at_ms: 2,
                    joins: Default::default(),
                    event: harness::CoreAgentCodec
                        .encode_event(&completion(vec![Attachment::File(reference.clone())]))
                        .unwrap(),
                }],
            })
            .await
            .unwrap();
        assert_eq!(
            h.service
                .linked_attachments("parent", &reference.handle)
                .await
                .unwrap(),
            envelope.attachments
        );
        assert_eq!(
            h.blobs.read_bytes(&original).await.unwrap(),
            b"original report"
        );
        for status in [RunStatus::Failed, RunStatus::Cancelled] {
            let resolution = h
                .service
                .resolve(
                    child_ref(),
                    SubagentTerminal::Run {
                        status,
                        output: Some(harness::ContentRef::text(output.clone())),
                        failure_message_ref: None,
                    },
                )
                .await
                .unwrap();
            assert!(
                envelope_of(&h.blobs, &resolution)
                    .await
                    .attachments
                    .is_empty()
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_child_output_projects_text_without_exposing_provider_metadata() {
        let h = harness(Arc::new(FakeChildRuntime::default())).await;
        let bytes = serde_json::to_vec(&serde_json::json!([
            {"type":"text", "text":"Answer: "},
            {"type":"text", "text":"42", "citations":[{"encrypted_index":"provider-only"}]}
        ]))
        .unwrap();
        let content_ref = h.blobs.put_bytes(bytes).await.unwrap();
        let resolution = h
            .service
            .resolve(
                child_ref(),
                SubagentTerminal::Run {
                    status: RunStatus::Completed,
                    output: Some(harness::ContentRef {
                        content_ref,
                        media_type: Some("application/json".into()),
                        provider_kind: Some(
                            harness::ANTHROPIC_MESSAGES_TEXT_BLOCKS_PROVIDER_KIND.into(),
                        ),
                    }),
                    failure_message_ref: None,
                },
            )
            .await
            .unwrap();
        let envelope = envelope_of(&h.blobs, &resolution).await;
        assert_eq!(envelope.output.as_deref(), Some("Answer: 42"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_maps_failed_cancelled_and_deadline_terminals_to_failed_envelopes() {
        let runtime = Arc::new(FakeChildRuntime::default());
        let h = harness(runtime).await;
        let failure_ref = h
            .blobs
            .put_bytes(b"provider exploded".to_vec())
            .await
            .expect("put failure");

        let failed = h
            .service
            .resolve(
                child_ref(),
                SubagentTerminal::Run {
                    status: RunStatus::Failed,
                    output: None,
                    failure_message_ref: Some(failure_ref),
                },
            )
            .await
            .expect("resolve failed");
        assert!(matches!(failed, PromiseResolution::Failed { .. }));
        let envelope = envelope_of(&h.blobs, &failed).await;
        assert_eq!(envelope.status, SubagentResultStatus::Failed);
        assert_eq!(envelope.error.as_deref(), Some("provider exploded"));

        let cancelled = h
            .service
            .resolve(
                child_ref(),
                SubagentTerminal::Run {
                    status: RunStatus::Cancelled,
                    output: None,
                    failure_message_ref: None,
                },
            )
            .await
            .expect("resolve cancelled");
        assert!(matches!(cancelled, PromiseResolution::Failed { .. }));
        assert_eq!(
            envelope_of(&h.blobs, &cancelled).await.status,
            SubagentResultStatus::Cancelled
        );

        let deadline = h
            .service
            .resolve(child_ref(), SubagentTerminal::Deadline)
            .await
            .expect("resolve deadline");
        assert!(matches!(deadline, PromiseResolution::Failed { .. }));
        let envelope = envelope_of(&h.blobs, &deadline).await;
        assert_eq!(envelope.status, SubagentResultStatus::Deadline);
        assert!(
            envelope
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("deadline")
        );
        assert_eq!(envelope.output, None);
        assert_eq!(
            h.runtime.closed.lock().unwrap().len(),
            3,
            "every terminal closes the child"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_tolerates_a_missing_or_already_closed_child() {
        let runtime = Arc::new(FakeChildRuntime::default());
        let h = harness(runtime).await;

        *h.runtime.close_error.lock().unwrap() = Some(AgentApiError::not_found("session"));
        h.service
            .close("agent_child")
            .await
            .expect("missing child is fine");
        *h.runtime.close_error.lock().unwrap() =
            Some(AgentApiError::rejected("session is already closed"));
        h.service
            .close("agent_child")
            .await
            .expect("closed child is fine");
        *h.runtime.close_error.lock().unwrap() = Some(AgentApiError::internal("store down"));
        let error = h
            .service
            .close("agent_child")
            .await
            .expect_err("infrastructure error");
        assert_eq!(error.kind, AgentApiErrorKind::Internal);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn child_session_ids_are_stable_and_distinct_per_execution() {
        assert_eq!(child_session_id("wte:a"), child_session_id("wte:a"));
        assert_ne!(child_session_id("wte:a"), child_session_id("wte:b"));
        assert!(child_session_id("wte:a").as_str().starts_with("agent_"));
        assert_eq!(
            child_session_id("wte:a").as_str().len(),
            "agent_".len() + 32
        );
    }

    /// The in-memory store counts a closed child as no longer open; the
    /// service's reservation goes through the same store rule.
    #[tokio::test(flavor = "current_thread")]
    async fn prepare_reserves_a_concurrency_slot_freed_by_a_closed_child() {
        let runtime = FakeChildRuntime::with_profile("reviewer", 1);
        let h = harness(runtime).await;
        let limits = SubagentLimits {
            max_concurrent: 1,
            ..SubagentLimits::default()
        };
        let first = start_args(&h.blobs, "wte:a", "parent", "reviewer", "reviewer", limits).await;
        let second = start_args(&h.blobs, "wte:b", "parent", "reviewer", "reviewer", limits).await;
        let (child, _) = prepared(h.service.prepare(first, 100).await.expect("first prepare"));
        let message = rejection_message(
            &h.blobs,
            h.service
                .prepare(second.clone(), 101)
                .await
                .expect("second prepare"),
        )
        .await;
        assert!(message.contains("maxConcurrent"), "{message}");

        // Close the first child in the store (the fake runtime does not).
        let closed = harness::CoreAgentCodec
            .encode_uncommitted(&harness::UncommittedCoreAgentEvent {
                observed_at_ms: 150,
                joins: harness::CoreAgentJoins::default(),
                event: harness::CoreAgentEvent::Lifecycle(harness::CoreAgentLifecycleEvent::Closed),
            })
            .expect("encode close");
        h.sessions
            .append(AppendSessionEvents {
                session_id: SessionId::new(&child.session_id),
                expected_head: None,
                events: vec![closed],
            })
            .await
            .expect("close child");
        assert_eq!(
            h.sessions
                .load_session(&SessionId::new(&child.session_id))
                .await
                .expect("load")
                .expect("child")
                .lifecycle_status,
            SessionLifecycleStatus::Closed
        );
        assert!(matches!(
            h.service.prepare(second, 102).await.expect("third prepare"),
            SubagentPrepareActivityResult::Prepared { .. }
        ));
    }

    fn delegated(contract: &str) -> DelegatedContract {
        DelegatedContract {
            contract: contract.to_owned(),
            digest: format!("sha256:{contract}"),
            tool_id: contract.to_owned(),
            tool_name: contract.replace('.', "_"),
        }
    }

    /// `start` with its call arguments replaced by `arguments`.
    async fn with_arguments(
        blobs: &InMemoryBlobStore,
        mut start: WorkflowToolStartArgs,
        arguments: serde_json::Value,
    ) -> WorkflowToolStartArgs {
        start.invocation.arguments_ref = blobs
            .put_bytes(serde_json::to_vec(&arguments).unwrap())
            .await
            .expect("put arguments");
        start
    }

    async fn child_origin(h: &Harness, child: &SubagentChildRef) -> SessionOrigin {
        h.sessions
            .load_session(&SessionId::new(&child.session_id))
            .await
            .expect("load")
            .expect("child")
            .origin
            .expect("origin")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_program_narrows_its_child_to_the_contracts_it_names_in_env() {
        let runtime = FakeChildRuntime::with_profile("reviewer", 1);
        *runtime.parent_contracts.lock().unwrap() =
            Some(vec![delegated("time.sleep"), delegated("web.fetch")]);
        let h = harness(runtime).await;
        let start = start_args(
            &h.blobs,
            "wte:a",
            "parent",
            "reviewer",
            "reviewer",
            SubagentLimits::default(),
        )
        .await;
        let start = with_arguments(
            &h.blobs,
            start,
            serde_json::json!({ "agent": "reviewer", "input": "x", "env": ["web.fetch"] }),
        )
        .await;
        let (child, _) = prepared(h.service.prepare(start, 10).await.expect("prepare"));
        assert_eq!(
            child_origin(&h, &child).await.env,
            Some(vec![delegated("web.fetch")]),
            "the child holds what was named, as the caller held it"
        );

        // A contract the program does not hold is refused before any child
        // exists.
        let start = start_args(
            &h.blobs,
            "wte:b",
            "parent",
            "reviewer",
            "reviewer",
            SubagentLimits::default(),
        )
        .await;
        let start = with_arguments(
            &h.blobs,
            start,
            serde_json::json!({ "agent": "reviewer", "input": "x", "env": ["vfs.read_file"] }),
        )
        .await;
        let refused = rejection(
            &h.blobs,
            h.service.prepare(start, 11).await.expect("prepare"),
        )
        .await;
        assert_eq!(refused["reason"], "outside_env", "{refused}");
        assert!(
            h.sessions
                .load_session(&child_session_id("wte:b"))
                .await
                .expect("load")
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn only_a_program_narrows_and_a_narrowed_parent_hands_its_list_on() {
        // A model drives the parent: it holds no contracts to hand on.
        let runtime = FakeChildRuntime::with_profile("reviewer", 1);
        let h = harness(runtime).await;
        let start = start_args(
            &h.blobs,
            "wte:a",
            "parent",
            "reviewer",
            "reviewer",
            SubagentLimits::default(),
        )
        .await;
        let start = with_arguments(
            &h.blobs,
            start,
            serde_json::json!({ "agent": "reviewer", "input": "x", "env": ["web.fetch"] }),
        )
        .await;
        let message = rejection_message(
            &h.blobs,
            h.service.prepare(start, 10).await.expect("prepare"),
        )
        .await;
        assert!(message.contains("only a program"), "{message}");

        // A child of a narrowed session gets the same list.
        h.sessions
            .create_session(CreateSession {
                metadata: Default::default(),
                session_id: SessionId::new("narrowed"),
                display_name: None,
                origin: Some(SessionOrigin {
                    kind: SessionOriginKind::Subagent,
                    parent_session_id: SessionId::new("parent"),
                    parent_run_id: 1,
                    root_session_id: SessionId::new("parent"),
                    depth: 1,
                    root_run_id: Some(1),
                    invocation_id: format!("wti:sha256:{}", "d".repeat(64)),
                    profile_id: "reviewer".to_owned(),
                    profile_revision: 1,
                    limits: SubagentLimits::default(),
                    env: Some(vec![delegated("web.fetch")]),
                }),
                delete_after_close_ms: None,
                created_at_ms: 2,
            })
            .await
            .expect("create narrowed");
        let start = start_args(
            &h.blobs,
            "wte:c",
            "narrowed",
            "reviewer",
            "reviewer",
            SubagentLimits::default(),
        )
        .await;
        let (child, _) = prepared(h.service.prepare(start, 12).await.expect("prepare"));
        assert_eq!(
            child_origin(&h, &child).await.env,
            Some(vec![delegated("web.fetch")])
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_program_child_must_find_every_contract_it_requires_in_env() {
        let runtime =
            FakeChildRuntime::with_program("checker", 1, serde_json::json!({ "timers": {} }));
        runtime
            .profiles
            .lock()
            .unwrap()
            .get_mut("checker")
            .and_then(|profile| profile.document.program.as_mut())
            .expect("program")
            .requires = vec![api::ProgramContractRequirement {
            contract: "time.sleep".to_owned(),
            digest: None,
        }];
        *runtime.parent_config.lock().unwrap() = Some(api::SessionConfig {
            features: Some(serde_json::from_value(serde_json::json!({ "timers": {} })).unwrap()),
            ..Default::default()
        });
        *runtime.parent_contracts.lock().unwrap() =
            Some(vec![delegated("time.sleep"), delegated("web.fetch")]);
        let h = harness(runtime).await;
        let call = |env: &[&str]| serde_json::json!({ "program": "checker", "input": { "claim": "x" }, "env": env });

        let start = program_start_args(&h.blobs, "wte:a", "checker").await;
        let start = with_arguments(&h.blobs, start, call(&["web.fetch"])).await;
        let refused = rejection(
            &h.blobs,
            h.service.prepare(start, 10).await.expect("prepare"),
        )
        .await;
        assert_eq!(refused["reason"], "outside_env", "{refused}");
        assert!(
            refused["error"]
                .as_str()
                .is_some_and(|error| error.contains("requires time.sleep")),
            "{refused}"
        );

        let start = program_start_args(&h.blobs, "wte:b", "checker").await;
        let start = with_arguments(&h.blobs, start, call(&["time.sleep"])).await;
        let (child, _) = prepared(h.service.prepare(start, 11).await.expect("prepare"));
        assert_eq!(
            child_origin(&h, &child).await.env,
            Some(vec![delegated("time.sleep")])
        );
    }
}
