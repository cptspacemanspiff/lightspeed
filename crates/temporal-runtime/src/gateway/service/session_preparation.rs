//! Shared materialization service used by workflow activities and API preflight.
use super::*;
use ::profiles::ProfileStore;
use temporal_workflow::{SessionToolsetPreparation, SessionToolsetSource};

#[derive(Clone)]
pub(crate) struct SessionPreparationService {
    pub(crate) store: Arc<PgStore>,
    pub(crate) task_queue: String,
    pub(crate) code_task_queue: String,
    pub(crate) program_task_queue: String,
    pub(crate) input_timeouts: tools::human_input::InputTimeouts,
    /// Lists MCP server tools to pin program contracts against; absent
    /// where nothing pins programs.
    pub(crate) mcp_inventory: Option<Arc<dyn llm_runtime::McpInventoryResolver>>,
}

impl SessionPreparationService {
    pub(crate) fn session_toolset_config(
        session_config: &SessionConfig,
        include_environment_tools: bool,
        include_job_read_tool: bool,
    ) -> ToolsetConfig {
        let features = &session_config.features;
        let mut config = ToolsetConfig::empty();
        config.environment_read = features.environments.is_some();
        config.environment_selection = features
            .environments
            .as_ref()
            .is_some_and(|environments| environments.selection);
        // Tool surfaces are the union of attachment grants: any attachment
        // installs the read tools, any editing attachment the write tools.
        config.builtin = match features.vfs.as_ref().and_then(|vfs| vfs.tool_access()) {
            None => tools::toolset::BuiltinToolsetConfig::disabled(),
            Some(harness::WorkspaceAccess::Read) => tools::toolset::BuiltinToolsetConfig {
                vfs: tools::toolset::FilesystemToolsetConfig::read_only(),
                ..tools::toolset::BuiltinToolsetConfig::disabled()
            },
            Some(harness::WorkspaceAccess::Edit) => {
                tools::toolset::BuiltinToolsetConfig::workspace()
            }
        };
        if let Some(web) = features.web.as_ref() {
            if let Some(search) = &web.search {
                config.web.search = Some(WebSearchToolConfig::new(
                    search.allowed_domains.clone().unwrap_or_default(),
                    search.blocked_domains.clone(),
                ));
            }
            if web.fetch.is_some() {
                config.web.fetch = true;
            }
        }
        if features.timers.is_some() || features.subagents.is_some() {
            // Joining spawned sub-agents depends on the base concurrency
            // tools, so the subagents grant implies them; the timers grant
            // adds nothing extra today beyond the same surface.
            config.concurrency = tools::concurrency::ConcurrencyToolsetConfig::timer();
        }
        if include_environment_tools && let Some(environment) = &features.environments {
            let access = environment.tool_access();
            config.builtin.environment.filesystem = match access {
                None => tools::toolset::FilesystemToolsetConfig::disabled(),
                Some(access) if access.allows_edit() => {
                    tools::toolset::FilesystemToolsetConfig::workspace_edit()
                }
                Some(_) => tools::toolset::FilesystemToolsetConfig::read_only(),
            };
            let exec = access.is_some_and(|access| access.allows_exec());
            config.builtin.environment.run_process = exec;
            config.builtin.environment.continue_process = exec;
        }
        if include_job_read_tool {
            config.builtin.environment.job_read = true;
        }
        config
    }

    /// A program session's toolset: built-ins on the canonical presentation
    /// and no provider-hosted tools, since no model presents them. Program
    /// contracts bind to exactly these tools.
    pub(crate) fn program_toolset_config(
        config: &mut ToolsetConfig,
        session_config: &SessionConfig,
    ) {
        config.builtin.presentation = tools::toolset::BuiltinToolPresentation::Canonical;
        config.web.search = None;
        config.concurrency.durable_sleep = config.concurrency.timer;
        // `input.open` returns a promise, which the program awaits or
        // cancels, and its handle reminds and reassigns.
        config.concurrency.enabled = true;
        config.input_controls = true;
        // A child the program spawns and detaches can be taken back by a
        // later run.
        config.child_handles = session_config
            .features
            .subagents
            .as_ref()
            .map(tools::subagents::SubagentToolKind::granted)
            .unwrap_or_default()
            .into_iter()
            .filter(|kind| kind.handle_tool_id().is_some())
            .collect();
    }

    /// The tool contracts a program session with this configuration offers.
    /// MCP contracts are listed from the servers only `with_mcp`, since
    /// that reaches every attached server.
    pub(crate) async fn program_contracts(
        &self,
        session_config: &SessionConfig,
        with_mcp: bool,
    ) -> Result<Vec<tools::program::contracts::BoundContract>, AgentApiError> {
        let mut config = Self::session_toolset_config(
            session_config,
            session_config.features.environments.is_some(),
            false,
        );
        Self::program_toolset_config(&mut config, session_config);
        let toolset = register_toolset(&config)
            .map_err(|e| AgentApiError::internal(format!("build session tools: {e}")))?;
        let mut tools = toolset.tools.into_values().collect::<Vec<_>>();
        // Every program session binds the input workflow tools, and one with
        // the sub-agents grant the child-session tools it lists children for.
        tools.push(tools::human_input::input_request_tool_spec());
        tools.push(tools::human_input::input_open_tool_spec());
        if let Some(subagents) = &session_config.features.subagents {
            tools.extend(
                tools::subagents::SubagentToolKind::granted(subagents)
                    .into_iter()
                    .map(|kind| kind.tool_spec(tools::subagents::ChildCaller::Program)),
            );
        }
        let mut contracts = tools::program::contracts::bind_toolset(self.store.as_ref(), &tools)
            .await
            .map_err(|e| AgentApiError::internal(format!("bind program contracts: {e}")))?;
        if with_mcp {
            let inventory = self.mcp_inventory.as_ref().ok_or_else(|| {
                AgentApiError::internal("MCP inventory is not configured for program contracts")
            })?;
            let mcp_tools = self
                .desired_mcp_tools(&session_config.features)
                .await?
                .into_values()
                .collect::<Vec<_>>();
            let (servers, failures) =
                crate::worker::mcp::program_mcp_tools(inventory.as_ref(), &mcp_tools).await;
            if let Some((server, error)) = failures.into_iter().next() {
                return Err(AgentApiError::rejected(format!(
                    "list MCP server {server} tools for program contracts: {error}"
                )));
            }
            let (mcp, unbindable) = tools::program::contracts::bind_mcp(&servers);
            if !unbindable.is_empty() {
                tracing::warn!(?unbindable, "MCP tools with no program contract name");
            }
            contracts.extend(mcp);
        }
        Ok(contracts)
    }

    pub(crate) async fn core_environment_job_workflow_tool_declarations(
        &self,
    ) -> Result<Vec<WorkflowToolDeclaration>, AgentApiError> {
        let recipe_bytes = serde_json::to_vec(&temporal_workflow::WorkflowToolRecipeV1 {
            workflow_type: "EnvironmentJobWorkflow".to_owned(),
            task_queue: self.task_queue.clone(),
        })
        .map_err(|error| {
            AgentApiError::internal(format!(
                "encode core environment-job workflow recipe: {error}"
            ))
        })?;
        let recipe_fingerprint = temporal_workflow::workflow_tool_recipe_fingerprint(&recipe_bytes);
        let recipe_ref = self
            .store
            .put_bytes(recipe_bytes)
            .await
            .map_err(map_blob_store_error)?;

        let reply_schema_ref = self
            .store
            .put_bytes(
                serde_json::to_vec(&tools::definitions::output_schema_for::<
                    tools::environment::jobs::ModelJobResult,
                >())
                .map_err(|error| AgentApiError::internal(error.to_string()))?,
            )
            .await
            .map_err(map_blob_store_error)?;
        let definitions = [
            (
                BuiltinToolOperation::JobSubmit,
                JOB_SUBMIT_WORKFLOW_TOOL_ID,
                JOB_SUBMIT_WORKFLOW_SEMANTIC_TYPE,
                WorkflowToolCompletion::Promises {
                    reply_schema_ref: Some(reply_schema_ref.clone()),
                    deadline_after_ms: None,
                    max_promises: harness::MAX_COMPLETION_PROMISES,
                    key_source: WorkflowToolCompletionKeySource::ArrayItemField {
                        pointer: "/jobs".to_owned(),
                        field: "job_id".to_owned(),
                    },
                },
            ),
            (
                BuiltinToolOperation::JobRun,
                JOB_RUN_WORKFLOW_TOOL_ID,
                JOB_RUN_WORKFLOW_SEMANTIC_TYPE,
                WorkflowToolCompletion::Joined {
                    reply_schema_ref: Some(reply_schema_ref.clone()),
                    deadline_after_ms: JOB_RUN_DEADLINE_AFTER_MS,
                },
            ),
        ];
        let mut declarations = Vec::with_capacity(definitions.len());
        for (operation, tool_id, semantic_type, completion) in definitions {
            let builtin = BuiltinTool::environment_canonical(operation);
            let tool = tools::definitions::register(
                builtin.logical_id(),
                tools::definitions::BuiltinSettings {
                    presentation: tools::toolset::BuiltinToolPresentation::Canonical,
                    unscoped_paths: true,
                    ..Default::default()
                },
                builtin.parallelism(),
                builtin.execution_spec(),
            );
            declarations.push(WorkflowToolDeclaration::new(
                WorkflowToolDefinition {
                    tool_id: WorkflowToolId::new(tool_id),
                    revision: 1,
                    semantic_type: semantic_type.to_owned(),
                    tool,
                },
                WorkflowToolTarget::Start {
                    start: WorkflowStartRef {
                        recipe_format: temporal_workflow::WORKFLOW_TOOL_RECIPE_FORMAT_V1,
                        revision: 1,
                        recipe_ref: recipe_ref.clone(),
                        recipe_fingerprint: recipe_fingerprint.clone(),
                    },
                },
                completion,
            ));
        }
        Ok(declarations)
    }

    /// The child-session bindings of `kinds`, as `caller` calls them. Agent
    /// children reply with their result envelope; a program child's reply
    /// is its output, which its own output schema has already checked.
    pub(crate) async fn core_subagent_workflow_tool_declarations(
        &self,
        kinds: &[tools::subagents::SubagentToolKind],
        caller: tools::subagents::ChildCaller,
    ) -> Result<Vec<WorkflowToolDeclaration>, AgentApiError> {
        let recipe_bytes = serde_json::to_vec(&temporal_workflow::WorkflowToolRecipeV1 {
            workflow_type: tools::subagents::SUBAGENT_WORKFLOW_TYPE.to_owned(),
            task_queue: self.task_queue.clone(),
        })
        .map_err(|error| {
            AgentApiError::internal(format!("encode core subagent workflow recipe: {error}"))
        })?;
        let recipe_fingerprint = temporal_workflow::workflow_tool_recipe_fingerprint(&recipe_bytes);
        let recipe_ref = self
            .store
            .put_bytes(recipe_bytes)
            .await
            .map_err(map_blob_store_error)?;
        let reply_schema_ref = self
            .store
            .put_bytes(
                serde_json::to_vec(&tools::definitions::output_schema_for::<
                    tools::subagents::SubagentResultEnvelope,
                >())
                .map_err(|error| AgentApiError::internal(error.to_string()))?,
            )
            .await
            .map_err(map_blob_store_error)?;
        // The binding carries the hard ceiling; the grant's `deadlineMs` is
        // pinned per call and enforced inside the execution, so the
        // immutable binding never has to change with the grant.
        let mut declarations = Vec::with_capacity(kinds.len());
        for &kind in kinds {
            let completion = kind.completion(
                (!kind.is_program()).then(|| reply_schema_ref.clone()),
                harness::SUBAGENT_DEADLINE_CEILING_MS,
            );
            let tool = kind.tool_spec(caller);
            declarations.push(WorkflowToolDeclaration::new(
                WorkflowToolDefinition {
                    tool_id: WorkflowToolId::new(kind.workflow_tool_id()),
                    revision: 1,
                    semantic_type: kind.semantic_type().to_owned(),
                    tool,
                },
                WorkflowToolTarget::Start {
                    start: WorkflowStartRef {
                        recipe_format: temporal_workflow::WORKFLOW_TOOL_RECIPE_FORMAT_V1,
                        revision: 1,
                        recipe_ref: recipe_ref.clone(),
                        recipe_fingerprint: recipe_fingerprint.clone(),
                    },
                },
                completion,
            ));
        }
        Ok(declarations)
    }

    pub(crate) async fn core_code_workflow_tool_declaration(
        &self,
    ) -> Result<WorkflowToolDeclaration, AgentApiError> {
        let recipe_bytes = serde_json::to_vec(&temporal_workflow::WorkflowToolRecipeV1 {
            workflow_type: tools::code::CODE_EXECUTION_WORKFLOW_TYPE.to_owned(),
            task_queue: self.code_task_queue.clone(),
        })
        .map_err(|error| {
            AgentApiError::internal(format!("encode code workflow recipe: {error}"))
        })?;
        let recipe_fingerprint = temporal_workflow::workflow_tool_recipe_fingerprint(&recipe_bytes);
        let recipe_ref = self
            .store
            .put_bytes(recipe_bytes)
            .await
            .map_err(map_blob_store_error)?;
        let output_schema = serde_json::to_vec(&tools::code::code_execution_output_schema())
            .map_err(|error| {
                AgentApiError::internal(format!("encode code output schema: {error}"))
            })?;
        let reply_schema_ref = self
            .store
            .put_bytes(output_schema)
            .await
            .map_err(map_blob_store_error)?;
        Ok(WorkflowToolDeclaration::new(
            WorkflowToolDefinition {
                tool_id: WorkflowToolId::new(tools::code::CODE_EXECUTE_WORKFLOW_TOOL_ID),
                revision: 1,
                semantic_type: tools::code::CODE_EXECUTE_WORKFLOW_SEMANTIC_TYPE.to_owned(),
                tool: tools::definitions::register(
                    "code.execute",
                    Default::default(),
                    harness::ToolParallelism::ParallelSafe,
                    Default::default(),
                ),
            },
            WorkflowToolTarget::Start {
                start: WorkflowStartRef {
                    recipe_format: temporal_workflow::WORKFLOW_TOOL_RECIPE_FORMAT_V1,
                    revision: 1,
                    recipe_ref,
                    recipe_fingerprint,
                },
            },
            WorkflowToolCompletion::Joined {
                reply_schema_ref: Some(reply_schema_ref),
                deadline_after_ms: tools::code::CODE_EXECUTION_DEADLINE_CEILING_MS,
            },
        ))
    }

    /// The system binding that executes a program session's program: a
    /// joined start of the program run workflow on the program queue. The run
    /// owns its single invocation; the program itself can never call it.
    pub(crate) async fn core_program_execute_workflow_tool_declaration(
        &self,
    ) -> Result<WorkflowToolDeclaration, AgentApiError> {
        let recipe_bytes = serde_json::to_vec(&temporal_workflow::WorkflowToolRecipeV1 {
            workflow_type: tools::program::PROGRAM_RUN_WORKFLOW_TYPE.to_owned(),
            task_queue: self.program_task_queue.clone(),
        })
        .map_err(|error| {
            AgentApiError::internal(format!("encode program workflow recipe: {error}"))
        })?;
        let recipe_fingerprint = temporal_workflow::workflow_tool_recipe_fingerprint(&recipe_bytes);
        let recipe_ref = self
            .store
            .put_bytes(recipe_bytes)
            .await
            .map_err(map_blob_store_error)?;
        Ok(WorkflowToolDeclaration::new(
            WorkflowToolDefinition {
                tool_id: WorkflowToolId::new(tools::program::PROGRAM_EXECUTE_WORKFLOW_TOOL_ID),
                revision: 1,
                semantic_type: tools::program::PROGRAM_EXECUTE_WORKFLOW_SEMANTIC_TYPE.to_owned(),
                tool: tools::definitions::register(
                    tools::program::PROGRAM_EXECUTE_TOOL_NAME,
                    Default::default(),
                    harness::ToolParallelism::ParallelSafe,
                    Default::default(),
                ),
            },
            WorkflowToolTarget::Start {
                start: WorkflowStartRef {
                    recipe_format: temporal_workflow::WORKFLOW_TOOL_RECIPE_FORMAT_V1,
                    revision: 1,
                    recipe_ref,
                    recipe_fingerprint,
                },
            },
            WorkflowToolCompletion::Joined {
                reply_schema_ref: None,
                deadline_after_ms: tools::program::PROGRAM_EXECUTION_DEADLINE_CEILING_MS,
            },
        ))
    }

    /// The system bindings behind a program's `input.request` and
    /// `input.open`: starts of the input-request workflow, which records the
    /// request in the universe's inbox and replies with the answer. A
    /// request joins the reply; an open request returns its promise. The
    /// approval binding starts the same workflow for a call the program's
    /// scope parked for approval, and replies with the approved call's
    /// result, so its reply has no fixed schema and its ceiling covers the
    /// call too.
    pub(crate) async fn core_input_workflow_tool_declarations(
        &self,
    ) -> Result<Vec<WorkflowToolDeclaration>, AgentApiError> {
        let recipe_bytes = serde_json::to_vec(&temporal_workflow::WorkflowToolRecipeV1 {
            workflow_type: tools::human_input::INPUT_REQUEST_WORKFLOW_TYPE.to_owned(),
            task_queue: self.task_queue.clone(),
        })
        .map_err(|error| {
            AgentApiError::internal(format!("encode input-request workflow recipe: {error}"))
        })?;
        let recipe_fingerprint = temporal_workflow::workflow_tool_recipe_fingerprint(&recipe_bytes);
        let recipe_ref = self
            .store
            .put_bytes(recipe_bytes)
            .await
            .map_err(map_blob_store_error)?;
        let reply_schema_ref = self
            .store
            .put_bytes(
                serde_json::to_vec(&tools::definitions::output_schema_for::<
                    tools::human_input::InputAnswer,
                >())
                .map_err(|error| AgentApiError::internal(error.to_string()))?,
            )
            .await
            .map_err(map_blob_store_error)?;
        let deadline_after_ms = self.input_timeouts.deadline_ceiling_ms();
        let definitions = [
            (
                tools::human_input::INPUT_REQUEST_WORKFLOW_TOOL_ID,
                tools::human_input::INPUT_REQUEST_WORKFLOW_SEMANTIC_TYPE,
                tools::human_input::input_request_tool_spec(),
                WorkflowToolCompletion::Joined {
                    reply_schema_ref: Some(reply_schema_ref.clone()),
                    deadline_after_ms,
                },
            ),
            (
                tools::human_input::INPUT_OPEN_WORKFLOW_TOOL_ID,
                tools::human_input::INPUT_OPEN_WORKFLOW_SEMANTIC_TYPE,
                tools::human_input::input_open_tool_spec(),
                tools::human_input::input_open_completion(
                    Some(reply_schema_ref.clone()),
                    deadline_after_ms,
                ),
            ),
            (
                tools::human_input::APPROVAL_WORKFLOW_TOOL_ID,
                tools::human_input::APPROVAL_WORKFLOW_SEMANTIC_TYPE,
                tools::human_input::approval_tool_spec(),
                WorkflowToolCompletion::Joined {
                    reply_schema_ref: None,
                    deadline_after_ms: deadline_after_ms.saturating_add(
                        temporal_workflow::tool_call_operation_timeout(
                            harness::ToolExecutionClass::RemoteInteractive,
                        )
                        .as_millis() as u64
                            * 5,
                    ),
                },
            ),
        ];
        Ok(definitions
            .into_iter()
            .map(|(tool_id, semantic_type, tool, completion)| {
                WorkflowToolDeclaration::new(
                    WorkflowToolDefinition {
                        tool_id: WorkflowToolId::new(tool_id),
                        revision: 1,
                        semantic_type: semantic_type.to_owned(),
                        tool,
                    },
                    WorkflowToolTarget::Start {
                        start: WorkflowStartRef {
                            recipe_format: temporal_workflow::WORKFLOW_TOOL_RECIPE_FORMAT_V1,
                            revision: 1,
                            recipe_ref: recipe_ref.clone(),
                            recipe_fingerprint: recipe_fingerprint.clone(),
                        },
                    },
                    completion,
                )
            })
            .collect())
    }

    /// The tools `source` materializes for `session_id`. An agent session a
    /// program delegated to with `env` keeps only what that list names.
    pub(crate) async fn prepare_toolset(
        &self,
        source: SessionToolsetSource,
        session_id: Option<&SessionId>,
    ) -> Result<SessionToolsetPreparation, AgentApiError> {
        let session_config = &source.config;
        let jobs = session_config
            .features
            .environments
            .as_ref()
            .and_then(|environments| environments.tool_access())
            .is_some_and(|access| access.allows_jobs());
        let subagents = session_config
            .features
            .subagents
            .as_ref()
            .map(tools::subagents::SubagentToolKind::granted)
            .unwrap_or_default();
        let code_mode = session_config.features.code_mode.is_some();
        let caller = if source.driver.is_program() {
            tools::subagents::ChildCaller::Program
        } else {
            tools::subagents::ChildCaller::Model
        };
        let mut declarations = Vec::new();
        if source.driver.is_program() {
            declarations.push(
                self.core_program_execute_workflow_tool_declaration()
                    .await?,
            );
            declarations.extend(self.core_input_workflow_tool_declarations().await?);
        }
        if jobs {
            declarations.extend(
                self.core_environment_job_workflow_tool_declarations()
                    .await?,
            );
        }
        if !subagents.is_empty() {
            declarations.extend(
                self.core_subagent_workflow_tool_declarations(&subagents, caller)
                    .await?,
            );
        }
        if code_mode {
            declarations.push(self.core_code_workflow_tool_declaration().await?);
        }
        for declaration in &declarations {
            let id = &declaration.definition.tool_id;
            if let Some(existing) = source.bindings.get(id)
                && (!source.system_binding_ids.contains(id)
                    || existing.session_universe_id != self.store.config().universe_id
                    || existing.definition.semantic_type != declaration.definition.semantic_type
                    || existing.definition.tool.name != declaration.definition.tool.name)
            {
                return Err(AgentApiError::invalid_request(format!(
                    "system workflow tool {id} conflicts with an existing immutable binding"
                )));
            }
        }
        let mut validation_state = harness::CoreAgentState::new();
        validation_state.workflow_tools.bindings = source.bindings.clone();
        validate_subagent_deadline_for_existing_bindings(
            &validation_state,
            &session_config.features,
        )?;
        validate_code_deadline_for_existing_bindings(&validation_state, &session_config.features)?;
        declarations.retain(|declaration| {
            !source
                .bindings
                .contains_key(&declaration.definition.tool_id)
        });
        let mut bindings = source.bindings.clone();
        for declaration in &declarations {
            let binding = harness::WorkflowToolBinding::admit(
                self.store.config().universe_id,
                declaration.definition.clone(),
                declaration.target.clone(),
                declaration.completion.clone(),
            )
            .map_err(|error| AgentApiError::invalid_request(error.to_string()))?;
            bindings.insert(binding.definition.tool_id.clone(), binding);
        }
        let materialized = bindings
            .values()
            .filter(|binding| {
                (jobs || !is_core_environment_job_binding(binding))
                    && core_subagent_kind(binding).is_none_or(|kind| subagents.contains(&kind))
                    && (code_mode
                        || binding.definition.tool_id.as_str()
                            != tools::code::CODE_EXECUTE_WORKFLOW_TOOL_ID)
                    && (source.driver.is_program() || !is_core_input_binding(binding))
            })
            .collect::<Vec<_>>();
        let mut config = Self::session_toolset_config(
            session_config,
            session_config.features.environments.is_some(),
            jobs,
        );
        if source.driver.is_program() {
            Self::program_toolset_config(&mut config, session_config);
        }
        enable_concurrency_for_workflow_tools(&mut config, materialized.iter().copied());
        let mut toolset = register_toolset(&config)
            .map_err(|e| AgentApiError::internal(format!("build session tools: {e}")))?;
        register_workflow_tools(&mut toolset, materialized.iter().copied()).map_err(|e| {
            AgentApiError::invalid_request(format!("materialize workflow tools: {e}"))
        })?;
        let desired_mcp = self.desired_mcp_tools(&session_config.features).await?;
        if let Some(name) = toolset
            .tools
            .keys()
            .find(|name| desired_mcp.contains_key(*name))
        {
            return Err(AgentApiError::invalid_request(format!(
                "tool name {name} collides with a remote MCP tool"
            )));
        }
        let mut tools = toolset.tools;
        tools.extend(desired_mcp);
        // A program child can call only the contracts it requires, which
        // its preparation already checked against the list.
        if !source.driver.is_program()
            && let Some(session_id) = session_id
            && let Some(env) = self.delegated_env(session_id).await?
        {
            narrow_to_env(&mut tools, &env);
        }
        Ok(SessionToolsetPreparation {
            source,
            declarations,
            tools,
        })
    }

    /// The capabilities a delegating program narrowed this session to, from
    /// its origin. A session not yet created has none.
    async fn delegated_env(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<Vec<harness::storage::DelegatedContract>>, AgentApiError> {
        let record = self
            .store
            .load_session(session_id)
            .await
            .map_err(api_projection::map_session_store_error)?;
        Ok(record
            .and_then(|record| record.origin)
            .and_then(|origin| origin.env))
    }
}

/// Keep the tools `env` names: a tool it names by id, and of an MCP server
/// only the server tools it names. Everything else goes, including tools no
/// contract serves.
fn narrow_to_env(
    tools: &mut BTreeMap<ToolName, harness::ToolSpec>,
    env: &[harness::storage::DelegatedContract],
) {
    tools.retain(|name, spec| {
        let granted = env
            .iter()
            .filter(|contract| contract.tool_id == name.as_str());
        let harness::ToolKind::RemoteMcp(mcp) = &mut spec.kind else {
            return granted.count() > 0;
        };
        let prefix = format!("{name}__");
        let remote = granted
            .filter_map(|contract| contract.tool_name.strip_prefix(&prefix))
            .filter(|remote| {
                mcp.allowed_tools
                    .as_ref()
                    .is_none_or(|allowed| allowed.iter().any(|tool| tool == remote))
            })
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if remote.is_empty() {
            return false;
        }
        mcp.allowed_tools = Some(remote);
        true
    });
}

impl SessionPreparationService {
    pub(crate) async fn validate_workspace_attachment_targets(
        &self,
        features: &harness::FeaturesConfig,
    ) -> Result<(), AgentApiError> {
        let Some(vfs) = features.vfs.as_ref() else {
            return Ok(());
        };
        if vfs.workspaces.is_empty() {
            return Ok(());
        }
        let blobs: Arc<dyn BlobStore> = self.store.clone();
        let workspace_store: Arc<dyn VfsWorkspaceStore> = self.store.clone();
        let resolved = vfs::resolve_workspace_attachments(blobs, workspace_store, &vfs.workspaces)
            .await
            .map_err(map_vfs_catalog_error)?;
        if let Some(link) = resolved.iter().find(|link| !link.is_available()) {
            return Err(AgentApiError::invalid_request(format!(
                "workspace attachment target at {} is unavailable: {}",
                link.path,
                link.unavailable_reason().unwrap_or("unknown reason")
            )));
        }
        Ok(())
    }

    pub(crate) async fn validate_configuration(
        &self,
        config: &SessionConfig,
    ) -> Result<(), AgentApiError> {
        config
            .validate()
            .map_err(|e| AgentApiError::invalid_request(e.to_string()))?;
        // Everything attached must exist before anything resolves it.
        authorization::require_resources(
            &store_pg::PgAccessStore::new(self.store.pool().clone()),
            self.store.config().universe_id,
            &temporal_workflow::attached_resources(&config.features),
        )
        .await?;
        self.validate_workspace_attachment_targets(&config.features)
            .await?;
        if let Some(subagents) = &config.features.subagents {
            for (kind, profile_id) in api_config::subagent_profiles(subagents) {
                let id = api::ProfileId::try_new(profile_id.to_owned())
                    .map_err(|e| AgentApiError::invalid_request(e.to_string()))?;
                let profile = self
                    .store
                    .read_agent_profile(&id)
                    .await
                    .map_err(profiles::map_profile_error)?;
                kind.check(&profile)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn prepare_profile(
        &self,
        request: temporal_workflow::SessionProfilePreparationRequest,
    ) -> Result<temporal_workflow::SessionProfilePreparation, AgentApiError> {
        self.validate_configuration(&request.source.config).await?;
        let mut instructions = BTreeMap::new();
        // A program session has no model context; its profile's
        // instructions are not published.
        if let Some(input) = request
            .instructions
            .filter(|_| !request.source.driver.is_program())
        {
            let reference = match input {
                ProfileInstructions::Text { text } => self
                    .store
                    .put_bytes(text.into_bytes())
                    .await
                    .map_err(map_blob_store_error)?,
                ProfileInstructions::TextRef { blob_ref } => {
                    let reference = parse_blob_ref(&blob_ref)?;
                    if !self
                        .store
                        .has_blob(&reference)
                        .await
                        .map_err(map_blob_store_error)?
                    {
                        return Err(AgentApiError::not_found(format!(
                            "profile instructions blob not found: {reference}"
                        )));
                    }
                    reference
                }
            };
            instructions.insert(
                ContextEntryKey::new("instructions.050.profile"),
                ContextEntryInput {
                    kind: ContextEntryKind::Instructions,
                    content: harness::ContentRef::text(reference),
                    preview: Some("Profile instructions".to_owned()),
                    origin: None,
                    provenance_ref: None,
                    token_estimate: None,
                },
            );
        }
        // The fill candidate must be an attachment of the configuration being
        // applied and selectable in the registry; whether it is applied is
        // decided by the workflow against the live pointer.
        let environment_id = match request.environment {
            None => None,
            Some(environment_id) => {
                let attached = request
                    .source
                    .config
                    .features
                    .environments
                    .as_ref()
                    .is_some_and(|environments| environments.is_attached(environment_id.as_str()));
                if !attached {
                    return Err(AgentApiError::rejected(format!(
                        "environment {environment_id} is not attached in the session configuration"
                    )));
                }
                crate::environments::resolver::EnvironmentResolver::from_pg_store(
                    self.store.clone(),
                )
                .selectable(&environment_id)
                .await
                .map_err(super::environments::map_environment_resolve_error)?;
                Some(environment_id)
            }
        };
        let toolset = self
            .prepare_toolset(request.source, Some(&request.session_id))
            .await?;
        Ok(temporal_workflow::SessionProfilePreparation {
            toolset,
            instructions,
            environment_id,
        })
    }
}

impl GatewayAgentApi {
    pub(super) fn preparation_service(&self) -> SessionPreparationService {
        SessionPreparationService {
            store: self.store.clone(),
            task_queue: self.task_queue.clone(),
            code_task_queue: self.code_task_queue.clone(),
            program_task_queue: self.program_task_queue.clone(),
            input_timeouts: self.input_timeouts,
            mcp_inventory: Some(self.mcp_inventory.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delegated(
        contract: &str,
        tool_id: &str,
        tool_name: &str,
    ) -> harness::storage::DelegatedContract {
        harness::storage::DelegatedContract {
            contract: contract.to_owned(),
            digest: format!("sha256:{contract}"),
            tool_id: tool_id.to_owned(),
            tool_name: tool_name.to_owned(),
        }
    }

    fn mcp_server(name: &str, allowed: Option<&[&str]>) -> harness::ToolSpec {
        harness::ToolSpec {
            name: ToolName::new(name),
            kind: harness::ToolKind::RemoteMcp(harness::RemoteMcpToolSpec {
                server_id: "github".to_owned(),
                record_revision: 1,
                server_label: "github".to_owned(),
                server_url: "https://mcp.example.com".to_owned(),
                description_ref: None,
                allowed_tools: allowed
                    .map(|tools| tools.iter().map(|tool| (*tool).to_owned()).collect()),
                execution: harness::RemoteMcpExecution::Native,
                exposure: harness::RemoteMcpExposure::Inject,
                approval: harness::RemoteMcpApprovalPolicy::Never,
                defer_loading: None,
                auth_ref: None,
                auth_required: false,
                allow_private_network: false,
            }),
            parallelism: harness::ToolParallelism::ParallelSafe,
            execution: harness::ToolExecutionSpec::default(),
        }
    }

    #[test]
    fn a_narrowed_session_keeps_only_the_tools_and_server_tools_its_list_names() {
        let builtin = |id: &str| {
            tools::definitions::register(
                id,
                Default::default(),
                harness::ToolParallelism::ParallelSafe,
                Default::default(),
            )
        };
        let mut tools: BTreeMap<ToolName, harness::ToolSpec> = [
            builtin("web.fetch"),
            builtin("vfs.read_file"),
            builtin("concurrency.await"),
            mcp_server("mcp_github", None),
            mcp_server("mcp_slack", None),
        ]
        .into_iter()
        .map(|tool| (tool.name.clone(), tool))
        .collect();
        let env = [
            delegated("web.fetch", "web.fetch", "web_fetch"),
            delegated("mcp.github.read", "mcp_github", "mcp_github__read"),
        ];
        narrow_to_env(&mut tools, &env);
        assert_eq!(
            tools.keys().map(ToolName::as_str).collect::<Vec<_>>(),
            ["mcp_github", "web.fetch"]
        );
        let harness::ToolKind::RemoteMcp(github) = &tools[&ToolName::new("mcp_github")].kind else {
            panic!("MCP server");
        };
        assert_eq!(github.allowed_tools, Some(vec!["read".to_owned()]));

        // A server's own allow-list still bounds what the list names.
        let mut tools: BTreeMap<ToolName, harness::ToolSpec> =
            [mcp_server("mcp_github", Some(&["write"]))]
                .into_iter()
                .map(|tool| (tool.name.clone(), tool))
                .collect();
        narrow_to_env(&mut tools, &env);
        assert!(tools.is_empty(), "{tools:?}");
    }
}
