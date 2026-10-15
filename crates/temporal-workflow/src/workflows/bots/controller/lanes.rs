//! Detached lanes: one delivery on one session, a steer/append sidecar
//! beside a running lane, and the answer to a pushed `bot_*` invocation.
//! Each lane owns a context clone, finishes its own bookkeeping, and
//! records failures on the controller instead of failing the workflow.

use crate::workflows::WorkflowContextExt as _;
use api::{BotEventOutcome, BotRecentDeliverySnapshot, LlmUsageView};
use bots::{BotDeliveryPhase, RoutedSession, ids};
use futures::{FutureExt, pin_mut, select};
use harness::{BlobRef, EmissionEnvelope, PromiseResolution, REPLY_COMPLETION_KEY, RunStatus};

use super::super::{
    BOT_BUSY_RETRY_DELAY, BOT_EVENT_TERMINAL_TIMEOUT, BOT_EXTRA_SESSION_CAP, BotActivities,
    BotAppendContextRequest, BotCancelRunRequest, BotEnsureSessionResult, BotExecuteToolRequest,
    BotExecuteToolResult, BotPublishDirectoryRequest, BotReadRunUsageRequest,
    BotRecordOutcomesRequest, BotSendBotReceiptsRequest, BotSendDeliveryReceiptsRequest,
    BotSessionRequest, BotSessionStatus, BotStartRunRequest, BotStartRunResult, BotSteerRunRequest,
    BotSteerRunResult,
};
use super::state::{
    BotDelivery, DeliveryOutcome, LaneResolution, LaneTerminal, ManagedSession, PendingInvocation,
    WaitingRun, delivery_outcome,
};
use super::{Ctx, activity, close_session, ensure_request, now_ms, wait_until_session_idle};
use crate::AgentSessionWorkflow;

// ── Delivery lane ───────────────────────────────────────────────────────────

/// Run one delivery on its session. Any failure ends the delivery
/// `run_failed` with the failure text; the lane never fails the workflow.
pub(super) async fn run_delivery(ctx: Ctx, delivery: BotDelivery, target: String) {
    let mut target = target;
    if let Err(message) = run_delivery_inner(&ctx, &delivery, &mut target).await {
        ctx.state_mut(|state| state.record_error(message.clone()));
        finish_delivery(
            &ctx,
            &target,
            &delivery,
            DeliveryOutcome {
                outcome: BotEventOutcome::RunFailed,
                summary: Some(message),
                run_id: None,
            },
            None,
        )
        .await;
    }
}

async fn run_delivery_inner(
    ctx: &Ctx,
    delivery: &BotDelivery,
    target: &mut String,
) -> Result<(), String> {
    let Some(first_event) = delivery.events.first() else {
        ctx.state_mut(|state| {
            state.active_by_session.remove(target.as_str());
            state.lane_tick += 1;
        });
        return Ok(());
    };
    let universe_id = ctx.state(|state| state.config.universe_id);

    if let Some(session) = &delivery.session {
        match ensure_routed_session(ctx, session, target, first_event.tools_ref.clone()).await {
            None => {
                let reason = ctx
                    .state(|state| state.last_error.clone())
                    .unwrap_or_else(|| "unknown".to_owned());
                finish_delivery(
                    ctx,
                    target,
                    delivery,
                    DeliveryOutcome {
                        outcome: BotEventOutcome::RunFailed,
                        summary: Some(format!("failed to create session {target}: {reason}")),
                        run_id: None,
                    },
                    None,
                )
                .await;
                return Ok(());
            }
            Some(ensured) if ensured != *target => {
                // The routed session rotated during ensure; move the lane
                // to the successor id so terminals and busy checks find it.
                ctx.state_mut(|state| state.rekey_lane(target, &ensured));
                *target = ensured;
            }
            Some(_) => {}
        }
    }

    if ctx.state(|state| state.config.emit) {
        // An emitting bot reads the directory before it decides. A failed
        // put costs a stale directory, never the delivery.
        let bot_id = ctx.state(|state| state.config.bot_id.clone());
        if let Err(message) = activity(
            ctx,
            BotActivities::publish_directory,
            BotPublishDirectoryRequest {
                universe_id,
                bot_id,
                session_id: target.clone(),
            },
        )
        .await
        {
            ctx.state_mut(|state| state.record_error(message));
        }
    }

    // A routed session's kind is known only once it is ensured above.
    match ctx.state(|state| state.when_busy_of(delivery)) {
        api::BotWhenBusy::Append => {
            activity(
                ctx,
                BotActivities::append_context,
                BotAppendContextRequest {
                    universe_id,
                    session_id: target.clone(),
                    delivery_id: delivery.id.clone(),
                    events: delivery.events.clone(),
                },
            )
            .await?;
            finish_delivery(
                ctx,
                target,
                delivery,
                DeliveryOutcome {
                    outcome: BotEventOutcome::Appended,
                    summary: None,
                    run_id: None,
                },
                None,
            )
            .await;
            return Ok(());
        }
        api::BotWhenBusy::Steer => {
            // A busy session takes the events as steering; a run that
            // finished under us falls through to an ordinary run.
            let status = read_session_status(ctx, target).await?;
            if !matches!(status, BotSessionStatus::Idle)
                && let BotSteerRunResult::Steered { run_id } =
                    steer_run(ctx, delivery, target).await?
            {
                finish_delivery(
                    ctx,
                    target,
                    delivery,
                    DeliveryOutcome {
                        outcome: BotEventOutcome::Steered,
                        summary: None,
                        run_id: Some(run_id),
                    },
                    None,
                )
                .await;
                return Ok(());
            }
        }
        api::BotWhenBusy::Queue => {}
    }

    wait_until_session_idle(ctx, target).await?;
    let started = activity(
        ctx,
        BotActivities::start_run,
        BotStartRunRequest {
            universe_id,
            session_id: target.clone(),
            delivery_id: delivery.id.clone(),
            events: delivery.events.clone(),
            submission_id: ids::delivery_submission_id(&delivery.id),
            terminal_token: ids::delivery_terminal_token(&delivery.id),
        },
    )
    .await;
    let run_id = match started {
        Ok(BotStartRunResult::Started { run_id }) => run_id,
        Ok(BotStartRunResult::Rejected { message }) | Err(message) => {
            // A direct run can win the narrow read/start race. Hold the
            // lane through a short delay, then requeue at the front.
            ctx.state_mut(|state| state.record_error(message));
            ctx.timer_with_manual_cancellation(BOT_BUSY_RETRY_DELAY)
                .await;
            ctx.state_mut(|state| {
                state.active_by_session.remove(target.as_str());
                state.requeue_front(delivery.clone());
                state.lane_tick += 1;
            });
            return Ok(());
        }
    };
    let started_at = now_ms(ctx);
    ctx.state_mut(|state| state.mark_lane_started(target, &run_id, started_at));
    send_delivery_receipts(
        ctx,
        delivery,
        target,
        BotDeliveryPhase::Started,
        Some(run_id.clone()),
        None,
        None,
    )
    .await;

    {
        let session_id = target.clone();
        let wait = ctx.wait_for_state(move |state| {
            state.lane_has_terminal(&session_id) || state.lane_is_waiting(&session_id)
        });
        let timeout = ctx
            .timer_with_manual_cancellation(BOT_EVENT_TERMINAL_TIMEOUT)
            .fuse();
        pin_mut!(wait, timeout);
        select! {
            _ = wait => {}
            _ = timeout => {}
        }
    }

    if ctx.state(|state| state.lane_is_waiting(target)) {
        park_waiting_delivery(ctx, delivery, target, &run_id).await;
        return Ok(());
    }

    let (terminal, resolution) = ctx.state(|state| {
        state
            .active_by_session
            .get(target.as_str())
            .map(|lane| (lane.terminal.clone(), lane.resolution.clone()))
            .unwrap_or((None, None))
    });
    let outcome = delivery_outcome(terminal.as_ref(), resolution.as_ref(), Some(&run_id));
    let usage = match (&outcome.run_id, outcome.outcome) {
        (Some(run_id), outcome) if outcome != BotEventOutcome::RunFailed => {
            read_run_usage(ctx, target, run_id).await
        }
        _ => None,
    };
    finish_delivery(ctx, target, delivery, outcome, usage).await;
    Ok(())
}

/// The run waits on people, which can take days: record the delivery
/// `deferred` and end the lane, leaving the session occupied. No terminal
/// timeout applies; the run's terminal settles the delivery
/// (`settle_waiting_run`) and sends its receipts.
async fn park_waiting_delivery(ctx: &Ctx, delivery: &BotDelivery, target: &str, run_id: &str) {
    let (universe_id, bot_id) =
        ctx.state(|state| (state.config.universe_id, state.config.bot_id.clone()));
    if let Err(message) = activity(
        ctx,
        BotActivities::record_outcomes,
        BotRecordOutcomesRequest {
            universe_id,
            bot_id,
            event_ids: delivery.event_ids(),
            outcome: BotEventOutcome::Deferred,
            detail: Some(WAITING_DETAIL.to_owned()),
            run_id: Some(run_id.to_owned()),
            after_waiting: false,
        },
    )
    .await
    {
        ctx.state_mut(|state| state.record_error(message));
    }
    let now = now_ms(ctx);
    ctx.state_mut(|state| state.park_waiting_lane(target, run_id, now));
}

/// Cancel a waiting run for the delivery that supersedes it. The run's
/// terminal archives the waiting delivery and frees the session.
pub(super) async fn supersede_waiting_run(ctx: Ctx, target: String, run_id: String) {
    let universe_id = ctx.state(|state| state.config.universe_id);
    if let Err(message) = activity(
        &ctx,
        BotActivities::cancel_run,
        BotCancelRunRequest {
            universe_id,
            session_id: target.clone(),
            run_id: run_id.clone(),
        },
    )
    .await
    {
        ctx.state_mut(|state| {
            state.record_error(format!(
                "cancel superseded run {run_id} of {target}: {message}"
            ))
        });
    }
}

/// What a superseded delivery's event shows.
const SUPERSEDED_DETAIL: &str = "superseded by a later event";

/// What a waiting delivery's event shows until its run ends.
pub(super) const WAITING_DETAIL: &str = "waiting for people to answer";

/// The terminal of a run that waited on people: its outcome replaces the
/// `deferred` recorded while it waited, and its receipts go out.
pub(super) async fn settle_waiting_run(
    ctx: &Ctx,
    session_id: &str,
    waiting: WaitingRun,
    terminal: LaneTerminal,
    resolution: Option<LaneResolution>,
) {
    let outcome = if waiting.superseded_by.is_some() && terminal.status == RunStatus::Cancelled {
        DeliveryOutcome {
            outcome: BotEventOutcome::Archived,
            summary: Some(SUPERSEDED_DETAIL.to_owned()),
            run_id: Some(waiting.run_id.clone()),
        }
    } else {
        delivery_outcome(Some(&terminal), resolution.as_ref(), Some(&waiting.run_id))
    };
    let usage = match (&outcome.run_id, outcome.outcome) {
        (Some(run_id), outcome) if outcome != BotEventOutcome::RunFailed => {
            read_run_usage(ctx, session_id, run_id).await
        }
        _ => None,
    };
    let now = now_ms(ctx);
    let recent = recent_snapshot(&waiting.delivery, session_id, &outcome, usage, now);
    ctx.state_mut(|state| {
        state.remember_delivery(recent);
        state.touch_session(session_id, now);
    });
    settle_delivery(ctx, &waiting.delivery, session_id, &outcome, true).await;
}

/// Best effort: the cached share is observability, never a reason to fail
/// a delivery that already finished.
async fn read_run_usage(ctx: &Ctx, session_id: &str, run_id: &str) -> Option<LlmUsageView> {
    let universe_id = ctx.state(|state| state.config.universe_id);
    activity(
        ctx,
        BotActivities::read_run_usage,
        BotReadRunUsageRequest {
            universe_id,
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
        },
    )
    .await
    .ok()
    .and_then(|result| result.usage)
}

async fn read_session_status(ctx: &Ctx, session_id: &str) -> Result<BotSessionStatus, String> {
    let universe_id = ctx.state(|state| state.config.universe_id);
    activity(
        ctx,
        BotActivities::read_session_status,
        BotSessionRequest {
            universe_id,
            session_id: session_id.to_owned(),
        },
    )
    .await
}

async fn steer_run(
    ctx: &Ctx,
    delivery: &BotDelivery,
    session_id: &str,
) -> Result<BotSteerRunResult, String> {
    let universe_id = ctx.state(|state| state.config.universe_id);
    activity(
        ctx,
        BotActivities::steer_run,
        BotSteerRunRequest {
            universe_id,
            session_id: session_id.to_owned(),
            delivery_id: delivery.id.clone(),
            events: delivery.events.clone(),
        },
    )
    .await
}

/// Create a routed (perKey / perEvent) session on first use, returning the
/// session id actually ensured. A declaration mismatch — the session
/// pre-exists under an older toolset, e.g. after a controller restart
/// without carry — rotates the key to its next generation and retries
/// once, mirroring the main session's rotation, instead of wedging the
/// delivery. `None` means the session could not be created; the reason is
/// in `last_error`.
async fn ensure_routed_session(
    ctx: &Ctx,
    session: &RoutedSession,
    resolved_id: &str,
    tools_ref: Option<String>,
) -> Option<String> {
    let mut session_id = resolved_id.to_owned();
    // A program session pinned its program at creation. Once the bot's
    // profile has moved on, retire the session while it is idle and let the
    // key continue in its next generation with the current program. A busy
    // session refuses the non-forced close and keeps its pin until a later
    // delivery finds it idle.
    if ctx.state(|state| state.routed_pin_is_stale(&session_id))
        && close_session(ctx, &session_id, false).await
    {
        session_id = ctx.state_mut(|state| {
            state.forget_routed_session(&session_id);
            state.resolve_routed_session_id(ids::routed_session_base(&session_id))
        });
    }
    for attempt in 0..2 {
        let observed_at_ms = now_ms(ctx);
        let known = ctx
            .state_mut(|state| state.observe_routed_session(&session_id, session, observed_at_ms));
        if known {
            return Some(session_id);
        }
        let request = ctx.state(|state| {
            ensure_request(
                state,
                ctx.workflow_id(),
                session_id.clone(),
                state.routed_label(&session.label),
                // Routed sessions take the profile at creation; only the
                // main session tracks profile revisions across its life.
                None,
                tools_ref.clone(),
                Some(session),
            )
        });
        match activity(ctx, BotActivities::ensure_session, request).await {
            Ok(BotEnsureSessionResult::Ready {
                profile_revision,
                carried_tool_ids,
                program,
            }) => {
                let now = now_ms(ctx);
                let kind = ids::bot_session_kind(&session_id);
                ctx.state_mut(|state| {
                    state.program_sessions = program;
                    state.extra_sessions.push(ManagedSession {
                        session_id: session_id.clone(),
                        label: session.label.clone(),
                        session_key: session.session_key.clone(),
                        kind,
                        last_active_at_ms: Some(now),
                        close_policy: session.close_policy,
                        carried_tool_ids,
                        profile_revision: Some(profile_revision),
                    });
                });
                enforce_extra_session_cap(ctx, &session_id).await;
                return Some(session_id);
            }
            Ok(
                BotEnsureSessionResult::DeclarationMismatch { message }
                | BotEnsureSessionResult::ProfileUnapplicable { message },
            ) => {
                if attempt == 0 {
                    let base = ids::routed_session_base(&session_id).to_owned();
                    session_id = ctx.state_mut(|state| {
                        state.bump_routed_generation(&base);
                        state.resolve_routed_session_id(&base)
                    });
                    continue;
                }
                ctx.state_mut(|state| state.record_error(message));
                return None;
            }
            Err(message) => {
                ctx.state_mut(|state| state.record_error(message));
                return None;
            }
        }
    }
    None
}

/// A session evicted from the tracked set would otherwise escape the
/// idle-close sweep and teardown: close the idlest free one (non-force) as
/// it leaves. A session that will not close stays tracked for the next
/// sweep.
async fn enforce_extra_session_cap(ctx: &Ctx, keep: &str) {
    while ctx.state(|state| state.extra_sessions.len() > BOT_EXTRA_SESSION_CAP) {
        let Some(victim) = ctx.state(|state| state.idlest_free_session(keep)) else {
            break;
        };
        if !close_session(ctx, &victim, false).await {
            break;
        }
        ctx.state_mut(|state| state.forget_routed_session(&victim));
    }
}

// ── Sidecar ─────────────────────────────────────────────────────────────────

/// A steer/append delivery on a session whose lane has a started run. The
/// sidecar never starts a run: if the run finished underneath it, the
/// delivery goes back to the front of the queue for an ordinary lane.
pub(super) async fn run_busy_sidecar(ctx: Ctx, delivery: BotDelivery, target: String) {
    let settled = match run_busy_sidecar_inner(&ctx, &delivery, &target).await {
        Ok(settled) => settled,
        Err(message) => {
            ctx.state_mut(|state| state.record_error(message.clone()));
            Some(DeliveryOutcome {
                outcome: BotEventOutcome::RunFailed,
                summary: Some(message),
                run_id: None,
            })
        }
    };
    let now = now_ms(&ctx);
    if let Some(outcome) = settled {
        let recent = recent_snapshot(&delivery, &target, &outcome, None, now);
        ctx.state_mut(|state| {
            state.remember_delivery(recent);
            state.release_sidecar(&target, now);
        });
        settle_delivery(&ctx, &delivery, &target, &outcome, false).await;
    } else {
        ctx.state_mut(|state| state.release_sidecar(&target, now));
    }
}

/// `Ok(None)` when the delivery was requeued instead of settled.
async fn run_busy_sidecar_inner(
    ctx: &Ctx,
    delivery: &BotDelivery,
    target: &str,
) -> Result<Option<DeliveryOutcome>, String> {
    if delivery.events.is_empty() {
        return Ok(None);
    }
    let universe_id = ctx.state(|state| state.config.universe_id);
    if ctx.state(|state| state.when_busy_of(delivery)) == api::BotWhenBusy::Append {
        activity(
            ctx,
            BotActivities::append_context,
            BotAppendContextRequest {
                universe_id,
                session_id: target.to_owned(),
                delivery_id: delivery.id.clone(),
                events: delivery.events.clone(),
            },
        )
        .await?;
        return Ok(Some(DeliveryOutcome {
            outcome: BotEventOutcome::Appended,
            summary: None,
            run_id: None,
        }));
    }
    let status = read_session_status(ctx, target).await?;
    if !matches!(status, BotSessionStatus::Idle)
        && let BotSteerRunResult::Steered { run_id } = steer_run(ctx, delivery, target).await?
    {
        return Ok(Some(DeliveryOutcome {
            outcome: BotEventOutcome::Steered,
            summary: None,
            run_id: Some(run_id),
        }));
    }
    // The tracked run finished (or has not started) under us. Preserve the
    // delivery for an ordinary lane attempt once the session is available.
    ctx.state_mut(|state| state.requeue_front(delivery.clone()));
    Ok(None)
}

// ── Finishing ───────────────────────────────────────────────────────────────

fn recent_snapshot(
    delivery: &BotDelivery,
    session_id: &str,
    outcome: &DeliveryOutcome,
    usage: Option<LlmUsageView>,
    finished_at_ms: i64,
) -> BotRecentDeliverySnapshot {
    BotRecentDeliverySnapshot {
        delivery_id: delivery.id.clone(),
        seqs: delivery.seqs(),
        session_id: session_id.to_owned(),
        run_id: outcome.run_id.clone(),
        outcome: outcome.outcome,
        summary: outcome.summary.clone(),
        finished_at_ms,
        usage,
    }
}

/// Remember the delivery, free the lane (the session is dispatchable again
/// at once), then settle the read model and receipts best-effort.
async fn finish_delivery(
    ctx: &Ctx,
    session_id: &str,
    delivery: &BotDelivery,
    outcome: DeliveryOutcome,
    usage: Option<LlmUsageView>,
) {
    let now = now_ms(ctx);
    let recent = recent_snapshot(delivery, session_id, &outcome, usage, now);
    ctx.state_mut(|state| {
        state.remember_delivery(recent);
        state.release_lane(session_id, now);
    });
    settle_delivery(ctx, delivery, session_id, &outcome, false).await;
}

/// The write-once outcome on every event row, `bot.reply` receipts for
/// events that asked, and the `finished` delivery receipt for events whose
/// source asked. Each is best effort — a failed write costs a pending
/// badge or a receipt, never a delivery. `after_waiting` lets the outcome
/// replace the `deferred` recorded while the run waited.
async fn settle_delivery(
    ctx: &Ctx,
    delivery: &BotDelivery,
    session_id: &str,
    outcome: &DeliveryOutcome,
    after_waiting: bool,
) {
    let (universe_id, bot_id) =
        ctx.state(|state| (state.config.universe_id, state.config.bot_id.clone()));
    if let Err(message) = activity(
        ctx,
        BotActivities::record_outcomes,
        BotRecordOutcomesRequest {
            universe_id,
            bot_id: bot_id.clone(),
            event_ids: delivery.event_ids(),
            outcome: outcome.outcome,
            detail: outcome.summary.clone(),
            run_id: outcome.run_id.clone(),
            after_waiting,
        },
    )
    .await
    {
        ctx.state_mut(|state| state.record_error(message));
    }
    ctx.state_mut(|state| state.lane_tick += 1);

    let reply_ids = delivery.reply_event_ids();
    if !reply_ids.is_empty() {
        if let Err(message) = activity(
            ctx,
            BotActivities::send_bot_receipts,
            BotSendBotReceiptsRequest {
                universe_id,
                bot_id,
                delivery_id: delivery.id.clone(),
                event_ids: reply_ids,
                outcome: outcome.outcome,
                summary: outcome.summary.clone(),
                hops: delivery.hops(),
            },
        )
        .await
        {
            ctx.state_mut(|state| state.record_error(message));
        }
        ctx.state_mut(|state| state.lane_tick += 1);
    }

    send_delivery_receipts(
        ctx,
        delivery,
        session_id,
        BotDeliveryPhase::Finished,
        outcome.run_id.clone(),
        Some(outcome.outcome),
        outcome.summary.clone(),
    )
    .await;
}

/// `started` / `finished` receipts to the admitting source of any event in
/// the delivery that asked for them (a chat conversation waiting to type
/// and to send the fallback reply). Best effort.
async fn send_delivery_receipts(
    ctx: &Ctx,
    delivery: &BotDelivery,
    session_id: &str,
    phase: BotDeliveryPhase,
    run_id: Option<String>,
    outcome: Option<BotEventOutcome>,
    summary: Option<String>,
) {
    let event_ids = delivery.notify_event_ids();
    if event_ids.is_empty() {
        return;
    }
    let (universe_id, bot_id) =
        ctx.state(|state| (state.config.universe_id, state.config.bot_id.clone()));
    if let Err(message) = activity(
        ctx,
        BotActivities::send_delivery_receipts,
        BotSendDeliveryReceiptsRequest {
            universe_id,
            bot_id,
            event_ids,
            phase,
            delivery_id: delivery.id.clone(),
            seqs: delivery.seqs(),
            session_id: session_id.to_owned(),
            run_id,
            outcome,
            summary,
        },
    )
    .await
    {
        ctx.state_mut(|state| state.record_error(message));
    }
    ctx.state_mut(|state| state.lane_tick += 1);
}

// ── Pushed tools ────────────────────────────────────────────────────────────

/// Answer a pushed `bot_*` invocation from the controller's own state and
/// the tool activity, then resolve the session's parked call by signalling
/// the holder session workflow directly. Every pushed tool is joined —
/// including `bot_emit`, whose refusals the model must read — so a failed
/// activity still resolves the promise, as failed.
pub(super) async fn handle_invocation(ctx: Ctx, pending: PendingInvocation) {
    let PendingInvocation {
        invocation,
        holder_workflow_id,
    } = pending;
    let Some(reply_promise_id) = invocation
        .completion_promises
        .as_ref()
        .and_then(|promises| promises.get(REPLY_COMPLETION_KEY))
        .cloned()
    else {
        ctx.state_mut(|state| {
            state.record_error(format!(
                "pushed invocation {} of {} carries no reply promise",
                invocation.invocation_id, invocation.tool_id
            ))
        });
        return;
    };
    let now = now_ms(&ctx);
    let (universe_id, request) = ctx.state(|state| {
        (
            state.config.universe_id,
            BotExecuteToolRequest {
                universe_id: state.config.universe_id,
                bot_id: state.config.bot_id.clone(),
                session_id: invocation.session_id.as_str().to_owned(),
                invocation_id: invocation.invocation_id.as_str().to_owned(),
                tool_id: invocation.tool_id.as_str().to_owned(),
                arguments_ref: invocation.arguments_ref.as_str().to_owned(),
                controller: state.controller_summary(invocation.session_id.as_str(), now),
            },
        )
    });
    let resolution = match activity(&ctx, BotActivities::execute_tool, request).await {
        Ok(BotExecuteToolResult::Resolved { payload_ref }) => PromiseResolution::Resolved {
            payload_ref: parse_blob_ref(&ctx, payload_ref),
        },
        Ok(BotExecuteToolResult::Failed { error_ref, .. }) => PromiseResolution::Failed {
            error_ref: parse_blob_ref(&ctx, error_ref),
        },
        Err(message) => {
            ctx.state_mut(|state| state.record_error(message));
            PromiseResolution::Failed { error_ref: None }
        }
    };
    let envelope = EmissionEnvelope::source_resolution(
        universe_id,
        ctx.workflow_id().to_owned(),
        &holder_workflow_id,
        reply_promise_id,
        resolution,
    );
    let signalled = ctx
        .external_workflow(holder_workflow_id.clone(), None)
        .signal(
            AgentSessionWorkflow::deliver_emission,
            envelope,
            crate::workflows::signal_options(),
        )
        .await;
    if let Err(failure) = signalled {
        ctx.state_mut(|state| {
            state.record_error(format!(
                "resolve pushed invocation at {holder_workflow_id}: {}",
                failure
            ))
        });
    }
    ctx.state_mut(|state| state.lane_tick += 1);
}

/// A CAS ref the tool activity stored; a malformed one is recorded and
/// resolves the promise without a payload.
fn parse_blob_ref(ctx: &Ctx, value: String) -> Option<BlobRef> {
    match BlobRef::parse(value) {
        Ok(blob_ref) => Some(blob_ref),
        Err(error) => {
            ctx.state_mut(|state| state.record_error(format!("tool result ref: {error}")));
            None
        }
    }
}
