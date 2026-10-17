//! The bridge between a program and its host: the native callbacks the
//! prelude calls, module evaluation, completion delivery and error mapping.

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    rc::Rc,
};

use rquickjs::{Ctx, Exception, Function, Module, Object, Promise, Value as JsValue};
use serde_json::Value;

use crate::*;

pub(crate) const PRELUDE: &str = include_str!("prelude.js");

#[derive(Default)]
pub(crate) struct State {
    pub(crate) pending: BTreeSet<String>,
    /// Calls whose cancel the program asked for, so it is sent or settled
    /// once.
    pub(crate) cancelled: BTreeSet<String>,
    /// Promises whose cancel the program asked for.
    pub(crate) cancelled_promises: BTreeSet<String>,
    /// Calls cancelled in the step that issued them: they never reach the
    /// host and are delivered to the program as cancelled, in order.
    pub(crate) local: VecDeque<HostCompletion>,
    pub(crate) failure: Option<ExecutionError>,
    pub(crate) calls: u32,
    pub(crate) jobs: u64,
    /// The exit `run` returned and the state it hands over.
    pub(crate) exit: Option<(ProgramExit, Value)>,
    /// The program's clock: where it started, then the latest completion
    /// time delivered.
    pub(crate) clock_ms: u64,
}

pub(crate) fn validate(input: &ExecutionInput) -> Result<(), StartError> {
    let limits = &input.limits;
    for (name, value) in [
        ("max_memory_bytes", limits.max_memory_bytes),
        ("max_stack_bytes", limits.max_stack_bytes),
        ("max_source_bytes", limits.max_source_bytes),
        ("max_catalog_bytes", limits.max_catalog_bytes),
        ("max_request_bytes", limits.max_request_bytes),
        ("max_result_bytes", limits.max_result_bytes),
        ("max_output_bytes", limits.max_output_bytes),
        ("max_tool_calls", u64::from(limits.max_tool_calls)),
        (
            "max_outstanding_tool_calls",
            u64::from(limits.max_outstanding_tool_calls),
        ),
    ] {
        if value == 0 || usize::try_from(value).is_err() {
            return Err(StartError::InvalidInput(format!(
                "{name} must be positive and fit native size"
            )));
        }
    }
    if limits.max_outstanding_tool_calls > limits.max_tool_calls {
        return Err(StartError::InvalidInput(
            "outstanding budget exceeds total call budget".into(),
        ));
    }
    if input.source.len() as u64 > limits.max_source_bytes {
        return Err(StartError::InvalidInput(
            "source exceeds source byte limit".into(),
        ));
    }
    if serialized_len(&input.program.input) > limits.max_request_bytes {
        return Err(StartError::InvalidInput(
            "program input exceeds request byte limit".into(),
        ));
    }
    if serialized_len(&input.bindings) > limits.max_catalog_bytes {
        return Err(StartError::InvalidInput(
            "bindings exceed catalog byte limit".into(),
        ));
    }
    let mut names = HashSet::new();
    let mut ids = HashSet::new();
    for binding in &input.bindings {
        if binding.name.is_empty()
            || binding.binding_id.is_empty()
            || !names.insert(&binding.name)
            || !ids.insert(&binding.binding_id)
        {
            return Err(StartError::InvalidInput(
                "bindings need unique nonempty names and identities".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn error(kind: ExecutionErrorKind, message: impl Into<String>) -> ExecutionError {
    ExecutionError {
        kind,
        message: message.into(),
    }
}

/// The bridge controls a prepared context drives: completion delivery,
/// return-value serialization, and the promise the script or program
/// handler settles.
pub(crate) struct Prepared<'js> {
    pub(crate) deliver: Function<'js>,
    pub(crate) serialize: Function<'js>,
    pub(crate) promise: Promise<'js>,
}

/// The value a settled script or program promise produced, serialized
/// within the output budget.
pub(crate) fn settled_value<'js>(
    ctx: &Ctx<'js>,
    serialize: &Function<'js>,
    limits: &ExecutionLimits,
    result: rquickjs::Result<JsValue<'js>>,
) -> Result<Option<Value>, ExecutionError> {
    let value = result.map_err(|failure| js_error(ctx, failure))?;
    if value.is_undefined() {
        return Ok(None);
    }
    let json: String = serialize.call((value,)).map_err(|failure| {
        let mut error = js_error(ctx, failure);
        error.kind = ExecutionErrorKind::UnsupportedValue;
        error
    })?;
    if json.len() as u64 > limits.max_output_bytes {
        return Err(error(
            ExecutionErrorKind::LimitExceeded,
            "return value exceeds output byte limit",
        ));
    }
    serde_json::from_str(&json)
        .map(Some)
        .map_err(|failure| error(ExecutionErrorKind::Internal, failure.to_string()))
}

/// Install the bridge and prelude in `ctx`, compile the program module,
/// evaluate it and call its handler.
pub(crate) fn prepare_context<'js>(
    ctx: &Ctx<'js>,
    input: &ExecutionInput,
    state: Rc<RefCell<State>>,
    fatal: Rc<Cell<bool>>,
    outbox: Rc<RefCell<Vec<HostRequest>>>,
    cancels: Rc<RefCell<Vec<HostCancel>>>,
) -> Result<Prepared<'js>, ExecutionError> {
    let ctx = ctx.clone();
    let send_state = state.clone();
    let cancel_outbox = outbox.clone();
    let send_fatal = fatal.clone();
    let send_limits = input.limits.clone();
    let allowed: HashMap<_, _> = input
        .bindings
        .iter()
        .map(|binding| (binding.binding_id.clone(), binding.name.clone()))
        .collect();
    let send = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, binding_id: String, json: String| -> rquickjs::Result<String> {
            let mut state = send_state.borrow_mut();
            let failure = if !allowed.contains_key(&binding_id) {
                Some(error(ExecutionErrorKind::Internal, "unrecognized binding"))
            } else if json.len() as u64 > send_limits.max_request_bytes {
                Some(error(
                    ExecutionErrorKind::LimitExceeded,
                    "tool arguments exceed request byte limit",
                ))
            } else if state.calls >= send_limits.max_tool_calls {
                Some(error(
                    ExecutionErrorKind::LimitExceeded,
                    "tool call budget exhausted",
                ))
            } else if state.pending.len() >= send_limits.max_outstanding_tool_calls as usize {
                Some(error(
                    ExecutionErrorKind::LimitExceeded,
                    "outstanding tool call budget exhausted",
                ))
            } else {
                None
            };
            if let Some(failure) = failure {
                return Err(fail_callback(&ctx, &mut state, &send_fatal, failure));
            }
            let arguments: Value = serde_json::from_str(&json).map_err(|_| {
                fail_callback(
                    &ctx,
                    &mut state,
                    &send_fatal,
                    error(ExecutionErrorKind::Internal, "invalid bridge JSON"),
                )
            })?;
            let request_id = format!("call-{}", state.calls + 1);
            let request = HostRequest {
                request_id: request_id.clone(),
                binding_id,
                arguments,
            };
            outbox.borrow_mut().push(request);
            state.calls += 1;
            state.pending.insert(request_id.clone());
            Ok(request_id)
        },
    )
    .map_err(|failure| js_error(&ctx, failure))?;
    // `cancel(kind, id)`: the program cancels a call or a held promise. A
    // call still in this step's outbox is withdrawn and settles here; one
    // the host has admitted, and a promise, are cancelled by the host.
    let cancel_state = state.clone();
    let cancel_fatal = fatal.clone();
    let cancel = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, kind: String, id: String| -> rquickjs::Result<()> {
            let mut state = cancel_state.borrow_mut();
            match kind.as_str() {
                "call" => {
                    if !issued(&state, &id) {
                        return Err(fail_callback(
                            &ctx,
                            &mut state,
                            &cancel_fatal,
                            error(ExecutionErrorKind::Internal, "cancel of an unissued call"),
                        ));
                    }
                    if !state.pending.contains(&id) || !state.cancelled.insert(id.clone()) {
                        return Ok(());
                    }
                    let mut outbox = cancel_outbox.borrow_mut();
                    if let Some(index) = outbox.iter().position(|request| request.request_id == id)
                    {
                        outbox.remove(index);
                        state.local.push_back(HostCompletion {
                            request_id: id,
                            outcome: Err(HostError {
                                kind: CANCELLED_CALL_KIND.into(),
                                message: "call was cancelled before it was sent".into(),
                                value: None,
                            }),
                            completed_at_ms: None,
                        });
                    } else {
                        cancels
                            .borrow_mut()
                            .push(HostCancel::Call { request_id: id });
                    }
                    Ok(())
                }
                "promise" => {
                    if state.cancelled_promises.insert(id.clone()) {
                        cancels
                            .borrow_mut()
                            .push(HostCancel::Promise { promise_id: id });
                    }
                    Ok(())
                }
                _ => Err(fail_callback(
                    &ctx,
                    &mut state,
                    &cancel_fatal,
                    error(ExecutionErrorKind::Internal, "unknown cancel target"),
                )),
            }
        },
    )
    .map_err(|failure| js_error(&ctx, failure))?;
    // `keep(json, exit)`: an exit checks its state when called (an oversized
    // value is an ordinary exception the program can catch), and the
    // prelude keeps it with the exit's kind once `run` returns the token.
    let keep_state = state.clone();
    let keep = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, json: String, exit: Option<String>| -> rquickjs::Result<()> {
            if json.len() > MAX_PROGRAM_STATE_BYTES {
                return Err(Exception::throw_range(
                    &ctx,
                    &format!("program state exceeds {MAX_PROGRAM_STATE_BYTES} bytes of JSON"),
                ));
            }
            let value = serde_json::from_str(&json)
                .map_err(|_| Exception::throw_type(&ctx, "program state is not JSON"))?;
            let exit = match exit.as_deref() {
                None => return Ok(()),
                Some("complete") => ProgramExit::Complete,
                Some("continueAsNew") => ProgramExit::ContinueAsNew,
                Some(_) => return Err(Exception::throw_type(&ctx, "unknown program exit")),
            };
            keep_state.borrow_mut().exit = Some((exit, value));
            Ok(())
        },
    )
    .map_err(|failure| js_error(&ctx, failure))?;
    let prelude: Function = ctx
        .eval(PRELUDE)
        .map_err(|failure| js_error(&ctx, failure))?;
    let catalog = serde_json::to_string(&input.bindings)
        .map_err(|failure| error(ExecutionErrorKind::Internal, failure.to_string()))?;
    let program = serde_json::to_string(&input.program)
        .map_err(|failure| error(ExecutionErrorKind::Internal, failure.to_string()))?;
    let controls: Object = prelude
        .call((send, cancel, keep, catalog, program))
        .map_err(|failure| js_error(&ctx, failure))?;
    let deliver: Function = controls
        .get("deliver")
        .map_err(|failure| js_error(&ctx, failure))?;
    let serialize: Function = controls
        .get("serialize")
        .map_err(|failure| js_error(&ctx, failure))?;
    let promise = start_program(&ctx, &controls, &input.source)?;
    Ok(Prepared {
        deliver,
        serialize,
        promise,
    })
}

/// Compile the program module before any guest statement runs, build its
/// `env` and disable code generation from strings, then evaluate the module
/// and call its handler. The returned promise settles with the handler's
/// result.
fn start_program<'js>(
    ctx: &Ctx<'js>,
    controls: &Object<'js>,
    source: &str,
) -> Result<Promise<'js>, ExecutionError> {
    let module = Module::declare(ctx.clone(), "program.js", source)
        .map_err(|failure| js_error(ctx, failure))?;
    let prepare: Function = controls
        .get("prepare")
        .map_err(|failure| js_error(ctx, failure))?;
    prepare
        .call::<_, ()>(())
        .map_err(|failure| js_error(ctx, failure))?;
    let (module, evaluation) = module.eval().map_err(|failure| js_error(ctx, failure))?;
    let namespace = module
        .namespace()
        .map_err(|failure| js_error(ctx, failure))?;
    let program: Function = controls
        .get("program")
        .map_err(|failure| js_error(ctx, failure))?;
    program
        .call((namespace, evaluation))
        .map_err(|failure| js_error(ctx, failure))
}

pub(crate) fn apply_completion<'js>(
    ctx: &Ctx<'js>,
    deliver: &Function<'js>,
    state: &Rc<RefCell<State>>,
    completion: HostCompletion,
) -> Result<(), ExecutionError> {
    // A duplicate host completion cannot settle a later request or inject a new
    // result. Treat a faulty host bridge as a terminal integration error.
    if !state.borrow().pending.contains(&completion.request_id) {
        // Under quiescent delivery a recorded completion always names a
        // request the program has already issued. One it has not issued
        // means this execution diverged from the one that was recorded.
        if !issued(&state.borrow(), &completion.request_id) {
            return Err(error(
                ExecutionErrorKind::Nondeterminism,
                format!(
                    "recorded completion for {} reached a program that has not issued it",
                    completion.request_id
                ),
            ));
        }
        return Err(error(
            ExecutionErrorKind::Internal,
            "host completed an unknown or already completed request",
        ));
    }
    let (success, json) = match completion.outcome {
        Ok(value) => (true, serde_json::to_string(&value)),
        Err(value) => (false, serde_json::to_string(&value)),
    };
    let json = json.map_err(|failure| error(ExecutionErrorKind::Internal, failure.to_string()))?;
    // Millisecond timestamps are exact in a double well past year 275000.
    let completed_at = completion.completed_at_ms.map(|at| at as f64);
    deliver
        .call::<_, ()>((&completion.request_id, success, json, completed_at))
        .map_err(|failure| js_error(ctx, failure))?;
    let mut state = state.borrow_mut();
    state.pending.remove(&completion.request_id);
    if let Some(at) = completion.completed_at_ms {
        state.clock_ms = state.clock_ms.max(at);
    }
    Ok(())
}

/// Whether `request_id` names a request this execution has issued.
fn issued(state: &State, request_id: &str) -> bool {
    request_id
        .strip_prefix("call-")
        .and_then(|sequence| sequence.parse::<u32>().ok())
        .is_some_and(|sequence| sequence >= 1 && sequence <= state.calls)
}

fn fail_callback(
    ctx: &Ctx<'_>,
    state: &mut State,
    fatal: &Cell<bool>,
    failure: ExecutionError,
) -> rquickjs::Error {
    let exception = Exception::throw_type(ctx, &failure.message);
    if state.failure.is_none() {
        state.failure = Some(failure);
    }
    // The interrupt handler stops the program at its next check, so a
    // guest cannot catch a bridge failure and carry on.
    fatal.set(true);
    exception
}

pub(crate) fn native_error(failure: rquickjs::Error) -> ExecutionError {
    let kind = if matches!(failure, rquickjs::Error::Allocation) {
        ExecutionErrorKind::LimitExceeded
    } else {
        ExecutionErrorKind::Internal
    };
    error(kind, failure.to_string())
}

pub(crate) fn js_error(ctx: &Ctx<'_>, failure: rquickjs::Error) -> ExecutionError {
    if !matches!(failure, rquickjs::Error::Exception) {
        return native_error(failure);
    }
    let exception = ctx.catch();
    let message = if let Some(exception) = exception.as_exception() {
        exception
            .message()
            .unwrap_or_else(|| "JavaScript exception".into())
    } else if let Some(message) = exception.as_string() {
        message
            .to_string()
            .unwrap_or_else(|_| "JavaScript exception".into())
    } else {
        "JavaScript threw a non-error value".into()
    };
    // A guest can throw an arbitrarily large error string; report retention has
    // a small independent cap rather than bypassing selected-output limits.
    let mut message = message;
    if message.len() > 4096 {
        let mut end = 4096;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    let kind = if message.contains("out of memory")
        || message.contains("stack overflow")
        || message.contains("Maximum call stack size exceeded")
    {
        ExecutionErrorKind::LimitExceeded
    } else {
        ExecutionErrorKind::Javascript
    };
    error(kind, message)
}
