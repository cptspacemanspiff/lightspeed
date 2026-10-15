use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap, HashSet},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc as sync_mpsc,
    },
    time::{Duration, Instant},
};

use rquickjs::{
    Context, Ctx, Exception, Function, Module, Object, Promise, Runtime, Value as JsValue,
};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::*;

pub(crate) const PRELUDE: &str = include_str!("prelude.js");
const IDLE_POLL: Duration = Duration::from_millis(5);
const EVENT_BACKOFF: Duration = Duration::from_millis(1);

#[derive(Default)]
pub(crate) struct State {
    pub(crate) pending: BTreeSet<String>,
    pub(crate) failure: Option<ExecutionError>,
    pub(crate) calls: u32,
    pub(crate) jobs: u64,
    startup_micros: u64,
    pub(crate) program_context: Option<Value>,
}

pub(super) fn start(
    input: ExecutionInput,
    cancellation: Cancellation,
) -> Result<Execution, StartError> {
    validate(&input)?;
    let capacity = input.limits.max_outstanding_tool_calls as usize;
    let stack_size = (input.limits.max_stack_bytes as usize)
        .checked_add(512 * 1024)
        .ok_or_else(|| StartError::InvalidInput("stack budget overflows native size".into()))?
        .max(2 * 1024 * 1024);
    // One extra event slot lets terminal reporting go out behind all
    // outstanding requests while the host is not polling. A replaying
    // program can get further ahead; its sends wait for room.
    let (event_tx, event_rx) = mpsc::channel(capacity + 1);
    let (completion_tx, completion_rx) = sync_mpsc::sync_channel(capacity);
    let completions = CompletionSender {
        sender: completion_tx,
        max_result_bytes: input.limits.max_result_bytes,
    };
    let thread_cancel = cancellation.clone();
    let waiting = Arc::new(AtomicBool::new(false));
    let thread_waiting = waiting.clone();
    std::thread::Builder::new()
        .name("codemode".into())
        .stack_size(stack_size)
        .spawn(move || {
            let report = run(
                input,
                thread_cancel,
                thread_waiting,
                &event_tx,
                completion_rx,
            );
            // Waits for room behind unread requests; fails only once the
            // host has dropped the execution.
            let _ = event_tx.blocking_send(ExecutionEvent::Finished(report));
        })?;
    Ok(Execution {
        events: event_rx,
        completions,
        cancellation,
        waiting,
    })
}

pub(crate) fn validate(input: &ExecutionInput) -> Result<(), StartError> {
    let limits = &input.limits;
    for (name, value) in [
        ("timeout_ms", limits.timeout_ms),
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
    if Instant::now()
        .checked_add(Duration::from_millis(limits.timeout_ms))
        .is_none()
    {
        return Err(StartError::InvalidInput(
            "timeout exceeds native duration".into(),
        ));
    }
    Ok(())
}

/// Where a guest's host requests go: the host's event channel for a
/// threaded execution, or a buffer a stepped program instance drains after
/// each step.
#[derive(Clone)]
pub(crate) enum RequestSink {
    Channel(mpsc::Sender<ExecutionEvent>),
    Buffer(Rc<RefCell<Vec<HostRequest>>>),
}

impl RequestSink {
    fn send(
        &self,
        request: HostRequest,
        cancellation: &Cancellation,
        deadline: Instant,
    ) -> Result<(), ExecutionError> {
        let sender = match self {
            Self::Buffer(buffer) => {
                buffer.borrow_mut().push(request);
                return Ok(());
            }
            Self::Channel(sender) => sender,
        };
        // A program replaying its journal can issue requests faster than
        // the host reads them, since recorded completions arrive ahead; wait
        // for room rather than fail. Waiting is invisible to the guest, so
        // it cannot change what the program does.
        let mut event = ExecutionEvent::Request(request);
        loop {
            match sender.try_send(event) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    if let Some(reason) = stop_reason(cancellation, deadline) {
                        return Err(reason);
                    }
                    event = returned;
                    std::thread::sleep(EVENT_BACKOFF);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(error(
                        ExecutionErrorKind::HostDisconnected,
                        "host request receiver is unavailable",
                    ));
                }
            }
        }
    }
}

pub(crate) fn error(kind: ExecutionErrorKind, message: impl Into<String>) -> ExecutionError {
    ExecutionError {
        kind,
        message: message.into(),
    }
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn stop_reason(cancellation: &Cancellation, deadline: Instant) -> Option<ExecutionError> {
    if cancellation.is_cancelled() {
        Some(error(
            ExecutionErrorKind::Cancelled,
            "JavaScript execution was cancelled",
        ))
    } else if Instant::now() >= deadline {
        Some(error(
            ExecutionErrorKind::TimedOut,
            "JavaScript execution exceeded its wall-clock limit",
        ))
    } else {
        None
    }
}

fn run(
    input: ExecutionInput,
    cancellation: Cancellation,
    waiting: Arc<AtomicBool>,
    events: &mpsc::Sender<ExecutionEvent>,
    completions: sync_mpsc::Receiver<HostCompletion>,
) -> ExecutionReport {
    let started = Instant::now();
    let deadline = started + Duration::from_millis(input.limits.timeout_ms);
    let state = Rc::new(RefCell::new(State::default()));
    let fatal = Arc::new(AtomicBool::new(false));
    let result = (|| {
        if let Some(reason) = stop_reason(&cancellation, deadline) {
            return Err(reason);
        }
        let runtime = Runtime::new().map_err(native_error)?;
        runtime.set_memory_limit(input.limits.max_memory_bytes as usize);
        runtime.set_max_stack_size(input.limits.max_stack_bytes as usize);
        let interrupt_cancel = cancellation.clone();
        let interrupt_fatal = fatal.clone();
        runtime.set_interrupt_handler(Some(Box::new(move || {
            interrupt_cancel.is_cancelled()
                || Instant::now() >= deadline
                || interrupt_fatal.load(Ordering::Acquire)
        })));
        let context = Context::full(&runtime).map_err(native_error)?;
        context.with(|ctx| {
            run_context(
                ctx,
                &input,
                state.clone(),
                fatal,
                &cancellation,
                &waiting,
                deadline,
                started,
                events,
                &completions,
            )
        })
    })();
    let mut state = state.borrow_mut();
    let (return_value, execution_error) = match result {
        Ok(value) => (value, None),
        Err(failure) => (None, Some(failure)),
    };
    // Resource limits and cancellation remain terminal even if guest code
    // catches the exception raised at an interrupt or host-callback boundary.
    let execution_error = stop_reason(&cancellation, deadline)
        .or_else(|| state.failure.take())
        .or(execution_error);
    ExecutionReport {
        return_value,
        program_context: state.program_context.take(),
        error: execution_error,
        pending_request_ids: state.pending.iter().cloned().collect(),
        metrics: ExecutionMetrics {
            startup_micros: state.startup_micros,
            elapsed_micros: elapsed_micros(started),
            tool_calls: state.calls,
            pending_jobs: state.jobs,
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn run_context<'js>(
    ctx: Ctx<'js>,
    input: &ExecutionInput,
    state: Rc<RefCell<State>>,
    fatal: Arc<AtomicBool>,
    cancellation: &Cancellation,
    waiting: &AtomicBool,
    deadline: Instant,
    started: Instant,
    events: &mpsc::Sender<ExecutionEvent>,
    completions: &sync_mpsc::Receiver<HostCompletion>,
) -> Result<Option<Value>, ExecutionError> {
    let Prepared {
        deliver,
        serialize,
        promise,
    } = prepare_context(
        &ctx,
        input,
        state.clone(),
        fatal,
        cancellation,
        deadline,
        started,
        RequestSink::Channel(events.clone()),
    )?;
    loop {
        if let Some(reason) = stop_reason(cancellation, deadline) {
            return Err(reason);
        }
        if let Some(failure) = state.borrow().failure.clone() {
            return Err(failure);
        }
        if let Some(result) = promise.result::<JsValue>() {
            return settled_value(&ctx, &serialize, &input.limits, result);
        }
        // A program takes one completion only when it has no runnable job
        // (quiescent delivery), so its execution is a function of the
        // completion order alone. The deadline stops a job chain that never
        // quiesces.
        if ctx.execute_pending_job() {
            waiting.store(false, Ordering::Release);
            state.borrow_mut().jobs += 1;
            continue;
        }
        match completions.try_recv() {
            Ok(completion) => {
                waiting.store(false, Ordering::Release);
                apply_completion(&ctx, &deliver, &state, completion)?;
                continue;
            }
            Err(sync_mpsc::TryRecvError::Disconnected) => {
                return Err(error(
                    ExecutionErrorKind::HostDisconnected,
                    "host completion sender disconnected",
                ));
            }
            Err(sync_mpsc::TryRecvError::Empty) => {}
        }
        if state.borrow().pending.is_empty() {
            return Err(error(
                ExecutionErrorKind::Javascript,
                "program is awaiting a promise with no runnable jobs or host calls",
            ));
        }
        let wait = deadline
            .saturating_duration_since(Instant::now())
            .min(IDLE_POLL);
        waiting.store(true, Ordering::Release);
        match completions.recv_timeout(wait) {
            Ok(completion) => {
                waiting.store(false, Ordering::Release);
                apply_completion(&ctx, &deliver, &state, completion)?
            }
            Err(sync_mpsc::RecvTimeoutError::Disconnected) => {
                return Err(error(
                    ExecutionErrorKind::HostDisconnected,
                    "host completion sender disconnected",
                ));
            }
            Err(sync_mpsc::RecvTimeoutError::Timeout) => {}
        }
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
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_context<'js>(
    ctx: &Ctx<'js>,
    input: &ExecutionInput,
    state: Rc<RefCell<State>>,
    fatal: Arc<AtomicBool>,
    cancellation: &Cancellation,
    deadline: Instant,
    started: Instant,
    sink: RequestSink,
) -> Result<Prepared<'js>, ExecutionError> {
    let ctx = ctx.clone();
    let send_state = state.clone();
    let send_fatal = fatal.clone();
    let send_events = sink;
    let send_limits = input.limits.clone();
    let send_cancel = cancellation.clone();
    let allowed: HashMap<_, _> = input
        .bindings
        .iter()
        .map(|binding| (binding.binding_id.clone(), binding.name.clone()))
        .collect();
    let send = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, binding_id: String, json: String| -> rquickjs::Result<String> {
            let mut state = send_state.borrow_mut();
            let failure = if let Some(reason) = stop_reason(&send_cancel, deadline) {
                Some(reason)
            } else if !allowed.contains_key(&binding_id) {
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
            if let Err(failure) = send_events.send(request, &send_cancel, deadline) {
                return Err(fail_callback(&ctx, &mut state, &send_fatal, failure));
            }
            state.calls += 1;
            state.pending.insert(request_id.clone());
            Ok(request_id)
        },
    )
    .map_err(|failure| js_error(&ctx, failure))?;
    // `ctx.setContext`: keeps the program's context for the host. An
    // oversized value is an ordinary exception the program can catch.
    let keep_state = state.clone();
    let keep = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, json: String| -> rquickjs::Result<()> {
            if json.len() > MAX_PROGRAM_CONTEXT_BYTES {
                return Err(Exception::throw_range(
                    &ctx,
                    &format!("program context exceeds {MAX_PROGRAM_CONTEXT_BYTES} bytes of JSON"),
                ));
            }
            let value = serde_json::from_str(&json)
                .map_err(|_| Exception::throw_type(&ctx, "program context is not JSON"))?;
            keep_state.borrow_mut().program_context = Some(value);
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
        .call((send, keep, catalog, program))
        .map_err(|failure| js_error(&ctx, failure))?;
    let deliver: Function = controls
        .get("deliver")
        .map_err(|failure| js_error(&ctx, failure))?;
    let serialize: Function = controls
        .get("serialize")
        .map_err(|failure| js_error(&ctx, failure))?;
    let promise = start_program(&ctx, &controls, &input.source)?;
    state.borrow_mut().startup_micros = elapsed_micros(started);
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
    state.borrow_mut().pending.remove(&completion.request_id);
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
    fatal: &AtomicBool,
    failure: ExecutionError,
) -> rquickjs::Error {
    let exception = Exception::throw_type(ctx, &failure.message);
    if state.failure.is_none() {
        state.failure = Some(failure);
    }
    fatal.store(true, Ordering::Release);
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
