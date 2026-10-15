//! A program driven one step at a time on the caller's thread, for a host
//! that is itself deterministic and replayed, such as a Temporal workflow.
//!
//! A step delivers completions one at a time, running the program until it
//! has no runnable job after each (quiescent delivery), and reports the
//! requests it issued or how it finished. Nothing here reads a clock: the
//! budget is a count of QuickJS interrupt checks, which fall at the same
//! points on every run, so the same completions in the same order always
//! give the same requests, the same result, and the same budget failure.

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use rquickjs::{Context, Ctx, Function, Persistent, Promise, Runtime, Value as JsValue};

use crate::{
    engine::{
        Prepared, State, apply_completion, error, native_error, prepare_context, settled_value,
        validate,
    },
    serialized_len, *,
};

/// What a program asks of its host after a step.
#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    /// The program waits on its outstanding requests; `requests` are those
    /// it issued during this step, in issue order.
    Waiting { requests: Vec<HostRequest> },
    /// The program returned or failed. `requests` are those it issued
    /// during this step before finishing, which the host still sends, as
    /// a threaded execution's host would. Every later step reports the
    /// same report with no requests.
    Finished {
        requests: Vec<HostRequest>,
        report: ExecutionReport,
    },
}

/// How much work one step may do, in QuickJS interrupt checks (QuickJS
/// checks roughly every ten thousand bytecode branches and calls).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StepBudget {
    pub interrupts_per_step: u64,
}

impl Default for StepBudget {
    /// The cost of an interrupt check depends on the code: in a release
    /// build two thousand take about 40 ms of a tight loop and about 1.7 s
    /// of an `await` loop, inside a workflow task's default ten seconds.
    fn default() -> Self {
        Self {
            interrupts_per_step: 2_000,
        }
    }
}

struct Controls {
    deliver: Persistent<Function<'static>>,
    serialize: Persistent<Function<'static>>,
    promise: Persistent<Promise<'static>>,
}

/// One program run, held between steps. Not `Send`: it lives on the thread
/// that steps it.
pub struct ProgramInstance {
    // Field order is drop order: saved JavaScript values go before the
    // context and runtime that own them.
    controls: Option<Controls>,
    context: Context,
    _runtime: Runtime,
    input: ExecutionInput,
    state: Rc<RefCell<State>>,
    outbox: Rc<RefCell<Vec<HostRequest>>>,
    budget: StepBudget,
    remaining: Rc<Cell<u64>>,
    exhausted: Rc<Cell<bool>>,
    finished: Option<ExecutionReport>,
}

impl ProgramInstance {
    /// Compile and start a program, running it until it first has no
    /// runnable job.
    pub fn start(input: ExecutionInput, budget: StepBudget) -> Result<(Self, Step), StartError> {
        validate(&input)?;
        if budget.interrupts_per_step == 0 {
            return Err(StartError::InvalidInput(
                "the step budget must be positive".into(),
            ));
        }
        let runtime = Runtime::new().map_err(|failure| StartError::Engine(failure.to_string()))?;
        runtime.set_memory_limit(input.limits.max_memory_bytes as usize);
        runtime.set_max_stack_size(input.limits.max_stack_bytes as usize);
        let fatal = Rc::new(Cell::new(false));
        let remaining = Rc::new(Cell::new(budget.interrupts_per_step));
        let exhausted = Rc::new(Cell::new(false));
        let handler_fatal = fatal.clone();
        let handler_remaining = remaining.clone();
        let handler_exhausted = exhausted.clone();
        runtime.set_interrupt_handler(Some(Box::new(move || {
            if handler_fatal.get() || handler_exhausted.get() {
                return true;
            }
            match handler_remaining.get() {
                0 => {
                    handler_exhausted.set(true);
                    true
                }
                left => {
                    handler_remaining.set(left - 1);
                    false
                }
            }
        })));
        let context =
            Context::full(&runtime).map_err(|failure| StartError::Engine(failure.to_string()))?;
        let mut instance = Self {
            controls: None,
            context,
            _runtime: runtime,
            input,
            state: Rc::new(RefCell::new(State::default())),
            outbox: Rc::new(RefCell::new(Vec::new())),
            budget,
            remaining,
            exhausted,
            finished: None,
        };
        let prepared = instance.context.with(|ctx| {
            let Prepared {
                deliver,
                serialize,
                promise,
            } = prepare_context(
                &ctx,
                &instance.input,
                instance.state.clone(),
                fatal,
                instance.outbox.clone(),
            )?;
            Ok::<_, ExecutionError>(Controls {
                deliver: Persistent::save(&ctx, deliver),
                serialize: Persistent::save(&ctx, serialize),
                promise: Persistent::save(&ctx, promise),
            })
        });
        let step = match prepared {
            Ok(controls) => {
                instance.controls = Some(controls);
                instance.advance(Vec::new())
            }
            Err(failure) => instance.finish(Err(failure)),
        };
        Ok((instance, step))
    }

    /// Deliver `completions` in order, one per quiescent point, and run the
    /// program as far as it can go.
    pub fn step(&mut self, completions: Vec<HostCompletion>) -> Step {
        if let Some(report) = &self.finished {
            return Step::Finished {
                requests: Vec::new(),
                report: report.clone(),
            };
        }
        self.remaining.set(self.budget.interrupts_per_step);
        self.advance(completions)
    }

    /// Whether the program has finished.
    pub fn is_finished(&self) -> bool {
        self.finished.is_some()
    }

    fn advance(&mut self, completions: Vec<HostCompletion>) -> Step {
        let Some(controls) = &self.controls else {
            return self.finish(Err(error(
                ExecutionErrorKind::Internal,
                "program instance has no controls",
            )));
        };
        let state = self.state.clone();
        let limits = self.input.limits.clone();
        let outcome = self
            .context
            .with(|ctx| -> Result<Option<Option<Value>>, ExecutionError> {
                let deliver = controls
                    .deliver
                    .clone()
                    .restore(&ctx)
                    .map_err(native_error)?;
                let serialize = controls
                    .serialize
                    .clone()
                    .restore(&ctx)
                    .map_err(native_error)?;
                let promise = controls
                    .promise
                    .clone()
                    .restore(&ctx)
                    .map_err(native_error)?;
                let settled = || -> Result<Option<Option<Value>>, ExecutionError> {
                    quiesce(&ctx, &state)?;
                    match promise.result::<JsValue>() {
                        Some(result) => settled_value(&ctx, &serialize, &limits, result).map(Some),
                        None => Ok(None),
                    }
                };
                if let Some(value) = settled()? {
                    return Ok(Some(value));
                }
                for mut completion in completions {
                    // A result too large for the program arrives as a
                    // failure in its place.
                    if serialized_len(&completion) > limits.max_result_bytes {
                        completion.outcome = Err(HostError {
                            kind: "payload_too_large".into(),
                            message: "host completion exceeds result budget".into(),
                            value: None,
                        });
                    }
                    apply_completion(&ctx, &deliver, &state, completion)?;
                    if let Some(value) = settled()? {
                        return Ok(Some(value));
                    }
                }
                if state.borrow().pending.is_empty() {
                    return Err(error(
                        ExecutionErrorKind::Javascript,
                        "script is awaiting a promise with no runnable jobs or host calls",
                    ));
                }
                Ok(None)
            });
        match outcome {
            Ok(None) => Step::Waiting {
                requests: std::mem::take(&mut *self.outbox.borrow_mut()),
            },
            Ok(Some(value)) => self.finish(Ok(value)),
            Err(failure) => self.finish(Err(failure)),
        }
    }

    fn finish(&mut self, result: Result<Option<Value>, ExecutionError>) -> Step {
        // A budget or bridge failure stays terminal even if the guest
        // caught the exception raised at the interrupt.
        let mut state = self.state.borrow_mut();
        let budget_failure = self.exhausted.get().then(|| {
            error(
                ExecutionErrorKind::LimitExceeded,
                "program exceeded its step budget",
            )
        });
        let (return_value, failure) = match result {
            Ok(value) => (value, None),
            Err(failure) => (None, Some(failure)),
        };
        let failure = budget_failure.or_else(|| state.failure.take()).or(failure);
        let report = ExecutionReport {
            return_value: if failure.is_some() {
                None
            } else {
                return_value
            },
            program_context: state.program_context.take(),
            error: failure,
            pending_request_ids: state.pending.iter().cloned().collect(),
            // Timings are left out: a replayed run must report the same.
            metrics: ExecutionMetrics {
                startup_micros: 0,
                elapsed_micros: 0,
                tool_calls: state.calls,
                pending_jobs: state.jobs,
            },
        };
        drop(state);
        let requests = std::mem::take(&mut *self.outbox.borrow_mut());
        self.controls = None;
        self.finished = Some(report.clone());
        Step::Finished { requests, report }
    }
}

/// Run jobs until none is runnable, stopping at a bridge failure.
fn quiesce(ctx: &Ctx<'_>, state: &Rc<RefCell<State>>) -> Result<(), ExecutionError> {
    loop {
        if let Some(failure) = state.borrow().failure.clone() {
            return Err(failure);
        }
        if !ctx.execute_pending_job() {
            return Ok(());
        }
        state.borrow_mut().jobs += 1;
    }
}
