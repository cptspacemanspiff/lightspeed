//! Live proof for keeping program values out of history (plan D7 "Long
//! runs"): an activity may return a *reference* payload, history keeps only
//! the reference, and a decode-only worker codec hands the workflow the
//! value. Checked for a local activity (its result lives in a marker) and a
//! regular activity, on the first run and on replay.
//!
//! Needs only Temporal:
//!
//! ```bash
//! source scripts/dev/env.sh
//! cargo test -p temporal-runtime --test payload_codec_live -- --ignored
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use temporal_workflow::{DEFAULT_TEMPORAL_NAMESPACE, DEFAULT_TEMPORAL_TARGET};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_common::data_converters::{
    DataConverter, DefaultFailureConverter, PayloadCodec, PayloadConversionError, PayloadConverter,
    RawValue, SerializationContextData,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use temporalio_sdk::workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions};
use temporalio_sdk::{
    ActivityOptions, LocalActivityOptions, WorkerOptions, WorkflowContext, WorkflowResult,
};

const REF_ENCODING: &[u8] = b"binary/lightspeed-blob-ref";
const PROBE: &str = "payload-codec-probe;";

/// Stands in for the content store: reference payloads carry a key, and
/// decoding swaps in the stored JSON. Encoding passes everything through.
#[derive(Clone, Default)]
struct RefCodec {
    blobs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    decoded: Arc<AtomicUsize>,
}

impl PayloadCodec for RefCodec {
    fn encode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        Box::pin(async move { Ok(payloads) })
    }

    fn decode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        let codec = self.clone();
        Box::pin(async move {
            payloads
                .into_iter()
                .map(|payload| {
                    if payload.metadata.get("encoding").map(Vec::as_slice) != Some(REF_ENCODING) {
                        return Ok(payload);
                    }
                    let key = String::from_utf8_lossy(&payload.data).into_owned();
                    let data = codec
                        .blobs
                        .lock()
                        .unwrap()
                        .get(&key)
                        .cloned()
                        .ok_or_else(|| {
                            PayloadConversionError::EncodingError(format!("no blob {key}").into())
                        })?;
                    codec.decoded.fetch_add(1, Ordering::SeqCst);
                    Ok(Payload {
                        metadata: HashMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
                        data,
                        ..Default::default()
                    })
                })
                .collect()
        })
    }
}

fn data_converter(codec: RefCodec) -> DataConverter {
    DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter::default(),
        codec,
    )
}

/// What the workflow declares: a JSON value.
pub struct SpikeDefinitions;

#[activities]
impl SpikeDefinitions {
    #[activity(name = "payload_codec_live::fetch")]
    pub async fn fetch(
        _ctx: ActivityContext,
        _key: String,
    ) -> Result<serde_json::Value, ActivityError> {
        unimplemented!("workflow activity definition only")
    }
}

/// What the worker runs: a reference to the value, never the value.
pub struct SpikeActivities;

#[activities]
impl SpikeActivities {
    #[activity(name = "payload_codec_live::fetch")]
    pub async fn fetch(
        self: Arc<Self>,
        _ctx: ActivityContext,
        key: String,
    ) -> Result<RawValue, ActivityError> {
        Ok(RawValue::new(vec![Payload {
            metadata: HashMap::from([("encoding".to_owned(), REF_ENCODING.to_vec())]),
            data: key.into_bytes(),
            ..Default::default()
        }]))
    }
}

/// What the workflow saw, small enough to keep its own result out of the
/// way of the history check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seen {
    local_len: usize,
    remote_len: usize,
    same: bool,
}

#[workflow]
#[derive(Default)]
pub struct PayloadCodecSpikeWorkflow;

#[workflow_methods]
impl PayloadCodecSpikeWorkflow {
    #[run(name = "PayloadCodecSpikeWorkflow")]
    pub async fn run(ctx: &mut WorkflowContext<Self>, key: String) -> WorkflowResult<Seen> {
        let local = ctx
            .execute_local_activity(
                SpikeDefinitions::fetch,
                key.clone(),
                LocalActivityOptions::builder()
                    .start_to_close_timeout(Duration::from_secs(5))
                    .build(),
            )
            .await
            .map_err(|error| {
                temporalio_sdk::ApplicationFailure::new(anyhow::anyhow!("local fetch: {error}"))
            })?;
        let remote = ctx
            .execute_activity(
                SpikeDefinitions::fetch,
                key,
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5)).build(),
            )
            .await
            .map_err(|error| {
                temporalio_sdk::ApplicationFailure::new(anyhow::anyhow!("fetch: {error}"))
            })?;
        let len =
            |value: &serde_json::Value| value["text"].as_str().map(str::len).unwrap_or_default();
        Ok(Seen {
            local_len: len(&local),
            remote_len: len(&remote),
            same: local == remote,
        })
    }
}

async fn connect(codec: RefCodec) -> anyhow::Result<Client> {
    let target = std::env::var("TEMPORAL_TARGET")
        .or_else(|_| std::env::var("LIGHTSPEED_TEMPORAL_TARGET"))
        .unwrap_or_else(|_| DEFAULT_TEMPORAL_TARGET.to_owned());
    let namespace = std::env::var("TEMPORAL_NAMESPACE")
        .unwrap_or_else(|_| DEFAULT_TEMPORAL_NAMESPACE.to_owned());
    let url = if target.contains("://") {
        target
    } else {
        format!("http://{target}")
    };
    let connection =
        Connection::connect(ConnectionOptions::new(url.parse::<url::Url>()?).build()).await?;
    Ok(Client::new(
        connection,
        ClientOptions::new(namespace)
            .data_converter(data_converter(codec))
            .build(),
    )?)
}

fn has_probe(payload: &Payload) -> bool {
    payload
        .data
        .windows(PROBE.len())
        .any(|window| window == PROBE.as_bytes())
}

fn is_reference(payload: &Payload) -> bool {
    payload.metadata.get("encoding").map(Vec::as_slice) == Some(REF_ENCODING)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the local Temporal stack (source scripts/dev/env.sh)"]
async fn a_reference_result_stays_out_of_history_and_decodes_on_replay() -> anyhow::Result<()> {
    let codec = RefCodec::default();
    let key = format!("blob-{}", uuid::Uuid::new_v4().simple());
    let value = serde_json::json!({ "text": PROBE.repeat(10_000) });
    let value_len = value["text"].as_str().unwrap().len();
    codec
        .blobs
        .lock()
        .unwrap()
        .insert(key.clone(), serde_json::to_vec(&value)?);
    let client = connect(codec.clone()).await?;

    let queue = format!("payload-codec-live-{}", uuid::Uuid::new_v4().simple());
    let runtime = temporal_runtime::worker::worker_runtime()?;
    let mut worker = temporalio_sdk::Worker::new(
        &runtime,
        client.clone(),
        WorkerOptions::new(queue.clone())
            .register_workflow::<PayloadCodecSpikeWorkflow>()?
            .register_activities(SpikeActivities)
            .build(),
    )?;
    let shutdown = worker.shutdown_handle();
    let run = worker.run();
    tokio::pin!(run);
    let workflow_id = format!("payload-codec-live-{}", uuid::Uuid::new_v4().simple());
    let body = async {
        let handle = client
            .start_workflow(
                PayloadCodecSpikeWorkflow::run,
                key.clone(),
                WorkflowStartOptions::new(queue.clone(), workflow_id.clone()).build(),
            )
            .await?;
        let seen = tokio::time::timeout(
            Duration::from_secs(30),
            handle.get_result(WorkflowGetResultOptions::default()),
        )
        .await??;
        anyhow::Ok((handle, seen))
    };
    let (handle, seen) = tokio::select! {
        result = body => result?,
        result = &mut run => anyhow::bail!("worker stopped early: {result:?}"),
    };
    shutdown();
    let _ = run.await;

    // First run: the workflow saw the value through both kinds of activity.
    assert_eq!(
        seen,
        Seen {
            local_len: value_len,
            remote_len: value_len,
            same: true,
        }
    );
    let first_run_decodes = codec.decoded.load(Ordering::SeqCst);
    assert!(
        first_run_decodes >= 2,
        "decodes on first run: {first_run_decodes}"
    );

    // History holds the references, never the value.
    let events = handle
        .fetch_history(Default::default())
        .into_events()
        .await?;
    let mut marker_refs = 0;
    let mut activity_refs = 0;
    for event in &events {
        match &event.attributes {
            Some(Attributes::MarkerRecordedEventAttributes(marker)) => {
                for payload in marker.details.values().flat_map(|p| &p.payloads) {
                    assert!(!has_probe(payload), "value in a local-activity marker");
                    marker_refs += usize::from(is_reference(payload));
                }
            }
            Some(Attributes::ActivityTaskCompletedEventAttributes(completed)) => {
                for payload in completed.result.iter().flat_map(|p| &p.payloads) {
                    assert!(!has_probe(payload), "value in an activity result");
                    activity_refs += usize::from(is_reference(payload));
                }
            }
            _ => {}
        }
    }
    assert_eq!((marker_refs, activity_refs), (1, 1));

    // Replay: a fresh replayer with the codec decodes the references from
    // history again and the workflow takes the same path.
    WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .data_converter(data_converter(codec.clone()))
            .register_workflow::<PayloadCodecSpikeWorkflow>()?
            .build(),
    )?
    .replay_workflow(handle.fetch_history(Default::default()))
    .await?;
    let replay_decodes = codec.decoded.load(Ordering::SeqCst) - first_run_decodes;
    assert!(replay_decodes >= 2, "decodes on replay: {replay_decodes}");

    // Without the codec a replayer cannot read the history: every worker
    // that runs these workflows must install it.
    let without_codec = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .register_workflow::<PayloadCodecSpikeWorkflow>()?
            .build(),
    )?
    .replay_workflow(handle.fetch_history(Default::default()))
    .await;
    eprintln!("replay without codec: {without_codec:?}");
    assert!(
        without_codec.is_err(),
        "a replay without the codec succeeded"
    );
    Ok(())
}
