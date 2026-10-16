//! Program values out of workflow history (plan D7 "Long runs").
//!
//! A program's workflow needs real values (the JS runs there), but the
//! large ones are already blobs the session owns: the program's source,
//! input and context, and its tools' outputs and errors. An activity that
//! returns one of those over the inline budget puts the placeholder
//! `lightspeed-ref:<blob ref>` at its place and lists the place in the
//! payload's `lightspeed-refs` metadata. History keeps only that.
//!
//! [`ValueRefCodec`] is installed on the program worker's client. It only
//! decodes: for each listed place whose placeholder matches, it reads the
//! blob from the universe's store and puts the value back, so the workflow
//! sees the same value on the first run and on replay. It never encodes,
//! so it never decides what a blob belongs to. A worker without it cannot
//! replay these histories.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use futures::future::BoxFuture;
use harness::{BlobRef, storage::BlobStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use temporalio_common::data_converters::{
    DataConverter, DefaultFailureConverter, PayloadCodec, PayloadConversionError, PayloadConverter,
    RawValue, SerializationContextData,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use uuid::Uuid;

/// Prefix of the placeholder a referenced value leaves in the JSON.
pub const REF_PREFIX: &str = "lightspeed-ref:";
/// Payload metadata key listing a payload's references.
pub const REFS_METADATA_KEY: &str = "lightspeed-refs";
/// Largest value, in serialized bytes, that stays inline in history.
pub const DEFAULT_INLINE_VALUE_BYTES: usize = 16 * 1024;
const INLINE_VALUE_BYTES_ENV: &str = "LIGHTSPEED_PROGRAM_INLINE_VALUE_BYTES";

const ENCODING_KEY: &str = "encoding";
const JSON_ENCODING: &[u8] = b"json/plain";

static INLINE_VALUE_BYTES: LazyLock<usize> =
    LazyLock::new(|| match std::env::var(INLINE_VALUE_BYTES_ENV) {
        Ok(value) => value.trim().parse().unwrap_or_else(|_| {
            tracing::warn!(
                target: "temporal_runtime",
                %value,
                "{INLINE_VALUE_BYTES_ENV} is not a byte count; using the default"
            );
            DEFAULT_INLINE_VALUE_BYTES
        }),
        Err(_) => DEFAULT_INLINE_VALUE_BYTES,
    });

/// How a blob's bytes become the value at its place.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefKind {
    /// The blob is JSON.
    Json,
    /// The blob is UTF-8 text, the value a JSON string.
    Text,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RefEntry {
    /// JSON pointer into the payload's value.
    path: String,
    blob: BlobRef,
    kind: RefKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RefManifest {
    universe_id: Uuid,
    refs: Vec<RefEntry>,
}

fn placeholder(blob: &BlobRef) -> Value {
    Value::String(format!("{REF_PREFIX}{blob}"))
}

/// The references one activity result carries, collected as the activity
/// builds it.
pub struct ValueRefs {
    universe_id: Uuid,
    inline_max: usize,
    refs: Vec<RefEntry>,
}

impl ValueRefs {
    pub fn new(universe_id: Uuid) -> Self {
        Self::with_inline_max(universe_id, *INLINE_VALUE_BYTES)
    }

    pub fn with_inline_max(universe_id: Uuid, inline_max: usize) -> Self {
        Self {
            universe_id,
            inline_max,
            refs: Vec::new(),
        }
    }

    /// Reference `blob` at `path` if `value`, the blob's JSON content as
    /// the result holds it, is over the inline budget.
    pub fn offer_json(&mut self, path: impl Into<String>, blob: &BlobRef, value: &Value) {
        let len = serde_json::to_vec(value).map_or(0, |bytes| bytes.len());
        self.offer(path.into(), blob, RefKind::Json, len);
    }

    /// Reference `blob` at `path` if `text`, the blob's UTF-8 content as
    /// the result holds it, is over the inline budget.
    pub fn offer_text(&mut self, path: impl Into<String>, blob: &BlobRef, text: &str) {
        self.offer(path.into(), blob, RefKind::Text, text.len());
    }

    fn offer(&mut self, path: String, blob: &BlobRef, kind: RefKind, len: usize) {
        if len > self.inline_max {
            self.refs.push(RefEntry {
                path,
                blob: blob.clone(),
                kind,
            });
        }
    }

    /// The result as a JSON payload, its referenced values replaced by
    /// placeholders.
    pub fn into_payload<T: Serialize>(self, result: &T) -> Result<RawValue, String> {
        let mut value = serde_json::to_value(result).map_err(|error| error.to_string())?;
        let mut metadata = HashMap::from([(ENCODING_KEY.to_owned(), JSON_ENCODING.to_vec())]);
        if !self.refs.is_empty() {
            for entry in &self.refs {
                let slot = value
                    .pointer_mut(&entry.path)
                    .ok_or_else(|| format!("referenced value {} is missing", entry.path))?;
                *slot = placeholder(&entry.blob);
            }
            let manifest = RefManifest {
                universe_id: self.universe_id,
                refs: self.refs,
            };
            metadata.insert(
                REFS_METADATA_KEY.to_owned(),
                serde_json::to_vec(&manifest).map_err(|error| error.to_string())?,
            );
        }
        Ok(RawValue::new(vec![Payload {
            metadata,
            data: serde_json::to_vec(&value).map_err(|error| error.to_string())?,
            ..Default::default()
        }]))
    }
}

/// A universe's blob store, for the codec.
pub type UniverseBlobs =
    Arc<dyn Fn(Uuid) -> BoxFuture<'static, Result<Arc<dyn BlobStore>, String>> + Send + Sync>;

/// Puts referenced values back into payloads; see the module docs.
#[derive(Clone)]
pub struct ValueRefCodec {
    blobs: UniverseBlobs,
}

impl ValueRefCodec {
    pub fn new(blobs: UniverseBlobs) -> Self {
        Self { blobs }
    }

    /// The default data converter with this codec.
    pub fn data_converter(self) -> DataConverter {
        DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            self,
        )
    }

    async fn decode_one(&self, mut payload: Payload) -> Result<Payload, String> {
        let Some(manifest) = payload.metadata.remove(REFS_METADATA_KEY) else {
            return Ok(payload);
        };
        let manifest: RefManifest = serde_json::from_slice(&manifest)
            .map_err(|error| format!("invalid {REFS_METADATA_KEY} metadata: {error}"))?;
        let mut value: Value = serde_json::from_slice(&payload.data)
            .map_err(|error| format!("referencing payload is not JSON: {error}"))?;
        let store = (self.blobs)(manifest.universe_id).await?;
        for entry in manifest.refs {
            let slot = value
                .pointer_mut(&entry.path)
                .filter(|slot| **slot == placeholder(&entry.blob))
                .ok_or_else(|| format!("no placeholder for {} at {}", entry.blob, entry.path))?;
            let bytes = store
                .read_bytes(&entry.blob)
                .await
                .map_err(|error| format!("read {}: {error}", entry.blob))?;
            *slot = match entry.kind {
                RefKind::Json => serde_json::from_slice(&bytes)
                    .map_err(|error| format!("{} is not JSON: {error}", entry.blob))?,
                RefKind::Text => Value::String(
                    String::from_utf8(bytes)
                        .map_err(|error| format!("{} is not UTF-8: {error}", entry.blob))?,
                ),
            };
        }
        payload.data = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
        Ok(payload)
    }
}

impl PayloadCodec for ValueRefCodec {
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
            let mut decoded = Vec::with_capacity(payloads.len());
            for payload in payloads {
                decoded.push(codec.decode_one(payload).await.map_err(|message| {
                    tracing::warn!(
                        target: "temporal_runtime",
                        %message,
                        "could not put referenced values back into a payload"
                    );
                    PayloadConversionError::EncodingError(message.into())
                })?);
            }
            Ok(decoded)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness::storage::InMemoryBlobStore;
    use serde_json::json;

    fn codec(store: Arc<InMemoryBlobStore>) -> ValueRefCodec {
        ValueRefCodec::new(Arc::new(move |_| {
            let store: Arc<dyn BlobStore> = store.clone();
            Box::pin(async move { Ok(store) })
        }))
    }

    async fn decode(codec: &ValueRefCodec, raw: RawValue) -> Vec<Payload> {
        codec
            .decode(&SerializationContextData::None, raw.payloads)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn large_values_travel_as_placeholders_and_decode_back() {
        let store = Arc::new(InMemoryBlobStore::default());
        let output = json!({ "rows": "x".repeat(64) });
        let output_ref = store
            .put_bytes(serde_json::to_vec(&output).unwrap())
            .await
            .unwrap();
        let message = "boom ".repeat(20);
        let message_ref = store.put_bytes(message.clone().into_bytes()).await.unwrap();
        let small = json!({ "ok": true });
        let result = json!({
            "completions": [
                { "outcome": { "Ok": output } },
                { "outcome": { "Err": { "kind": "tool_failed", "message": message, "value": null } } },
                { "outcome": { "Ok": small } },
            ]
        });
        let universe_id = Uuid::new_v4();
        let mut refs = ValueRefs::with_inline_max(universe_id, 32);
        refs.offer_json("/completions/0/outcome/Ok", &output_ref, &output);
        refs.offer_text("/completions/1/outcome/Err/message", &message_ref, &message);
        // Under the budget: stays inline.
        refs.offer_json("/completions/2/outcome/Ok", &output_ref, &small);
        let raw = refs.into_payload(&result).unwrap();

        let encoded: Value = serde_json::from_slice(&raw.payloads[0].data).unwrap();
        assert_eq!(
            encoded["completions"][0]["outcome"]["Ok"],
            json!(format!("{REF_PREFIX}{output_ref}"))
        );
        assert_eq!(
            encoded["completions"][1]["outcome"]["Err"]["message"],
            json!(format!("{REF_PREFIX}{message_ref}"))
        );
        assert_eq!(encoded["completions"][2]["outcome"]["Ok"], small);

        let decoded = decode(&codec(store), raw).await;
        assert!(!decoded[0].metadata.contains_key(REFS_METADATA_KEY));
        assert_eq!(decoded[0].metadata[ENCODING_KEY], JSON_ENCODING);
        assert_eq!(
            serde_json::from_slice::<Value>(&decoded[0].data).unwrap(),
            result
        );
    }

    #[tokio::test]
    async fn a_result_without_references_is_plain_json() {
        let raw = ValueRefs::with_inline_max(Uuid::new_v4(), 32)
            .into_payload(&json!({ "a": 1 }))
            .unwrap();
        assert_eq!(raw.payloads[0].metadata.len(), 1);
        let store = Arc::new(InMemoryBlobStore::default());
        let decoded = decode(&codec(store), raw.clone()).await;
        assert_eq!(decoded, raw.payloads);
    }

    #[tokio::test]
    async fn only_listed_places_with_a_matching_placeholder_are_replaced() {
        let store = Arc::new(InMemoryBlobStore::default());
        let blob = store.put_bytes(b"\"real\"".to_vec()).await.unwrap();
        // A program's own data that looks like a placeholder is left alone.
        let lookalike = format!("{REF_PREFIX}{blob}");
        let mut refs = ValueRefs::with_inline_max(Uuid::new_v4(), 0);
        refs.offer_json("/listed", &blob, &json!("real"));
        let raw = refs
            .into_payload(&json!({ "listed": "real", "own": lookalike }))
            .unwrap();
        let decoded = decode(&codec(store.clone()), raw.clone()).await;
        assert_eq!(
            serde_json::from_slice::<Value>(&decoded[0].data).unwrap(),
            json!({ "listed": "real", "own": lookalike })
        );

        // A listed place without its placeholder is an error, not a guess.
        let mut tampered = raw.payloads[0].clone();
        tampered.data = serde_json::to_vec(&json!({ "listed": 1, "own": 2 })).unwrap();
        assert!(
            codec(store)
                .decode(&SerializationContextData::None, vec![tampered])
                .await
                .is_err()
        );
    }
}
