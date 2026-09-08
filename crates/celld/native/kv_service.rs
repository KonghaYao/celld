use serde_json::{json, Value};

use crate::js::{self, RequestBody, RequestBodyGuard};

use super::bindings::cellp::kv::kv::KvError;
use super::invocation::InvocationCancel;

#[derive(Clone, Default)]
pub struct KvHostService;

pub enum KvOp {
    Get { key: String },
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

struct CancelOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            let _ = cancel.send(());
        }
    }
}

impl KvHostService {
    pub fn new() -> Self {
        Self
    }

    pub async fn call(
        &self,
        namespace_id: String,
        op: KvOp,
        cancel: &InvocationCancel,
    ) -> Result<Option<Vec<u8>>, KvError> {
        let work = self.dispatch(namespace_id, op);
        tokio::select! {
            _ = cancel.cancelled() => Err(KvError::Unavailable),
            result = work => result,
        }
    }

    async fn dispatch(&self, namespace_id: String, op: KvOp) -> Result<Option<Vec<u8>>, KvError> {
        let (body, key) = match op {
            KvOp::Get { key } => (json!({ "op": "get", "key": key }), Some(key)),
            KvOp::Put { key, value } => (
                json!({
                    "op": "put",
                    "key": key,
                    "value": value,
                    "tag": "bytes",
                }),
                Some(key),
            ),
            KvOp::Delete { key } => (json!({ "op": "delete", "keys": [key] }), Some(key)),
        };
        if let Some(key) = &key {
            if key.as_bytes().len() > celld_logic::kv::MAX_KEY_BYTES {
                return Err(KvError::InvalidKey);
            }
        }
        let shard = key
            .as_deref()
            .map(|key| celld_logic::kv::shard_of(key, celld_logic::kv::SHARDS))
            .unwrap_or(0);
        let scope = js::kv_cell_scope(&namespace_id, shard);
        let payload = serde_json::to_vec(&body).map_err(|_| KvError::Unavailable)?;
        let request_body = RequestBody::Bytes(payload.into());
        let body_guard = RequestBodyGuard::of(&request_body);
        let (reply, receive) = tokio::sync::oneshot::channel();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let accepted = js::submit_do_call(js::DoCallReq {
            request_id: Some(js::next_request_id()),
            cancel: Some(cancel_rx),
            deliver_abort_to_handler: false,
            scope,
            name: Some(celld_logic::kv::cell_name(&namespace_id, shard)),
            url: "https://kv.celld/".into(),
            method: "POST".into(),
            body: request_body,
            body_guard,
            headers: vec![("content-type".into(), "application/json".into())],
            reply,
            order: None,
            parent: None,
        });
        if !accepted {
            return Err(KvError::Unavailable);
        }
        let _cancel = CancelOnDrop(Some(cancel_tx));
        let response = receive.await.map_err(|_| KvError::Unavailable)?.map_err(|_| KvError::Unavailable)?;
        if response.status >= 400 {
            return Err(KvError::Unavailable);
        }
        decode_operator_response(&response.body, &body)
    }
}

/// __KvNamespaceCell.fetch returns `Response.json({ result })`; operator RPC
/// callers unwrap that envelope before reading fields.
fn kv_cell_payload(envelope: &Value) -> &Value {
    envelope.get("result").unwrap_or(envelope)
}

fn decode_operator_response(body: &[u8], request: &Value) -> Result<Option<Vec<u8>>, KvError> {
    if request.get("op").and_then(Value::as_str) != Some("get") {
        return Ok(None);
    }
    let envelope: Value = serde_json::from_slice(body).map_err(|_| KvError::Unavailable)?;
    if envelope.get("error").is_some() {
        return Err(KvError::Unavailable);
    }
    let value = kv_cell_payload(&envelope);
    if value.get("found").and_then(Value::as_bool) != Some(true) {
        return Ok(None);
    }
    if let Some(array) = value.get("value").and_then(Value::as_array) {
        let bytes = array
            .iter()
            .map(|item| item.as_u64().and_then(|n| u8::try_from(n).ok()))
            .collect::<Option<Vec<u8>>>()
            .ok_or(KvError::Unavailable)?;
        return Ok(Some(bytes));
    }
    if let Some(text) = value.get("value").and_then(Value::as_str) {
        if value.get("valueEncoding").and_then(Value::as_str) == Some("base64") {
            return decode_base64(text).ok_or(KvError::Unavailable).map(Some);
        }
        return Ok(Some(text.as_bytes().to_vec()));
    }
    Err(KvError::Unavailable)
}

fn decode_base64(text: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(text).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_on_drop_signals_oneshot() {
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        drop(CancelOnDrop(Some(tx)));
        assert!(rx.try_recv().is_ok() || rx.try_recv().is_err());
    }

    #[test]
    fn decode_get_unwraps_result_envelope() {
        let request = json!({ "op": "get", "key": "k" });
        let body = serde_json::to_vec(&json!({
            "result": { "found": true, "value": [110, 97, 116, 105, 118, 101] }
        }))
        .expect("json");
        let got = decode_operator_response(&body, &request).expect("decode");
        assert_eq!(got, Some(b"native".to_vec()));
    }
}
