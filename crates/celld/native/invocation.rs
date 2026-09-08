use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use crate::js;

use super::contract::NativeError;

#[derive(Clone, Default)]
pub struct InvocationCancel {
    inner: Arc<AtomicBool>,
}

impl InvocationCancel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.inner.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.load(Ordering::SeqCst)
    }

    pub async fn cancelled(&self) {
        while !self.is_cancelled() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

pub async fn run_with_client_cancellation<T, F>(
    request_id: js::RequestId,
    cancellation_grace: Duration,
    cancel: InvocationCancel,
    work: F,
) -> Result<T, NativeError>
where
    F: std::future::Future<Output = Result<T, NativeError>> + Send + 'static,
    T: Send + 'static,
{
    let handle = tokio::spawn(work);
    let abort_handle = handle.abort_handle();
    let abort = async {
        loop {
            if js::take_request_cancellation(Some(request_id)) || cancel.is_cancelled() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        result = handle => match result {
            Ok(result) => result,
            Err(_) => Err(NativeError::cancelled()),
        },
        _ = abort => {
            cancel.cancel();
            abort_handle.abort();
            Err(NativeError::cancelled())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn client_abort_stops_within_cancellation_grace() {
        let request = js::next_request_id();
        let cancel = InvocationCancel::new();
        let started = Instant::now();
        let work = async {
            loop {
                tokio::task::yield_now().await;
            }
            #[allow(unreachable_code)]
            Ok::<(), NativeError>(())
        };
        let runner = tokio::spawn(run_with_client_cancellation(
            request,
            Duration::from_millis(250),
            cancel,
            work,
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        js::abort_request(request);
        let result = runner.await.expect("join");
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(result.is_err());
    }
}
