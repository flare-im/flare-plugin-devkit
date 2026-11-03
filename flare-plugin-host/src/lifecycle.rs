//! Process-local admission ownership. A cloned identity retains one admission
//! until its last owner drops; cancellation also releases it.
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tonic::Status;

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("plugin server failed")]
    Transport(#[from] tonic::transport::Error),
    #[error("plugin shutdown signal unavailable")]
    Signal(#[from] std::io::Error),
    #[error("plugin shutdown deadline reached")]
    Deadline,
}

/// Stop listener acceptance and process admission together, then wait for
/// admitted work and transport completion within one deadline. Dropping a stuck
/// server cancels incomplete handlers; their database transactions roll back.
pub async fn serve_bounded<B, F>(
    host: crate::PluginRuntimeHost,
    deadline: Duration,
    build: B,
) -> Result<(), ServeError>
where
    B: FnOnce(tokio::sync::oneshot::Receiver<()>) -> F,
    F: std::future::Future<Output = Result<(), tonic::transport::Error>>,
{
    supervise(host.lifecycle(), deadline, build, shutdown_signal()).await
}

async fn supervise<B, F, S>(
    lifecycle: Arc<Lifecycle>,
    deadline: Duration,
    build: B,
    signal: S,
) -> Result<(), ServeError>
where
    B: FnOnce(tokio::sync::oneshot::Receiver<()>) -> F,
    F: std::future::Future<Output = Result<(), tonic::transport::Error>>,
    S: std::future::Future<Output = std::io::Result<()>>,
{
    let (stop, receive) = tokio::sync::oneshot::channel();
    let server = build(receive);
    tokio::pin!(server);
    let signal_error = tokio::select! {
        result = &mut server => { lifecycle.set_draining(true); return result.map_err(ServeError::Transport); },
        result = signal => result.err(),
    };
    lifecycle.set_draining(true);
    let _ = stop.send(());
    tokio::time::timeout(deadline.min(Duration::from_secs(120)), async {
        let (result, drained) = tokio::join!(&mut server, lifecycle.drain(deadline));
        result.map_err(ServeError::Transport)?;
        drained.map_err(|_| ServeError::Deadline)?;
        Ok::<(), ServeError>(())
    })
    .await
    .map_err(|_| ServeError::Deadline)??;
    if let Some(error) = signal_error {
        return Err(ServeError::Signal(error));
    }
    Ok(())
}

/// Wait for an operator shutdown on terminals and Unix process supervisors.
/// Install this future before serving traffic, then close admission and drain.
pub async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

#[derive(Default)]
pub(crate) struct Lifecycle {
    state: Mutex<(bool, usize)>,
    idle: Notify,
}
pub(crate) struct Admission(Arc<Lifecycle>);
/// Retains a process admission for independently authenticated provider work.
/// This guard grants no tenant, user or resource permissions.
pub struct RuntimeWork {
    pub(crate) _admission: Arc<Admission>,
}
impl Lifecycle {
    pub fn is_draining(&self) -> bool {
        self.state.lock().map(|s| s.0).unwrap_or(true)
    }
    pub fn set_draining(&self, value: bool) {
        if let Ok(mut state) = self.state.lock() {
            state.0 = value;
        }
    }
    pub fn in_flight(&self) -> usize {
        self.state.lock().map(|s| s.1).unwrap_or(usize::MAX)
    }
    pub fn admit(self: &Arc<Self>) -> Result<Arc<Admission>, Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::unavailable("runtime unavailable"))?;
        if state.0 {
            return Err(Status::unavailable("plugin draining"));
        }
        if state.1 >= 1024 {
            return Err(Status::resource_exhausted("plugin concurrency limit"));
        }
        state.1 += 1;
        Ok(Arc::new(Admission(self.clone())))
    }
    pub async fn drain(&self, deadline: Duration) -> Result<(), Status> {
        self.set_draining(true);
        tokio::time::timeout(deadline.min(Duration::from_secs(120)), async {
            loop {
                let notified = self.idle.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.in_flight() == 0 {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| Status::deadline_exceeded("plugin drain deadline"))
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.1 -= 1;
            if state.1 == 0 {
                self.0.idle.notify_waiters();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn shutdown_closes_transport_before_waiting_for_work() {
        let lifecycle = Arc::new(Lifecycle::default());
        let work = lifecycle.admit().unwrap();
        let observer = lifecycle.clone();
        supervise(
            lifecycle.clone(),
            Duration::from_secs(1),
            move |receive| async move {
                receive.await.unwrap();
                assert!(observer.admit().is_err());
                assert_eq!(observer.in_flight(), 1);
                drop(work);
                Ok(())
            },
            async { Ok(()) },
        )
        .await
        .unwrap();
        assert!(lifecycle.is_draining());
        assert_eq!(lifecycle.in_flight(), 0);
    }
    #[tokio::test]
    async fn shutdown_deadline_cancels_stuck_transport() {
        let lifecycle = Arc::new(Lifecycle::default());
        let work = lifecycle.admit().unwrap();
        assert!(matches!(
            supervise(
                lifecycle.clone(),
                Duration::ZERO,
                move |_| async move {
                    let _work = work;
                    std::future::pending::<()>().await;
                    Ok(())
                },
                async { Ok(()) }
            )
            .await,
            Err(ServeError::Deadline)
        ));
        assert!(lifecycle.is_draining());
        assert_eq!(lifecycle.in_flight(), 0);
    }
}
