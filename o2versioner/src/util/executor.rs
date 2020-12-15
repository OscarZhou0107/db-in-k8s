//! An executor to run as an individual task

use async_trait::async_trait;

/// Should spawn the `Future` returned by the `Executor::run`
/// to `Tokio::spawn`
#[async_trait]
pub trait Executor: Send + Sync {
    async fn run(mut self: Box<Self>);
}
