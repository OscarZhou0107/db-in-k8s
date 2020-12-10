use async_trait::async_trait;

#[async_trait]
pub trait Executor: Send + Sync {
    async fn run(mut self: Box<Self>);
}
