use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::info;

/// Trait that a `Request` must implement in order to use `ExecutorAddr`
pub trait ExecutorRequest {
    type ReplyType;
}

/// Alias for the receiver channel of `RequestWrapper<Request>`
pub type RequestReceiver<Request> = mpsc::Receiver<RequestWrapper<Request>>;

/// Wrapper of `Request` which also carries the sender channel for `Request::ReplyType`
#[derive(Debug)]
pub struct RequestWrapper<Request>
where
    Request: ExecutorRequest,
{
    payload: Request,
    /// A single use reply channel
    reply: Option<oneshot::Sender<Request::ReplyType>>,
}

impl<Request> RequestWrapper<Request>
where
    Request: ExecutorRequest,
{
    /// Returns a ref to the internal `Request`
    pub fn request(&self) -> &Request {
        &self.payload
    }

    /// Takes out both the actual `Request` and also the sender channel for `Request::ReplyType`
    pub fn unwrap(self) -> (Request, Option<oneshot::Sender<Request::ReplyType>>) {
        (self.payload, self.reply)
    }
}

/// Encloses a way to talk to the executor of the `Request`
#[derive(Debug)]
pub struct ExecutorAddr<Request>
where
    Request: ExecutorRequest,
{
    request_tx: mpsc::Sender<RequestWrapper<Request>>,
}

impl<Request> Clone for ExecutorAddr<Request>
where
    Request: ExecutorRequest,
{
    fn clone(&self) -> Self {
        Self {
            request_tx: self.request_tx.clone(),
        }
    }
}

impl<Request> ExecutorAddr<Request>
where
    Request: ExecutorRequest,
{
    /// Create a new `ExecutorAddr` and the receiver channel of `RequestWrapper<Request>`
    pub fn new(queue_size: usize) -> (Self, RequestReceiver<Request>) {
        let (tx, rx) = mpsc::channel(queue_size);
        (Self { request_tx: tx }, rx)
    }

    /// Send a `Request` to the executor, and waits for a `Request::ReplyType`
    pub async fn request(&self, request: Request) -> Result<Request::ReplyType, String> {
        // Create a reply oneshot channel
        let (tx, rx) = oneshot::channel();

        // Construct the request to sent
        let req_wrapper = RequestWrapper {
            payload: request,
            reply: Some(tx),
        };

        // Send the request
        self.request_tx.send(req_wrapper).await.map_err(|e| e.to_string())?;

        // Wait for the reply
        rx.await.map_err(|e| e.to_string())
    }
}

impl<Request> Drop for ExecutorAddr<Request>
where
    Request: ExecutorRequest,
{
    fn drop(&mut self) {
        info!("Dropping ExecutorAddr");
    }
}
