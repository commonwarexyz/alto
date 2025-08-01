use futures::{
    channel::oneshot,
    task::{Context, Poll},
};
use std::pin::Pin;

/// A future that manages the state of a [oneshot::Sender].
pub struct OneshotClosedFut<'a, T> {
    sender: &'a mut oneshot::Sender<T>,
}

impl<'a, T> OneshotClosedFut<'a, T> {
    /// Create a new [OneshotClosedFut].
    pub fn new(sender: &'a mut oneshot::Sender<T>) -> Self {
        Self { sender }
    }
}

impl<T> futures::Future for OneshotClosedFut<'_, T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Use poll_canceled to check if the receiver has dropped the channel
        match self.sender.poll_canceled(cx) {
            Poll::Ready(()) => Poll::Ready(()), // Receiver dropped, channel closed
            Poll::Pending => Poll::Pending,     // Channel still open
        }
    }
}
