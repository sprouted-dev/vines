#![allow(dead_code)]

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use vines::frame::Frame;
use vines::sink::{Ack, DurableSink, SinkError};

pub fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("test limits are non-zero")
}

pub const GRACE: Duration = Duration::from_secs(1);

/// Let spawned tasks run until they are all waiting.
pub async fn settle() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}

/// A sink recording every frame, optionally slow, optionally failing at one
/// frame index, optionally panicking.
#[derive(Clone)]
pub struct MemorySink<P> {
    pub frames: Arc<Mutex<Vec<Frame<P>>>>,
    pub delay: Option<Duration>,
    pub fail_at: Option<usize>,
    pub panic_at: Option<usize>,
}

impl<P> MemorySink<P> {
    pub fn new() -> Self {
        Self {
            frames: Arc::new(Mutex::new(Vec::new())),
            delay: None,
            fail_at: None,
            panic_at: None,
        }
    }

    pub fn frames(&self) -> Vec<Frame<P>>
    where
        P: Clone,
    {
        self.frames.lock().unwrap().clone()
    }
}

impl<P: Send + 'static> DurableSink<P> for MemorySink<P> {
    async fn accept(&mut self, frame: Frame<P>) -> Result<Ack, SinkError> {
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }
        let index = self.frames.lock().unwrap().len();
        if self.panic_at == Some(index) {
            panic!("sink panicked at frame {index}");
        }
        if self.fail_at == Some(index) {
            return Err(SinkError::new(format!("disk full at frame {index}")));
        }
        self.frames.lock().unwrap().push(frame);
        Ok(Ack::accepted())
    }
}
