use std::{
    future::Future,
    io::{ErrorKind, Result},
    time::Duration,
};

use async_io::Timer;
use futures_lite::future;

pub async fn io_timeout<T>(dur: Duration, f: impl Future<Output = Result<T>>) -> Result<T> {
    future::race(f, async move {
        Timer::after(dur).await;
        Err(ErrorKind::TimedOut.into())
    })
    .await
}
