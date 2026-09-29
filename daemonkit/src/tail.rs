//! Client-side tailing: live structured events over the authenticated control
//! channel, and file-based log tailing that works even when the daemon is
//! dead or too old to stream events.

use crate::{
    events::Event,
    native::lifecycle::NativeController,
    protocol::{read_event_frame, WireEvent},
    EndReason, Error, GenerationId,
};
use futures_core::Stream;
use std::{
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::mpsc;

/// One item of an [`EventStream`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventItem {
    Event(Event),
    /// The daemon shed `dropped` events because this subscriber fell behind.
    Lagged {
        dropped: u64,
    },
    /// The generation ended; the stream is waiting for a replacement.
    Detached {
        reason: EndReason,
    },
    /// A new generation appeared and the stream reattached to it.
    GenerationChanged {
        from: GenerationId,
        to: GenerationId,
    },
    /// The stream is over; `None` follows.
    Ended {
        reason: EndReason,
    },
}

/// Builder for a live event subscription. Defaults: no replay, follow, every
/// event, reattach across restarts.
#[derive(Clone)]
pub struct EventTail {
    controller: Arc<NativeController>,
    replay: u32,
    follow: bool,
    filter: String,
    reconnect: bool,
    poll: Duration,
}

impl EventTail {
    pub(crate) fn new(controller: Arc<NativeController>) -> Self {
        Self {
            controller,
            replay: 0,
            follow: true,
            filter: String::new(),
            reconnect: true,
            poll: Duration::from_millis(250),
        }
    }

    /// Newest retained events to deliver before live ones.
    pub fn replay(mut self, events: u32) -> Self {
        self.replay = events;
        self
    }

    /// Keep streaming after the replay. `false` ends after the replay.
    pub fn follow(mut self, follow: bool) -> Self {
        self.follow = follow;
        self
    }

    /// `tracing`-style directives such as `info,my_crate=debug`. Applied by
    /// the daemon; it never changes what the daemon records.
    pub fn filter(mut self, directives: impl Into<String>) -> Self {
        self.filter = directives.into();
        self
    }

    /// When following, wait for a replacement generation after the current
    /// one ends instead of finishing the stream.
    pub fn reconnect(mut self, reconnect: bool) -> Self {
        self.reconnect = reconnect;
        self
    }

    /// How often to look for a replacement generation while detached.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll = interval.max(Duration::from_millis(10));
        self
    }

    pub async fn open(self) -> Result<EventStream, Error> {
        let instance = self
            .controller
            .attach()
            .await?
            .ok_or(Error::Unavailable { generation: None })?;
        let stream = self
            .controller
            .open_event_stream(&instance.record, self.replay, self.follow, &self.filter)
            .await?;
        let (sender, receiver) = mpsc::channel(256);
        let generation = instance.record.generation;
        let task = tokio::spawn(self.run(stream, generation, sender));
        Ok(EventStream {
            receiver,
            task: Some(task),
        })
    }

    async fn run(
        self,
        mut stream: crate::native::transport::NativeStream,
        mut generation: GenerationId,
        sender: mpsc::Sender<Result<EventItem, Error>>,
    ) {
        loop {
            let reason = loop {
                match read_event_frame(&mut stream).await {
                    Ok(WireEvent::Event(event)) => {
                        if sender.send(Ok(EventItem::Event(event))).await.is_err() {
                            return;
                        }
                    }
                    Ok(WireEvent::Lagged { dropped }) => {
                        if sender
                            .send(Ok(EventItem::Lagged { dropped }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(WireEvent::End { reason }) => break reason,
                    Err(_) => break EndReason::Disconnected,
                }
            };
            let reattach = self.follow && self.reconnect && !matches!(reason, EndReason::Replayed);
            if !reattach {
                let _ = sender.send(Ok(EventItem::Ended { reason })).await;
                return;
            }
            if sender
                .send(Ok(EventItem::Detached { reason }))
                .await
                .is_err()
            {
                return;
            }
            // Wait for a different generation to publish a record and accept
            // an authenticated subscription. The old generation may linger in
            // Stopping for a while; its record still names the old generation.
            let next = loop {
                tokio::time::sleep(self.poll).await;
                let record = match self.controller.read_record() {
                    Ok(Some(record)) if record.generation != generation => record,
                    Ok(_) => continue,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                match self
                    .controller
                    .open_event_stream(&record, u32::MAX, true, &self.filter)
                    .await
                {
                    Ok(stream) => break (record.generation, stream),
                    Err(Error::Unsupported) | Err(Error::Rejected { .. }) => {
                        let _ = sender.send(Err(Error::Unsupported)).await;
                        return;
                    }
                    Err(_) => continue,
                }
            };
            let from = generation;
            generation = next.0;
            stream = next.1;
            if sender
                .send(Ok(EventItem::GenerationChanged {
                    from,
                    to: generation,
                }))
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

impl std::fmt::Debug for EventTail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventTail")
            .field("replay", &self.replay)
            .field("follow", &self.follow)
            .field("filter", &self.filter)
            .field("reconnect", &self.reconnect)
            .finish_non_exhaustive()
    }
}

/// Live events from a daemon. Dropping it closes the subscription.
pub struct EventStream {
    receiver: mpsc::Receiver<Result<EventItem, Error>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl EventStream {
    pub async fn next(&mut self) -> Option<Result<EventItem, Error>> {
        self.receiver.recv().await
    }
}

impl Stream for EventStream {
    type Item = Result<EventItem, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl std::fmt::Debug for EventStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventStream").finish_non_exhaustive()
    }
}

/// Which persistent log file to tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LogSource {
    /// Rendered daemonkit events (`daemon.log`), written by the daemon's hub.
    Daemon,
    /// The daemon process's own standard output.
    Stdout,
    /// The daemon process's own standard error.
    Stderr,
    /// A managed child's captured standard output.
    ManagedStdout,
    /// A managed child's captured standard error.
    ManagedStderr,
}

impl LogSource {
    fn path_in(self, generation_directory: &std::path::Path) -> PathBuf {
        generation_directory.join(match self {
            Self::Daemon => "daemon.log",
            Self::Stdout => "stdout.log",
            Self::Stderr => "stderr.log",
            Self::ManagedStdout => "managed-stdout.log",
            Self::ManagedStderr => "managed-stderr.log",
        })
    }
}

/// One item of a [`LogStream`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LogItem {
    /// Raw bytes appended to the file; may end mid-line.
    Chunk {
        generation: GenerationId,
        bytes: Vec<u8>,
    },
    /// The instance record now names a different generation; subsequent
    /// chunks come from its files, read from the start.
    GenerationChanged {
        from: GenerationId,
        to: GenerationId,
    },
}

/// Builder for file-based tailing. Needs no live daemon: it reads the
/// generation's log files named by the discovery record, or the newest
/// retained generation's files when no record exists (after a stop, crash
/// repair, or before the first start completes).
#[derive(Clone)]
pub struct LogTail {
    controller: Arc<NativeController>,
    source: LogSource,
    last_lines: Option<usize>,
    follow: bool,
    poll: Duration,
}

const TAIL_SCAN_BYTES: u64 = 1024 * 1024;
const READ_CHUNK_BYTES: usize = 64 * 1024;

impl LogTail {
    pub(crate) fn new(controller: Arc<NativeController>) -> Self {
        Self {
            controller,
            source: LogSource::Daemon,
            last_lines: None,
            follow: false,
            poll: Duration::from_millis(200),
        }
    }

    pub fn source(mut self, source: LogSource) -> Self {
        self.source = source;
        self
    }

    /// Start `lines` lines before the current end instead of at the start.
    pub fn last_lines(mut self, lines: usize) -> Self {
        self.last_lines = Some(lines);
        self
    }

    /// Keep polling for appended bytes and generation changes.
    pub fn follow(mut self, follow: bool) -> Self {
        self.follow = follow;
        self
    }

    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll = interval.max(Duration::from_millis(10));
        self
    }

    pub fn open(self) -> LogStream {
        let (sender, receiver) = mpsc::channel(64);
        let task = tokio::spawn(self.run(sender));
        LogStream {
            receiver,
            task: Some(task),
        }
    }

    async fn run(self, sender: mpsc::Sender<Result<LogItem, Error>>) {
        struct Current {
            generation: GenerationId,
            path: PathBuf,
            offset: u64,
        }
        let mut current: Option<Current> = None;
        let mut first = true;
        loop {
            // A live record wins; otherwise fall back to the newest retained
            // generation so logs stay readable after a stop or crash.
            let located = match self.controller.read_record() {
                Ok(Some(record)) => Some((record.generation, record.generation_directory)),
                Ok(None) if current.is_none() => match self.controller.newest_generation() {
                    Ok(newest) => newest,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                },
                Ok(None) => None,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };
            if let Some((generation, directory)) = located {
                let changed = current
                    .as_ref()
                    .map_or(true, |current| current.generation != generation);
                if changed {
                    let path = self.source.path_in(&directory);
                    let offset = match (first, self.last_lines) {
                        (true, Some(lines)) => tail_start(&path, lines),
                        _ => 0,
                    };
                    if let Some(previous) = &current {
                        if sender
                            .send(Ok(LogItem::GenerationChanged {
                                from: previous.generation,
                                to: generation,
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    current = Some(Current {
                        generation,
                        path,
                        offset,
                    });
                }
                first = false;
            }
            if let Some(state) = current.as_mut() {
                loop {
                    let (bytes, length) = read_from(&state.path, state.offset);
                    if length < state.offset {
                        // Truncated or replaced: start over from the top.
                        state.offset = 0;
                        continue;
                    }
                    if bytes.is_empty() {
                        break;
                    }
                    state.offset += bytes.len() as u64;
                    if sender
                        .send(Ok(LogItem::Chunk {
                            generation: state.generation,
                            bytes,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            if !self.follow {
                return;
            }
            tokio::time::sleep(self.poll).await;
        }
    }
}

/// Byte offset at which the last `lines` lines of `path` begin, scanning at
/// most `TAIL_SCAN_BYTES` from the end.
fn tail_start(path: &std::path::Path, lines: usize) -> u64 {
    let Ok(mut file) = std::fs::File::open(path) else {
        return 0;
    };
    let Ok(length) = file.metadata().map(|metadata| metadata.len()) else {
        return 0;
    };
    if lines == 0 {
        return length;
    }
    let scan = length.min(TAIL_SCAN_BYTES);
    let start = length - scan;
    if file.seek(SeekFrom::Start(start)).is_err() {
        return 0;
    }
    let mut buffer = Vec::with_capacity(scan as usize);
    if file.read_to_end(&mut buffer).is_err() {
        return 0;
    }
    // A trailing newline terminates the last line rather than starting an
    // empty one.
    let mut remaining = lines;
    let mut end = buffer.len();
    if buffer.last() == Some(&b'\n') {
        end -= 1;
    }
    for index in (0..end).rev() {
        if buffer[index] == b'\n' {
            remaining -= 1;
            if remaining == 0 {
                return start + index as u64 + 1;
            }
        }
    }
    start
}

/// Read up to one chunk from `offset`, returning the bytes and the file's
/// current length (zero when the file is missing).
fn read_from(path: &std::path::Path, offset: u64) -> (Vec<u8>, u64) {
    let Ok(mut file) = std::fs::File::open(path) else {
        return (Vec::new(), 0);
    };
    let length = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    if length <= offset || file.seek(SeekFrom::Start(offset)).is_err() {
        return (Vec::new(), length);
    }
    let mut buffer = vec![0; ((length - offset) as usize).min(READ_CHUNK_BYTES)];
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
    buffer.truncate(filled);
    (buffer, length)
}

impl std::fmt::Debug for LogTail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogTail")
            .field("source", &self.source)
            .field("last_lines", &self.last_lines)
            .field("follow", &self.follow)
            .finish_non_exhaustive()
    }
}

/// Bytes appended to a log file. Dropping it stops polling.
pub struct LogStream {
    receiver: mpsc::Receiver<Result<LogItem, Error>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl LogStream {
    pub async fn next(&mut self) -> Option<Result<LogItem, Error>> {
        self.receiver.recv().await
    }
}

impl Stream for LogStream {
    type Item = Result<LogItem, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for LogStream {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl std::fmt::Debug for LogStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogStream").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn tail_start_counts_lines_from_the_end() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("log");
        std::fs::write(&path, b"one\ntwo\nthree\n").unwrap();
        assert_eq!(tail_start(&path, 1), 8);
        assert_eq!(tail_start(&path, 2), 4);
        assert_eq!(tail_start(&path, 3), 0);
        assert_eq!(tail_start(&path, 50), 0);
        assert_eq!(tail_start(&path, 0), 14);
        std::fs::write(&path, b"one\ntwo\nthr").unwrap();
        assert_eq!(tail_start(&path, 1), 8);
        assert_eq!(tail_start(&directory.path().join("missing"), 3), 0);
    }

    #[test]
    fn read_from_reports_length_for_truncation_detection() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("log");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(b"hello world").unwrap();
        assert_eq!(read_from(&path, 6), (b"world".to_vec(), 11));
        assert_eq!(read_from(&path, 11), (Vec::new(), 11));
        std::fs::write(&path, b"hi").unwrap();
        assert_eq!(read_from(&path, 11), (Vec::new(), 2));
        assert_eq!(
            read_from(&directory.path().join("missing"), 0),
            (Vec::new(), 0)
        );
    }
}
