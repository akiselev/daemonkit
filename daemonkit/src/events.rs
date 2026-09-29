//! Structured runtime events: the model shared by the daemon-side hub, the
//! wire protocol, the optional `tracing` layer, and the client-side tail.
//!
//! Events never exert backpressure on the daemon. A slow subscriber sheds
//! events and is told how many it missed; the daemon's persistent logs remain
//! authoritative.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Severity of an [`Event`]. Ordered from least to most severe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }

    fn parse(value: &str) -> Option<Option<Self>> {
        Some(match value.trim().to_ascii_lowercase().as_str() {
            "trace" => Some(Self::Trace),
            "debug" => Some(Self::Debug),
            "info" => Some(Self::Info),
            "warn" | "warning" => Some(Self::Warn),
            "error" => Some(Self::Error),
            "off" | "none" => None,
            _ => return None,
        })
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Where an [`Event`] came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EventSource {
    /// Emitted through the `tracing` layer or [`EventSink::publish`].
    Application,
    /// One line of a managed child's standard output.
    Stdout,
    /// One line of a managed child's standard error.
    Stderr,
    /// Emitted by the daemonkit runtime itself (readiness, shutdown, drain).
    Runtime,
}

impl EventSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Application => "app",
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Runtime => "runtime",
        }
    }
}

/// One structured event. Serializable so a CLI can emit it as JSON lines.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Monotonic per-generation sequence number assigned when published.
    pub sequence: u64,
    pub unix_millis: u64,
    pub source: EventSource,
    pub level: Level,
    /// Module path for `tracing` events; `stdout`/`stderr` for child output.
    pub target: String,
    pub message: String,
    /// Enclosing span names from root to leaf.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub spans: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<(String, String)>,
    /// Set when the message or fields were cut to respect size bounds.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

impl Event {
    pub fn new(
        source: EventSource,
        level: Level,
        target: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            sequence: 0,
            unix_millis: now_unix_millis(),
            source,
            level,
            target: target.into(),
            message: message.into(),
            spans: Vec::new(),
            fields: Vec::new(),
            truncated: false,
        }
    }

    pub fn field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }

    /// Approximate serialized size used for ring accounting and bounding.
    pub(crate) fn weight(&self) -> usize {
        let fields: usize = self
            .fields
            .iter()
            .map(|(key, value)| key.len() + value.len() + 8)
            .sum();
        let spans: usize = self.spans.iter().map(|span| span.len() + 4).sum();
        96 + self.target.len() + self.message.len() + fields + spans
    }

    /// Cut fields and then the message until the event weighs at most
    /// `max_bytes`. Field values are cut before the message because the
    /// message is what a human reads first.
    #[cfg_attr(not(feature = "runtime-tokio"), allow(dead_code))]
    pub(crate) fn bound(&mut self, max_bytes: usize) {
        if self.weight() <= max_bytes {
            return;
        }
        self.truncated = true;
        let budget = max_bytes / 4;
        for (_, value) in &mut self.fields {
            if value.len() > budget {
                truncate_utf8(value, budget);
            }
        }
        if self.weight() > max_bytes {
            self.fields.clear();
        }
        if self.weight() > max_bytes {
            self.spans.clear();
        }
        let overhead = self.weight() - self.message.len();
        if self.weight() > max_bytes {
            truncate_utf8(&mut self.message, max_bytes.saturating_sub(overhead));
        }
    }

    /// Render one human-readable line without a trailing newline.
    pub fn render(&self) -> String {
        let mut line = String::with_capacity(self.weight());
        line.push_str(&format_unix_millis(self.unix_millis));
        line.push(' ');
        line.push_str(&format!("{:<5}", self.level.as_str().to_ascii_uppercase()));
        line.push(' ');
        if self.source != EventSource::Application {
            line.push('[');
            line.push_str(self.source.as_str());
            line.push_str("] ");
        }
        if !self.target.is_empty() && self.source == EventSource::Application {
            line.push_str(&self.target);
            line.push_str(": ");
        }
        if !self.spans.is_empty() {
            line.push_str(&self.spans.join(":"));
            line.push_str(": ");
        }
        line.push_str(&self.message);
        for (key, value) in &self.fields {
            line.push(' ');
            line.push_str(key);
            line.push('=');
            line.push_str(value);
        }
        if self.truncated {
            line.push_str(" [truncated]");
        }
        line
    }
}

#[cfg_attr(not(feature = "runtime-tokio"), allow(dead_code))]
pub(crate) fn truncate_utf8(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}

pub(crate) fn now_unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

/// `YYYY-MM-DDThh:mm:ss.mmmZ` without pulling in a date crate.
pub(crate) fn format_unix_millis(millis: u64) -> String {
    let seconds = millis / 1000;
    let millis = millis % 1000;
    let days = seconds / 86_400;
    let remainder = seconds % 86_400;
    let (hours, minutes, secs) = (remainder / 3600, (remainder % 3600) / 60, remainder % 60);
    let (year, month, day) = civil_from_days(days as i64);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{secs:02}.{millis:03}Z")
}

// Howard Hinnant's days-to-civil algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Level filter with `tracing`-style directives: `info`, `warn,my_crate=debug`,
/// `off`. Targets match on module-path prefixes, longest match wins. An empty
/// string allows everything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventFilter {
    default: Option<Level>,
    directives: Vec<(String, Option<Level>)>,
}

impl Default for EventFilter {
    fn default() -> Self {
        Self::allow_all()
    }
}

impl EventFilter {
    pub fn allow_all() -> Self {
        Self {
            default: Some(Level::Trace),
            directives: Vec::new(),
        }
    }

    pub fn parse(directives: &str) -> Result<Self, FilterError> {
        if directives.len() > MAX_FILTER_BYTES {
            return Err(FilterError {
                message: format!("filter exceeds {MAX_FILTER_BYTES} bytes"),
            });
        }
        let mut filter = Self::allow_all();
        for directive in directives.split(',') {
            let directive = directive.trim();
            if directive.is_empty() {
                continue;
            }
            match directive.split_once('=') {
                Some((target, level)) => {
                    let target = target.trim();
                    if target.is_empty() || !target.bytes().all(valid_target_byte) {
                        return Err(FilterError {
                            message: format!("invalid target in directive {directive:?}"),
                        });
                    }
                    let level = Level::parse(level).ok_or_else(|| FilterError {
                        message: format!("unknown level in directive {directive:?}"),
                    })?;
                    filter.directives.push((target.to_owned(), level));
                }
                None => {
                    filter.default = Level::parse(directive).ok_or_else(|| FilterError {
                        message: format!("unknown level {directive:?}"),
                    })?;
                }
            }
        }
        filter
            .directives
            .sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.0.cmp(&b.0)));
        Ok(filter)
    }

    pub fn enabled(&self, level: Level, target: &str) -> bool {
        let threshold = self
            .directives
            .iter()
            .find(|(prefix, _)| {
                target == prefix
                    || (target.len() > prefix.len()
                        && target.starts_with(prefix.as_str())
                        && target[prefix.len()..].starts_with("::"))
            })
            .map_or(self.default, |(_, level)| *level);
        threshold.is_some_and(|threshold| level >= threshold)
    }

    /// Least severe level any target could pass, or `None` when everything is off.
    pub fn max_level(&self) -> Option<Level> {
        self.directives
            .iter()
            .map(|(_, level)| *level)
            .chain(std::iter::once(self.default))
            .flatten()
            .min()
    }
}

fn valid_target_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'-' | b'.')
}

pub(crate) const MAX_FILTER_BYTES: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid event filter: {message}")]
pub struct FilterError {
    message: String,
}

impl FilterError {
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Bounds for the in-daemon event ring and per-subscriber queues.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventPolicy {
    /// Newest events retained for replay and diagnostics.
    pub ring_events: usize,
    /// Approximate byte bound for the retained ring.
    pub ring_bytes: usize,
    /// Events larger than this are cut before publication.
    pub max_event_bytes: usize,
    /// Live events queued per subscriber before it is told it lagged.
    pub subscriber_queue: usize,
}

impl Default for EventPolicy {
    fn default() -> Self {
        Self {
            ring_events: 2048,
            ring_bytes: 1024 * 1024,
            max_event_bytes: 16 * 1024,
            subscriber_queue: 1024,
        }
    }
}

#[cfg(feature = "runtime-tokio")]
pub(crate) use hub::{EventHub, LineSplitter};
#[cfg(feature = "runtime-tokio")]
pub use hub::{EventSink, Subscription};

#[cfg(feature = "runtime-tokio")]
mod hub {
    use super::*;
    use std::{
        collections::VecDeque,
        fs::File,
        io::Write,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
    };
    use tokio::sync::broadcast;

    type FilterReloader = dyn Fn(&str) -> Result<(), FilterError> + Send + Sync;

    struct Ring {
        events: VecDeque<Arc<Event>>,
        bytes: usize,
        policy: EventPolicy,
    }

    struct Inner {
        ring: Mutex<Ring>,
        live: broadcast::Sender<Live>,
        sequence: AtomicU64,
        log: Mutex<Option<File>>,
        reloader: Mutex<Option<Arc<FilterReloader>>>,
        /// Directives currently applied by the reloader, for reports.
        directives: Mutex<Option<String>>,
        /// Live subscribers still flushing; `close` waits briefly for them.
        streaming: tokio::sync::watch::Sender<usize>,
    }

    /// Counters exposed through `Inspect`.
    pub(crate) struct HubStats {
        pub(crate) published: u64,
        pub(crate) subscribers: usize,
        pub(crate) directives: Option<String>,
    }

    #[derive(Clone, Debug)]
    pub(crate) enum Live {
        Event(Arc<Event>),
        Closed,
    }

    /// Daemon-side event fan-out: bounded ring plus live broadcast.
    #[derive(Clone)]
    pub(crate) struct EventHub {
        inner: Arc<Inner>,
    }

    /// Cloneable publisher handle handed to the application.
    #[derive(Clone)]
    pub struct EventSink {
        inner: Arc<Inner>,
    }

    impl fmt::Debug for EventSink {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("EventSink").finish_non_exhaustive()
        }
    }

    impl EventHub {
        pub(crate) fn new(policy: EventPolicy) -> Self {
            let (live, _) = broadcast::channel(policy.subscriber_queue.max(1));
            Self {
                inner: Arc::new(Inner {
                    ring: Mutex::new(Ring {
                        events: VecDeque::new(),
                        bytes: 0,
                        policy,
                    }),
                    live,
                    sequence: AtomicU64::new(0),
                    log: Mutex::new(None),
                    reloader: Mutex::new(None),
                    directives: Mutex::new(None),
                    streaming: tokio::sync::watch::channel(0).0,
                }),
            }
        }

        /// Count one live stream until the guard drops.
        pub(crate) fn streaming(&self) -> StreamingGuard {
            self.inner.streaming.send_modify(|count| *count += 1);
            StreamingGuard {
                inner: self.inner.clone(),
            }
        }

        pub(crate) fn sink(&self) -> EventSink {
            EventSink {
                inner: self.inner.clone(),
            }
        }

        pub(crate) fn set_policy(&self, policy: EventPolicy) {
            let mut ring = lock(&self.inner.ring);
            ring.policy = policy;
            ring.trim();
        }

        /// Start mirroring events into `path`, first flushing what the ring
        /// already holds so early startup events are not lost.
        pub(crate) fn attach_log(&self, path: &std::path::Path) -> std::io::Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            let retained: Vec<Arc<Event>> = lock(&self.inner.ring).events.iter().cloned().collect();
            let mut log = lock(&self.inner.log);
            for event in retained {
                let _ = writeln!(file, "{}", event.render());
            }
            *log = Some(file);
            Ok(())
        }

        pub(crate) fn subscribe(&self, replay: usize, filter: &EventFilter) -> Subscription {
            // Subscribe before snapshotting so nothing published in between is
            // lost; the sequence number deduplicates the overlap.
            let live = self.inner.live.subscribe();
            let ring = lock(&self.inner.ring);
            let replayed: Vec<Arc<Event>> = ring
                .events
                .iter()
                .rev()
                .filter(|event| filter.enabled(event.level, &event.target))
                .take(replay)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            let last_sequence = ring.events.back().map_or(0, |event| event.sequence);
            Subscription {
                replayed,
                live,
                last_sequence,
                filter: filter.clone(),
            }
        }

        /// Newest events rendered as text, bounded to `max_bytes` from the end.
        pub(crate) fn render_recent(&self, max_bytes: usize) -> Vec<u8> {
            let ring = lock(&self.inner.ring);
            let mut lines: Vec<String> = Vec::new();
            let mut total = 0;
            for event in ring.events.iter().rev() {
                let line = event.render();
                if total + line.len() + 1 > max_bytes {
                    break;
                }
                total += line.len() + 1;
                lines.push(line);
            }
            lines.reverse();
            let mut bytes = lines.join("\n").into_bytes();
            if !bytes.is_empty() {
                bytes.push(b'\n');
            }
            bytes
        }

        pub(crate) fn set_filter(&self, directives: &str) -> Result<(), FilterError> {
            let reloader = lock(&self.inner.reloader).clone();
            match reloader {
                Some(reloader) => {
                    reloader(directives)?;
                    *lock(&self.inner.directives) = Some(directives.to_owned());
                    Ok(())
                }
                None => Err(FilterError {
                    message: "no reloadable log filter is installed in this daemon".into(),
                }),
            }
        }

        pub(crate) fn stats(&self) -> HubStats {
            HubStats {
                published: self.inner.sequence.load(Ordering::SeqCst),
                subscribers: *self.inner.streaming.borrow(),
                directives: lock(&self.inner.directives).clone(),
            }
        }

        /// Tell live subscribers the generation is ending and give them a
        /// bounded moment to deliver the end frame before the process exits.
        pub(crate) async fn close(&self) {
            let _ = self.inner.live.send(Live::Closed);
            if let Some(file) = lock(&self.inner.log).as_mut() {
                let _ = file.flush();
            }
            let mut streaming = self.inner.streaming.subscribe();
            let _ = tokio::time::timeout(CLOSE_FLUSH_TIMEOUT, async move {
                while *streaming.borrow() != 0 && streaming.changed().await.is_ok() {}
            })
            .await;
        }
    }

    const CLOSE_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

    pub(crate) struct StreamingGuard {
        inner: Arc<Inner>,
    }

    impl Drop for StreamingGuard {
        fn drop(&mut self) {
            self.inner
                .streaming
                .send_modify(|count| *count = count.saturating_sub(1));
        }
    }

    impl EventSink {
        /// Publish one event. Never blocks and never fails: the ring sheds the
        /// oldest entries and slow subscribers are told what they missed.
        pub fn publish(&self, mut event: Event) {
            let max_bytes = {
                let ring = lock(&self.inner.ring);
                ring.policy.max_event_bytes
            };
            event.bound(max_bytes);
            event.sequence = self.inner.sequence.fetch_add(1, Ordering::SeqCst) + 1;
            let event = Arc::new(event);
            {
                let mut ring = lock(&self.inner.ring);
                ring.bytes = ring.bytes.saturating_add(event.weight());
                ring.events.push_back(event.clone());
                ring.trim();
            }
            if let Some(file) = lock(&self.inner.log).as_mut() {
                let _ = writeln!(file, "{}", event.render());
            }
            let _ = self.inner.live.send(Live::Event(event));
        }

        /// Register the closure that applies a new filter directive string,
        /// plus the directives it currently applies. Installed by the
        /// `tracing` layer; applications with their own reloadable filter can
        /// register one too.
        pub fn register_filter_reloader<F>(&self, current: &str, reloader: F)
        where
            F: Fn(&str) -> Result<(), FilterError> + Send + Sync + 'static,
        {
            *lock(&self.inner.reloader) = Some(Arc::new(reloader));
            *lock(&self.inner.directives) = Some(current.to_owned());
        }

        pub(crate) fn max_event_bytes(&self) -> usize {
            lock(&self.inner.ring).policy.max_event_bytes
        }
    }

    /// One subscriber's view: a replay snapshot followed by live events.
    pub struct Subscription {
        pub(crate) replayed: Vec<Arc<Event>>,
        pub(crate) live: broadcast::Receiver<Live>,
        pub(crate) last_sequence: u64,
        pub(crate) filter: EventFilter,
    }

    impl Subscription {
        /// Next live event after the replay, deduplicated against it.
        /// `Ok(None)` means the hub closed; `Err(n)` reports `n` shed events.
        pub(crate) async fn next_live(&mut self) -> Result<Option<Arc<Event>>, u64> {
            loop {
                match self.live.recv().await {
                    Ok(Live::Event(event)) => {
                        if event.sequence <= self.last_sequence {
                            continue;
                        }
                        self.last_sequence = event.sequence;
                        if self.filter.enabled(event.level, &event.target) {
                            return Ok(Some(event));
                        }
                    }
                    Ok(Live::Closed) | Err(broadcast::error::RecvError::Closed) => return Ok(None),
                    Err(broadcast::error::RecvError::Lagged(count)) => return Err(count),
                }
            }
        }
    }

    impl Ring {
        fn trim(&mut self) {
            while self.events.len() > self.policy.ring_events.max(1)
                || (self.bytes > self.policy.ring_bytes && self.events.len() > 1)
            {
                if let Some(discarded) = self.events.pop_front() {
                    self.bytes = self.bytes.saturating_sub(discarded.weight());
                }
            }
        }
    }

    fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Turns a byte stream into bounded, newline-terminated events.
    pub(crate) struct LineSplitter {
        sink: EventSink,
        source: EventSource,
        partial: Vec<u8>,
        max_line_bytes: usize,
        truncated: bool,
    }

    impl LineSplitter {
        pub(crate) fn new(sink: EventSink, source: EventSource) -> Self {
            let max_line_bytes = sink.max_event_bytes().saturating_sub(128).max(64);
            Self {
                sink,
                source,
                partial: Vec::new(),
                max_line_bytes,
                truncated: false,
            }
        }

        pub(crate) fn push(&mut self, mut bytes: &[u8]) {
            while let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
                self.extend(&bytes[..newline]);
                self.flush();
                bytes = &bytes[newline + 1..];
            }
            self.extend(bytes);
        }

        fn extend(&mut self, bytes: &[u8]) {
            let room = self.max_line_bytes.saturating_sub(self.partial.len());
            if bytes.len() > room {
                self.partial.extend_from_slice(&bytes[..room]);
                self.truncated = true;
            } else {
                self.partial.extend_from_slice(bytes);
            }
        }

        pub(crate) fn flush(&mut self) {
            if self.partial.is_empty() && !self.truncated {
                return;
            }
            if self.partial.last() == Some(&b'\r') {
                self.partial.pop();
            }
            let message = String::from_utf8_lossy(&self.partial).into_owned();
            let mut event = Event::new(self.source, Level::Info, self.source.as_str(), message);
            event.truncated = self.truncated;
            self.partial.clear();
            self.truncated = false;
            self.sink.publish(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_directives_follow_longest_prefix() {
        let filter = EventFilter::parse("warn,app=debug,app::net=off, noisy = trace").unwrap();
        assert!(filter.enabled(Level::Warn, "other"));
        assert!(!filter.enabled(Level::Info, "other"));
        assert!(filter.enabled(Level::Debug, "app"));
        assert!(filter.enabled(Level::Debug, "app::db"));
        assert!(!filter.enabled(Level::Trace, "app::db"));
        assert!(!filter.enabled(Level::Error, "app::net::tcp"));
        assert!(!filter.enabled(Level::Debug, "application"));
        assert!(filter.enabled(Level::Trace, "noisy"));
        assert_eq!(filter.max_level(), Some(Level::Trace));
        assert_eq!(EventFilter::parse("off").unwrap().max_level(), None);
        assert!(EventFilter::parse("").unwrap().enabled(Level::Trace, "x"));
        assert!(EventFilter::parse("loud").is_err());
        assert!(EventFilter::parse("a b=info").is_err());
        assert!(EventFilter::parse(&"a".repeat(MAX_FILTER_BYTES + 1)).is_err());
    }

    #[test]
    fn bounding_cuts_fields_before_message_and_marks_truncation() {
        let mut event = Event::new(EventSource::Application, Level::Info, "t", "m".repeat(400))
            .field("k", "v".repeat(2000));
        event.bound(600);
        assert!(event.truncated);
        assert!(event.weight() <= 600);
        assert!(!event.message.is_empty());

        let mut tiny = Event::new(EventSource::Application, Level::Info, "t", "héllo wörld");
        tiny.bound(103);
        assert!(tiny.truncated);
        assert!(std::str::from_utf8(tiny.message.as_bytes()).is_ok());
    }

    #[test]
    fn timestamps_render_as_utc_iso8601() {
        assert_eq!(format_unix_millis(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_unix_millis(1_700_000_000_123),
            "2023-11-14T22:13:20.123Z"
        );
    }

    #[test]
    fn render_includes_source_spans_and_fields() {
        let mut event =
            Event::new(EventSource::Application, Level::Warn, "app::x", "boom").field("code", "7");
        event.spans = vec!["req".into(), "db".into()];
        event.unix_millis = 0;
        assert_eq!(
            event.render(),
            "1970-01-01T00:00:00.000Z WARN  app::x: req:db: boom code=7"
        );
        let line = Event::new(EventSource::Stderr, Level::Info, "stderr", "oops");
        assert!(line.render().ends_with("INFO  [stderr] oops"));
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn hub_replays_ring_then_streams_without_duplicates() {
        let hub = EventHub::new(EventPolicy {
            ring_events: 3,
            ..EventPolicy::default()
        });
        let sink = hub.sink();
        for index in 0..5 {
            sink.publish(Event::new(
                EventSource::Application,
                Level::Info,
                "t",
                format!("event {index}"),
            ));
        }
        let mut subscription = hub.subscribe(10, &EventFilter::allow_all());
        let replayed: Vec<u64> = subscription
            .replayed
            .iter()
            .map(|event| event.sequence)
            .collect();
        assert_eq!(replayed, vec![3, 4, 5]);

        sink.publish(Event::new(
            EventSource::Application,
            Level::Debug,
            "t",
            "live",
        ));
        let live = subscription.next_live().await.unwrap().unwrap();
        assert_eq!(live.sequence, 6);
        hub.close().await;
        assert!(subscription.next_live().await.unwrap().is_none());
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn hub_reports_lag_instead_of_blocking_publishers() {
        let hub = EventHub::new(EventPolicy {
            subscriber_queue: 2,
            ..EventPolicy::default()
        });
        let sink = hub.sink();
        let mut subscription = hub.subscribe(0, &EventFilter::allow_all());
        for index in 0..5 {
            sink.publish(Event::new(
                EventSource::Application,
                Level::Info,
                "t",
                format!("{index}"),
            ));
        }
        assert!(matches!(subscription.next_live().await, Err(3)));
        assert_eq!(
            subscription.next_live().await.unwrap().unwrap().message,
            "3"
        );
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn line_splitter_bounds_lines_and_strips_carriage_returns() {
        let hub = EventHub::new(EventPolicy {
            max_event_bytes: 256,
            ..EventPolicy::default()
        });
        let mut splitter = LineSplitter::new(hub.sink(), EventSource::Stdout);
        splitter.push(b"one\r\ntw");
        splitter.push(b"o\n");
        splitter.push(&[b'x'; 500]);
        splitter.push(b"\ntail");
        splitter.flush();
        let subscription = hub.subscribe(10, &EventFilter::allow_all());
        let messages: Vec<(String, bool)> = subscription
            .replayed
            .iter()
            .map(|event| (event.message.clone(), event.truncated))
            .collect();
        assert_eq!(messages[0], ("one".into(), false));
        assert_eq!(messages[1], ("two".into(), false));
        assert!(messages[2].1 && messages[2].0.len() < 500);
        assert_eq!(messages[3], ("tail".into(), false));
        assert!(subscription
            .replayed
            .iter()
            .all(|event| event.source == EventSource::Stdout));
    }
}
