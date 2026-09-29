//! `tracing` integration: a per-layer-filtered [`Layer`] that forwards
//! events to the daemon's event hub so clients can tail them live, plus a
//! runtime-reloadable filter driven by `set_log_filter`.
//!
//! The application keeps ownership of its subscriber. Compose the layer
//! with whatever else it uses:
//!
//! ```ignore
//! use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
//! let bootstrap = daemonkit::Bootstrap::detect()?.expect("launched by daemonkit");
//! tracing_subscriber::registry()
//!     .with(tracing_subscriber::fmt::layer().with_filter(my_env_filter))
//!     .with(daemonkit::tracing::layer(&bootstrap.events()))
//!     .init();
//! ```
//!
//! Give other layers their own per-layer filter (`with_filter`) rather than a
//! global one, otherwise a global filter stricter than daemonkit's will drop
//! events before this layer sees them and `set_log_filter` cannot raise
//! verbosity beyond it.

use crate::events::{Event, EventFilter, EventSink, EventSource, FilterError, Level};
use std::sync::{Arc, RwLock};
use tracing_core::{
    field::{Field, Visit},
    subscriber::Interest,
    LevelFilter, Metadata, Subscriber,
};
use tracing_subscriber::{
    filter::Filtered,
    layer::{Context, Filter, Layer},
    registry::LookupSpan,
};

/// Forwarding layer filtered at `info` until `set_log_filter` changes it.
pub fn layer<S>(sink: &EventSink) -> Filtered<EventLayer, DynamicFilter, S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    layer_with_filter(sink, "info").expect("static directive parses")
}

/// Forwarding layer with an initial directive string such as
/// `info,my_crate=debug`.
pub fn layer_with_filter<S>(
    sink: &EventSink,
    directives: &str,
) -> Result<Filtered<EventLayer, DynamicFilter, S>, FilterError>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let filter = DynamicFilter::new(EventFilter::parse(directives)?);
    let reload = filter.clone();
    sink.register_filter_reloader(directives, move |directives| reload.reload(directives));
    Ok(EventLayer { sink: sink.clone() }.with_filter(filter))
}

/// Publishes every enabled `tracing` event to an [`EventSink`].
#[derive(Clone, Debug)]
pub struct EventLayer {
    sink: EventSink,
}

impl EventLayer {
    pub fn new(sink: EventSink) -> Self {
        Self { sink }
    }
}

impl<S> Layer<S> for EventLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing_core::Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = Visitor::default();
        event.record(&mut visitor);
        let mut out = Event::new(
            EventSource::Application,
            level_from(metadata.level()),
            metadata.target(),
            visitor.message.unwrap_or_default(),
        );
        out.fields = visitor.fields;
        if let Some(scope) = ctx.event_scope(event) {
            out.spans = scope
                .from_root()
                .map(|span| span.name().to_owned())
                .collect();
        }
        self.sink.publish(out);
    }
}

/// Per-layer filter whose directives can be swapped at runtime.
#[derive(Clone, Debug)]
pub struct DynamicFilter {
    shared: Arc<RwLock<EventFilter>>,
}

impl DynamicFilter {
    pub fn new(filter: EventFilter) -> Self {
        Self {
            shared: Arc::new(RwLock::new(filter)),
        }
    }

    /// Parse and install new directives, then ask `tracing` to re-evaluate
    /// callsite interest so newly enabled levels start flowing.
    pub fn reload(&self, directives: &str) -> Result<(), FilterError> {
        let parsed = EventFilter::parse(directives)?;
        *self
            .shared
            .write()
            .unwrap_or_else(|error| error.into_inner()) = parsed;
        tracing_core::callsite::rebuild_interest_cache();
        Ok(())
    }

    pub fn current(&self) -> EventFilter {
        self.shared
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    fn enabled_for(&self, metadata: &Metadata<'_>) -> bool {
        self.shared
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .enabled(level_from(metadata.level()), metadata.target())
    }
}

impl<S: Subscriber> Filter<S> for DynamicFilter {
    fn enabled(&self, metadata: &Metadata<'_>, _: &Context<'_, S>) -> bool {
        self.enabled_for(metadata)
    }

    fn callsite_enabled(&self, _: &'static Metadata<'static>) -> Interest {
        // Directives change at runtime; never let a callsite cache a verdict.
        Interest::sometimes()
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(match self.current().max_level() {
            None => LevelFilter::OFF,
            Some(Level::Trace) => LevelFilter::TRACE,
            Some(Level::Debug) => LevelFilter::DEBUG,
            Some(Level::Info) => LevelFilter::INFO,
            Some(Level::Warn) => LevelFilter::WARN,
            Some(Level::Error) => LevelFilter::ERROR,
        })
    }
}

fn level_from(level: &tracing_core::Level) -> Level {
    match *level {
        tracing_core::Level::TRACE => Level::Trace,
        tracing_core::Level::DEBUG => Level::Debug,
        tracing_core::Level::INFO => Level::Info,
        tracing_core::Level::WARN => Level::Warn,
        tracing_core::Level::ERROR => Level::Error,
    }
}

#[derive(Default)]
struct Visitor {
    message: Option<String>,
    fields: Vec<(String, String)>,
}

impl Visitor {
    fn record(&mut self, field: &Field, value: String) {
        if field.name() == "message" && self.message.is_none() {
            self.message = Some(value);
        } else {
            self.fields.push((field.name().to_owned(), value));
        }
    }
}

impl Visit for Visitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record(field, format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value.to_owned());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record(field, value.to_string());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record(field, value.to_string());
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.record(field, value.to_string());
    }
}
