use proto_gazette::{broker, uuid};

mod actor;
mod gap;
mod handler;
mod heap;
mod listing;
mod producer;
pub mod read;
pub mod routing;
mod state;

use actor::SliceActor;
pub(crate) use handler::serve_slice;

/// LazyJournalClient uses a LazyCell to defer initialization of the Client.
///
/// An instantiated Client requires a background task to perform token refreshes,
/// but at scale not every Slice will interact with every binding and collection,
/// so avoid building a Client until we know it's needed.
pub type LazyJournalClient = std::cell::LazyCell<
    gazette::journal::Client,
    Box<dyn FnOnce() -> gazette::journal::Client + Send>,
>;

/// ReadLines using a type-erased inner Stream. Pin-boxed so that `StreamFuture` works
/// (`StreamFuture` requires `Unpin`, which `Pin<Box<T>>` always satisfies).
pub type ReadLines = std::pin::Pin<
    Box<
        gazette::journal::read::ReadLines<
            1_000_000,
            64,
            futures::stream::BoxStream<'static, gazette::RetryResult<broker::ReadResponse>>,
        >,
    >,
>;

/// Accumulated causal hints from ACK documents, keyed by (journal name, binding index).
/// Drained into the flush frontier each flush cycle.
pub type CausalHints =
    std::collections::HashMap<(Box<str>, u16), Vec<(uuid::Producer, uuid::Clock)>>;

#[derive(Clone)]
pub(crate) struct Metrics {
    /// Total bytes read from journals, accumulated from each progress flush.
    bytes_read: metrics::Counter,
    /// Total flush cycles started (broadcast Flush to Log shards).
    flushes: metrics::Counter,
    /// Total journal reads started over the session lifetime.
    reads_started: metrics::Counter,
    /// Total journal reads that terminated (EOF, JOURNAL_NOT_FOUND, SUSPENDED).
    reads_stopped: metrics::Counter,
    /// Number of active reads currently tailing their journal write head.
    tailing_reads: metrics::Gauge,
    /// Number of reads currently pending AND non-tailing: parked awaiting broker
    /// I/O while behind their write head, head-of-line-blocking the heap drain.
    stalled_reads: metrics::Gauge,
    /// Unresolved gapped producers across the Slice, including those currently
    /// backfilling until their final ACK flush completes.
    gapped_producers: metrics::Gauge,
    /// Backfills triggered (a gapped producer's committing ACK was observed).
    backfills_started: metrics::Counter,
    /// Backfills that completed their final ACK flush.
    backfills_completed: metrics::Counter,
    /// Backfills that failed (terminal error, or the journal disappeared).
    backfills_failed: metrics::Counter,
    /// Physical bytes fetched by historical backfill reads, including bytes
    /// skipped for non-target producers. Excluded from `bytes_read`.
    backfill_bytes_read: metrics::Counter,
    /// Backfill wall-clock duration, in seconds (trigger to final ACK flush).
    backfill_duration_seconds: metrics::Histogram,
    /// Requested backfill range size `ack_begin - F`, in bytes.
    backfill_range_bytes: metrics::Histogram,
    /// Bytes conservatively re-read on restart (`M - R`, summed per read).
    restart_reread_bytes: metrics::Counter,
    /// Uncommitted producers recovered normally by the main read (`F >= R`).
    restart_normal_producers: metrics::Counter,
    /// Uncommitted producers classified as gapped on restart (`F < R`).
    restart_gapped_producers: metrics::Counter,
}

impl Metrics {
    fn new(shard_id: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "shuffle_slice_bytes_read",
                metrics::Unit::Bytes,
                "bytes read from journals, observed at each progress flush",
            );
            metrics::describe_counter!(
                "shuffle_slice_flushes",
                metrics::Unit::Count,
                "flush cycles broadcast to Log shards",
            );
            metrics::describe_counter!(
                "shuffle_slice_reads_started",
                metrics::Unit::Count,
                "journal reads started over the session lifetime",
            );
            metrics::describe_counter!(
                "shuffle_slice_reads_stopped",
                metrics::Unit::Count,
                "journal reads that terminated (EOF, JOURNAL_NOT_FOUND, SUSPENDED)",
            );
            metrics::describe_gauge!(
                "shuffle_slice_tailing_reads",
                metrics::Unit::Count,
                "active reads currently tailing their journal write head",
            );
            metrics::describe_gauge!(
                "shuffle_slice_stalled_reads",
                metrics::Unit::Count,
                "active reads pending and non-tailing (awaiting I/O while behind), blocking the heap drain",
            );
            metrics::describe_gauge!(
                "shuffle_slice_gapped_producers",
                metrics::Unit::Count,
                "unresolved gapped producers, including those backfilling until their final ACK flush",
            );
            metrics::describe_counter!(
                "shuffle_slice_backfills_started",
                metrics::Unit::Count,
                "backfills triggered by a gapped producer's committing ACK",
            );
            metrics::describe_counter!(
                "shuffle_slice_backfills_completed",
                metrics::Unit::Count,
                "backfills that completed their final ACK flush",
            );
            metrics::describe_counter!(
                "shuffle_slice_backfills_failed",
                metrics::Unit::Count,
                "backfills that failed terminally or whose journal disappeared",
            );
            metrics::describe_counter!(
                "shuffle_slice_backfill_bytes_read",
                metrics::Unit::Bytes,
                "physical bytes fetched by historical backfill reads (excluded from bytes_read)",
            );
            metrics::describe_histogram!(
                "shuffle_slice_backfill_duration_seconds",
                metrics::Unit::Seconds,
                "backfill wall-clock duration from trigger to final ACK flush",
            );
            metrics::describe_histogram!(
                "shuffle_slice_backfill_range_bytes",
                metrics::Unit::Bytes,
                "requested backfill range size (ack_begin - F)",
            );
            metrics::describe_counter!(
                "shuffle_slice_restart_reread_bytes",
                metrics::Unit::Bytes,
                "bytes conservatively re-read on restart (M - R, per started read)",
            );
            metrics::describe_counter!(
                "shuffle_slice_restart_normal_producers",
                metrics::Unit::Count,
                "uncommitted producers recovered normally by the main read on restart",
            );
            metrics::describe_counter!(
                "shuffle_slice_restart_gapped_producers",
                metrics::Unit::Count,
                "uncommitted producers classified as gapped on restart",
            );
        });

        Self {
            bytes_read: metrics::counter!("shuffle_slice_bytes_read", "shard_id" => shard_id.to_string()),
            flushes: metrics::counter!("shuffle_slice_flushes", "shard_id" => shard_id.to_string()),
            reads_started: metrics::counter!("shuffle_slice_reads_started", "shard_id" => shard_id.to_string()),
            reads_stopped: metrics::counter!("shuffle_slice_reads_stopped", "shard_id" => shard_id.to_string()),
            tailing_reads: metrics::gauge!("shuffle_slice_tailing_reads", "shard_id" => shard_id.to_string()),
            stalled_reads: metrics::gauge!("shuffle_slice_stalled_reads", "shard_id" => shard_id.to_string()),
            gapped_producers: metrics::gauge!("shuffle_slice_gapped_producers", "shard_id" => shard_id.to_string()),
            backfills_started: metrics::counter!("shuffle_slice_backfills_started", "shard_id" => shard_id.to_string()),
            backfills_completed: metrics::counter!("shuffle_slice_backfills_completed", "shard_id" => shard_id.to_string()),
            backfills_failed: metrics::counter!("shuffle_slice_backfills_failed", "shard_id" => shard_id.to_string()),
            backfill_bytes_read: metrics::counter!("shuffle_slice_backfill_bytes_read", "shard_id" => shard_id.to_string()),
            backfill_duration_seconds: metrics::histogram!("shuffle_slice_backfill_duration_seconds", "shard_id" => shard_id.to_string()),
            backfill_range_bytes: metrics::histogram!("shuffle_slice_backfill_range_bytes", "shard_id" => shard_id.to_string()),
            restart_reread_bytes: metrics::counter!("shuffle_slice_restart_reread_bytes", "shard_id" => shard_id.to_string()),
            restart_normal_producers: metrics::counter!("shuffle_slice_restart_normal_producers", "shard_id" => shard_id.to_string()),
            restart_gapped_producers: metrics::counter!("shuffle_slice_restart_gapped_producers", "shard_id" => shard_id.to_string()),
        }
    }
}
