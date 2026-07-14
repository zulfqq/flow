//! Gapped-producer recovery: pure transitions and actor orchestration.
//!
//! On restart, a `(binding, journal)` read starts at the furthest journal
//! position justified by its checkpoint: the maximum offset magnitude `M`
//! across producer entries. An uncommitted producer span whose begin offset
//! `F` falls before `M` is *gapped*: the main read skips `[F, M)` and the
//! producer is frozen until it resolves. A gapped entry is marked in-memory by
//! `ProducerState::gapped`; while set, its `offset` is the pinned `F`. See
//! `plans/shuffle-gapped-restart.md` §Producer state machine and §Gapped state.
//!
//! The first newer main-read document of a gapped producer (a CONTINUE or ACK
//! with `clock > last_commit`) is the *trigger*. At trigger time the actor:
//!
//! 1. reconstructs the recovered open span directly into the read's ordinary
//!    `pending` map as `{last_commit, max_continue: 0, offset: F, gapped: false}`
//!    — this both installs the span and clears the gapped marker; and
//! 2. opens a bounded historical read of `[F, trigger.begin)`, held in the single
//!    actor-owned `Backfill` (`SliceActor::backfill`).
//!
//! There is no separate live sequencing state: the backfill drain
//! (`try_drain_backfill`) sequences historical target-producer documents against
//! the ordinary pending-else-settled lookup and commits results back into
//! `pending`, exactly the normal path's discipline. Sequencing is
//! pure/speculative and committed only after the append is sent, so a retry after
//! a full Log channel re-sequences from the same snapshot.
//!
//! **The trigger stays put.** It is never popped or shelved: it remains buffered
//! at the head of its read in the ready heap for the whole backfill. The heap does
//! not drain while a backfill is active (see the gate in
//! `SliceActor::try_log_request_tx`), so leaving it in place is behaviorally
//! identical to popping and re-pushing. This makes **at most one backfill per
//! Slice, globally**, a structural invariant — no second trigger can be reached
//! while one is in flight. It parallels both the all-reads-tailing stalled-read
//! gate and the legacy conservative restart (which re-read `[F, …)` on a
//! non-tailing main read, blocking all draining anyway).
//!
//! **Completion commits and installs nothing** — the span was installed at
//! trigger time. When the historical read reaches `trigger.begin`, the backfill
//! is simply cleared and its completion is reported. The next drain iteration
//! pops the trigger — now that the producer is no longer gapped and its `pending`
//! state is the reconstructed span — and sequences it through the wholly-normal
//! path: a CONTINUE extends the span and appends, an ACK commits it (causal hints,
//! committed offset, flush) via the existing normal-path code.
//!
//! This is durably safe because `max_continue` is NOT persisted in
//! `ProducerFrontier`, and the checkpoint's `F` is by definition the first pending
//! CONTINUE's begin offset, so a recovering `ContinueBeginSpan` re-derives
//! `offset = F`. An interim flush mid-backfill therefore carries exactly the
//! `(last_commit, F)` the durable checkpoint already records; a crash mid-backfill
//! recovers the unchanged positive `F` and re-gaps idempotently. (On fragment loss
//! the first found document begins at `F' > F`; if that `F'` leaks to a durable
//! base and the session then crashes, recovery re-gaps at `F'`, and `[F, F')` was
//! unreadable anyway — self-consistent.)
//!
//! This module follows the crate's decomposition convention: a pure decision layer
//! (`backfill_read_request`, and `state::sequence_gapped` / `state::sequence_producer`
//! which it leans on — all directly unit-tested) and an orchestration layer (an
//! `impl SliceActor` extension block driving the backfill lifecycle). The actor
//! owns the IO objects (in-flight historical read, batch cursor); `ReadState` holds
//! only ordinary producer state, which keeps it snapshot-testable.
//!
//! Backfilled documents do NOT flow through the ready heap; they are appended
//! directly. The trigger was sequenced (to start the backfill) only after clearing
//! the drain loop's clock-delay gate, so — same binding read-delay, and
//! strictly-ascending per-producer clocks — every historical CONTINUE in
//! `[F, trigger.begin)` has an adjusted clock already in the past. Heap ordering,
//! priority ordering, and clock-delay gating are therefore provable no-ops for
//! them; only the Log-append semantics matter (per-producer journal order
//! preserved, and the one-transaction invariant enforced: no committing document
//! appears inside the historical range). Ordering *relative to other journals* is
//! trivially preserved because the Slice blocks all other draining for the
//! backfill's duration.

use super::actor::{Buffers, SliceActor};
use super::producer::ProducerState;
use super::read::{self, Meta, ReadyRead};
use super::state;
use futures::StreamExt;
use proto_flow::shuffle;
use proto_gazette::{broker, uuid};
use tokio::sync::mpsc;

/// All live state for the single active backfill of a gapped producer's pending
/// transaction, owned by the actor in `SliceActor::backfill`.
///
/// At most one backfill exists per Slice, globally — a structural invariant, not
/// a per-read one: the heap does not drain while a backfill is active, so no
/// second trigger can be discovered until this one completes. It holds only the
/// in-flight historical read (`io`) and the bookkeeping needed to report
/// completion; the reconstructed open span lives in the read's ordinary `pending`
/// map (installed at trigger time), and the trigger itself stays buffered at the
/// head of its read in the ready heap.
pub struct Backfill {
    /// Read id of the read whose gapped producer triggered this backfill (index
    /// into `SliceActor::reads`). Its trigger document stays at the head of the
    /// ready heap for the whole backfill.
    pub read_key: u32,
    /// The gapped producer whose newer document triggered this backfill.
    pub target: uuid::Producer,
    /// `F`, the recovered span begin; retained for the completion range event.
    /// The reconstructed span was installed into `pending` at `offset = F`.
    pub gap_begin: i64,
    /// `trigger.begin`, the historical read's exclusive end; retained with
    /// `gap_begin` for the completion range event.
    pub trigger_begin: i64,
    /// The historical read's I/O state: either awaiting its next batch
    /// (`Reading`) or draining a resolved batch document-by-document
    /// (`Draining`). This two-state machine makes "the inner read is re-polled
    /// only once the current batch is fully drained" a plain enum fact: the
    /// `select!` arm polls the stream only while `Reading`.
    pub io: BackfillIo,
    /// Trigger instant, for the backfill-duration histogram.
    pub started_at: std::time::Instant,
    /// Physical bytes fetched so far by the historical read, for the
    /// completion event.
    pub physical_bytes: u64,
}

/// The historical backfill read's I/O state. Exactly one variant holds the inner
/// `ReadLines` at any time, so it is never both polled by the `select!` arm and
/// drained by `try_drain_backfill`.
pub enum BackfillIo {
    /// Awaiting the next historical batch from Gazette. The `select!` arm polls
    /// this stream (via `next_backfill_batch`); a resolved batch transitions to
    /// `Draining`, a stream end completes the backfill.
    Reading(super::ReadLines),
    /// A resolved batch being drained document-by-document. The inner `ReadLines`
    /// rides along in the `ReadyRead` and returns to `Reading` (re-polled) only
    /// once the batch is fully drained — so no batch is ever pending when the
    /// stream reaches its end.
    Draining(Box<ReadyRead>),
}

/// Build the bounded, non-blocking historical read of
/// `[gap_begin, trigger_begin)` for a triggered backfill. Same `begin_mod_time`
/// and partition-filtered journal as the main read; no write-head probe is
/// needed for a bounded range, and the end offset is exclusive.
pub(super) fn backfill_read_request(
    journal: &str,
    binding: &crate::Binding,
    gap_begin: i64,
    trigger_begin: i64,
) -> broker::ReadRequest {
    broker::ReadRequest {
        journal: format!("{};{}", journal, binding.journal_read_suffix),
        begin_mod_time: binding.not_before.to_unix().0 as i64,
        block: false,
        do_not_proxy: true,
        end_offset: trigger_begin, // exclusive
        metadata_only: false,
        offset: gap_begin, // F
        min_etcd_revision: 0,
        header: None,
    }
}

impl SliceActor {
    /// Begin a backfill for a gapped producer's *trigger* (its first newer
    /// CONTINUE or ACK, the current ready-heap head). The trigger is NOT sequenced
    /// now and is NOT popped: it stays buffered at the head of its read in the
    /// ready heap, and the `backfill.is_some()` gate in `try_log_request_tx` parks
    /// the whole Slice until this backfill completes (spec §Trigger and parking).
    ///
    /// `gap_begin` (`F`) and `recovered_last_commit` come from the drain loop's
    /// single pending-else-settled lookup of the frozen entry, so no second lookup
    /// is performed for the trigger document.
    ///
    /// Reconstructs the recovered open span directly into the read's ordinary
    /// `pending` map, which both installs the span and clears the gapped marker.
    /// The backfill drain and the eventual re-sequenced trigger then advance this
    /// ordinary state; there is no separate `live`. Because a backfill blocks all
    /// heap draining, this is only ever reached while no backfill is active, so a
    /// fresh one can always be installed.
    pub(super) fn start_backfill(
        &mut self,
        read_key: u32,
        trigger: &Meta,
        gap_begin: i64,
        recovered_last_commit: uuid::Clock,
    ) -> anyhow::Result<()> {
        let read_id = read_key as usize;
        let trigger_begin = trigger.begin_offset;
        let target = trigger.producer;

        // Freeze invariant: while gapped, `offset` is the pinned `F` (>= 0), and
        // only the four resolutions (trigger, clean rollback, deep rollback, newer
        // OUTSIDE) clear the bit. A trigger is one of them.
        debug_assert!(
            gap_begin >= 0,
            "a gapped entry's offset is the pinned begin offset F",
        );
        // The range is always non-empty: `F < M <= trigger_begin`. The main read
        // starts at M and reaches the trigger at or after M, while a gapped span
        // begin F is strictly below M.
        assert!(
            gap_begin < trigger_begin,
            "backfill range [{gap_begin}, {trigger_begin}) must be non-empty: F precedes M, \
             which is at-or-below the trigger",
        );

        // Reconstruct the recovered open span into `pending`, clearing the gapped
        // marker (the reconstructed entry has `gapped: false` and shadows the
        // frozen `settled` entry via pending-else-settled lookup, draining into
        // `settled` at the next flush). Durably safe: `max_continue` is not
        // persisted and `offset` stays at `F`, so an interim flush mid-backfill
        // carries exactly the `(last_commit, F)` the checkpoint already records,
        // and a crash re-derives the gap and re-triggers idempotently.
        _ = self.reads[read_id].pending.insert(
            target,
            ProducerState {
                last_commit: recovered_last_commit,
                max_continue: uuid::Clock::zero(),
                offset: gap_begin,
                gapped: false,
            },
        );

        let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];
        service_kit::event!(
            tracing::Level::INFO,
            "backfill",
            session = self.topology.session_id,
            read_id,
            binding = binding.index,
            journal = self.reads[read_id].journal.to_string(),
            producer = service_kit::event::debug(target),
            gap_begin, // F
            trigger_begin,
            trigger_is_ack = trigger.flags == uuid::Flags::ACK_TXN,
            "triggering backfill of gapped producer's pending transaction",
        );
        self.metrics.backfills_started.increment(1);

        // Start the bounded, non-blocking historical read. Same client, auth,
        // begin_mod_time, schema validation, and partition-filtered journal as
        // the main read; no write-head probe is needed for a bounded range. The
        // read carries the plain `read_key` as its id: the historical read never
        // shares the heap or `pending_reads` with main reads.
        let client = (*self.topology.journal_clients[binding.index as usize]).clone();
        let request = backfill_read_request(
            &self.reads[read_id].journal,
            binding,
            gap_begin,
            trigger_begin,
        );
        let read: super::ReadLines = Box::pin(gazette::journal::read::ReadLines::new(
            client.read(request).boxed(),
            read_key,
            false, // Never tailing: the range is bounded and historical.
        ));

        // At most one backfill exists per Slice, globally: the heap does not
        // drain while one is in flight, so no second trigger can be reached.
        assert!(
            self.backfill.is_none(),
            "a backfill is installed only while none is active (the heap drain is blocked)",
        );
        self.backfill = Some(Backfill {
            read_key,
            target,
            gap_begin,
            trigger_begin,
            io: BackfillIo::Reading(read),
            started_at: std::time::Instant::now(),
            physical_bytes: 0,
        });

        Ok(())
    }

    /// Drain the active backfill's batch cursor as far as it will go: append
    /// recovered target-producer documents in journal order and skip others,
    /// until the batch is exhausted (its historical read returns to `Reading`,
    /// re-polled by the `select!` arm) or an Append channel lacks capacity (the
    /// cursor parks at that document). Backfill documents advance the target's
    /// ordinary `pending` state but never touch the main read's offset baselines
    /// (spec §Read positions, §Completion).
    ///
    /// Called from `try_log_request_tx` after the flush-priority check and
    /// independent of the ready heap and its tailing gate (which the backfill
    /// blocks entirely). Returns `Some(tx)` when an Append channel lacked
    /// capacity — the caller wakes on `tx` and retries — or `None` otherwise.
    ///
    /// Sequences each document against the target's pending-else-settled snapshot
    /// and commits back into `pending` only after the append is sent, so a retry
    /// after a full channel re-sequences from the same snapshot and never
    /// double-appends. Takes ownership of the single `Backfill` for the duration
    /// so `io` can be restructured while `reads`/`topology` and the Log channels
    /// are borrowed; it is put back before returning.
    pub(super) fn try_drain_backfill(
        &mut self,
        buffers: &mut Buffers,
    ) -> anyhow::Result<Option<mpsc::Sender<shuffle::LogRequest>>> {
        let Some(mut backfill) = self.backfill.take() else {
            return Ok(None);
        };
        // Only a `Draining` batch has documents to append; `Reading` (awaiting
        // the next batch) has nothing to do here.
        if !matches!(backfill.io, BackfillIo::Draining(_)) {
            self.backfill = Some(backfill);
            return Ok(None);
        }

        let read_state = &mut self.reads[backfill.read_key as usize];
        let binding = &self.topology.bindings[read_state.binding_index as usize];

        loop {
            // Peek the cursor's head, retained until its append succeeds so a
            // retry after a full channel doesn't drop it.
            let BackfillIo::Draining(cursor) = &backfill.io else {
                break; // Batch fully drained; its read returned to `Reading`.
            };
            let meta = cursor.meta; // `Meta` is Copy.

            // Documents of other producers are already represented by checkpoint
            // state or belong to independent gaps: skip without sequencing, state
            // mutation, key extraction, or append.
            if meta.producer != backfill.target {
                backfill.io = advance_backfill_cursor(backfill.io);
                continue;
            }

            // Sequence against the target's ordinary pending-else-settled state:
            // the reconstructed open span installed at trigger time (or extended by
            // prior backfill iterations). Pure/speculative; committed only after
            // the append succeeds.
            let producer_state = (read_state.pending.get(&meta.producer))
                .or_else(|| read_state.settled.get(&meta.producer))
                .cloned()
                .unwrap_or_default();
            let sequenced =
                match state::sequence_producer(producer_state, &read_state.journal, binding, &meta)
                {
                    Ok(sequenced) => sequenced,
                    Err(err) => {
                        // Restore before fail-fast so teardown's `Drop` accounting
                        // still counts this backfill as stopped.
                        self.backfill = Some(backfill);
                        return Err(err);
                    }
                };

            // One-transaction invariant: `[F, trigger.begin)` reconstructs a single
            // open span and holds no committing boundary (any state-changing ACK
            // below M is reflected by checkpoint closure, and `[M, trigger.begin)`
            // holds no target document — the main read parked at the first one). A
            // commit here therefore contradicts the recovered checkpoint and is a
            // terminal consistency error.
            if sequenced.is_commit {
                self.backfill = Some(backfill);
                anyhow::bail!(
                    "backfill of journal {} (binding {}) hit an unexpected committing document at \
                     offset {} for target producer {:?}: a distinct transaction boundary inside the \
                     historical range contradicts the recovered checkpoint",
                    read_state.journal,
                    binding.state_key(),
                    meta.begin_offset,
                    meta.producer,
                );
            }

            if sequenced.is_append {
                let BackfillIo::Draining(cursor) = &backfill.io else {
                    unreachable!("still draining the peeked cursor");
                };
                if let Err(tx) = Self::try_log_request_append_tx(
                    binding,
                    buffers,
                    &read_state.journal,
                    &self.topology.shards,
                    &mut self.log_prev_journal,
                    &self.log_request_tx,
                    cursor,
                ) {
                    // Park the cursor here; `pending` is unchanged so the wake
                    // re-sequences this document from the same snapshot.
                    self.backfill = Some(backfill);
                    return Ok(Some(tx));
                }
            }

            // Commit the reconstructed span forward into `pending` (never touching
            // main-read offset baselines) and advance.
            _ = read_state
                .pending
                .insert(meta.producer, sequenced.producer_state);
            backfill.io = advance_backfill_cursor(backfill.io);
        }

        self.backfill = Some(backfill);
        Ok(None)
    }

    /// Complete a backfill once its historical range has been fully read.
    /// Completion commits and installs nothing (spec §Completion): the
    /// reconstructed span was installed into `pending` at trigger time, so this
    /// only reports the completion, increments `backfills_stopped`, and clears
    /// `self.backfill` (by consuming it). The trigger remains at the head of the
    /// ready heap; the next drain iteration pops it and sequences it against the
    /// reconstructed span through the wholly-normal path — a CONTINUE extends the
    /// span and appends, an ACK commits it (causal hints, committed offset, flush)
    /// — now that the producer is no longer gapped.
    fn complete_backfill(&mut self, backfill: Backfill) {
        let Backfill {
            read_key,
            target,
            gap_begin,
            trigger_begin,
            io,
            started_at,
            physical_bytes,
        } = backfill;
        let read_id = read_key as usize;
        debug_assert!(
            matches!(io, BackfillIo::Reading(_)),
            "the historical read reaches its end from `Reading`, only after its final batch drained",
        );
        drop(io); // The exhausted historical read.

        // An empty reconstructed span (no target-producer CONTINUEs found) leaves
        // the installed span at `max_continue == 0`. Expected only when historical
        // content was unavailable or filtered — a suspiciously short backfill — or,
        // benignly, for a hint-only producer backfilled from F = 0.
        let span_empty = (self.reads[read_id].pending.get(&target))
            .or_else(|| self.reads[read_id].settled.get(&target))
            .map(|ps| ps.max_continue == uuid::Clock::zero())
            .unwrap_or(true);

        let elapsed = started_at.elapsed();
        self.metrics.backfills_stopped.increment(1);

        service_kit::event!(
            tracing::Level::INFO,
            "backfill",
            session = self.topology.session_id,
            read_id,
            binding = self.reads[read_id].binding_index,
            journal = self.reads[read_id].journal.to_string(),
            producer = service_kit::event::debug(target),
            range_bytes = trigger_begin - gap_begin,
            physical_bytes,
            duration_ms = elapsed.as_millis() as u64,
            span_empty, // true flags a suspiciously short backfill (fragment loss)
            "completed backfill of gapped producer's transaction",
        );

        // `self.backfill` stays `None` (consumed). Draining resumes on the next
        // iteration, which pops the trigger and sequences it normally.
    }

    /// Process the active backfill's historical read resolution, yielded by the
    /// `select!` arm via `next_backfill_batch`. Mirrors `process_read_result` but
    /// never touches main-read offset baselines or `write_head`, counts physical
    /// bytes into the backfill counter, transitions a resolved batch to
    /// `BackfillIo::Draining` (drained by `try_drain_backfill`), and on stream
    /// end completes the backfill.
    ///
    /// Takes ownership of the `Backfill` for the duration; puts it back unless the
    /// stream ended or the journal was removed (both complete the backfill and
    /// consume it, also unblocking the Slice).
    pub(super) fn process_backfill_result(
        &mut self,
        result: Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
    ) -> anyhow::Result<()> {
        let mut backfill = self
            .backfill
            .take()
            .expect("a backfill batch resolved, so a backfill is in flight");
        let read_key = backfill.read_key;
        let read_id = read_key as usize;
        let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];

        let Some(result) = result else {
            // The bounded stream reached `end_offset`. Its final batch was fully
            // drained before we re-polled (so `io` is `Reading`), so every
            // recovered Append precedes the trigger's own append or commit on each
            // Log channel. Complete the backfill (which consumes `backfill`).
            self.complete_backfill(backfill);
            return Ok(());
        };

        let lines_batch = match result {
            Err(gazette::RetryError {
                attempt,
                inner: err,
            }) => match read::classify_read_failure(err) {
                read::ReadFailure::JournalRemoved(status) => {
                    // Journal removal during a backfill is the degenerate case of
                    // the accepted historical-fragment-loss risk: the entire unread
                    // remainder `[here, trigger.begin)` is one big hole — "you get
                    // what you get". Deletion or FULL suspension implies fragments
                    // were already gone. Treat it as an implicit EOF: complete the
                    // backfill with whatever span was reconstructed so far (possibly
                    // empty), which also unblocks the Slice. The trigger then
                    // sequences normally (an ACK commits the recovered extent,
                    // possibly empty; a CONTINUE extends a span that simply never
                    // commits), and the main read discovers the removal itself when
                    // its `ReadLines` is re-polled after the buffered tail drains —
                    // stopping through the ordinary main-read JournalRemoved arm with
                    // correct accounting. No heap surgery of any kind is needed.
                    service_kit::event!(
                        tracing::Level::INFO,
                        "backfill",
                        session = self.topology.session_id,
                        read_id,
                        binding = binding.index,
                        journal = self.reads[read_id].journal.to_string(),
                        producer = service_kit::event::debug(backfill.target),
                        "backfill journal removed ({}); completing as implicit EOF",
                        status.as_str_name(),
                    );
                    self.complete_backfill(backfill);
                    return Ok(());
                }
                read::ReadFailure::Transient(err) => {
                    service_kit::event!(
                        tracing::Level::WARN,
                        "backfill",
                        read_id,
                        binding = binding.index,
                        journal = self.reads[read_id].journal.to_string(),
                        attempt,
                        err = service_kit::event::debug(err),
                        "transient error during backfill read (will retry)",
                    );
                    // Retry: leave `io` in `Reading`; the `select!` arm re-polls
                    // the same stream on the next iteration.
                    self.backfill = Some(backfill);
                    return Ok(());
                }
                read::ReadFailure::Terminal(err) => {
                    // Fail-fast: the whole session tears down. The teardown is
                    // the signal, so no dedicated event fires — but restore the
                    // backfill so `Drop` accounting still counts it as stopped.
                    self.backfill = Some(backfill);
                    return Err(read::map_read_error(
                        err,
                        &self.reads[read_id].journal,
                        binding.state_key(),
                        "reading backfill lines",
                    ));
                }
            },
            Ok(lines_batch) => lines_batch,
        };

        // Physical bytes fetched, including bytes skipped for other producers.
        // Counted into the shared `bytes_read` total, but NEVER into the
        // main-read `write_head` or offset baselines (which stay purely
        // forward-read, so main-read byte deltas remain monotonic).
        let n = lines_batch.content.len() as u64;
        self.metrics.bytes_read.increment(n);
        backfill.physical_bytes += n;

        // Move the read out of `Reading` to build the batch cursor (which owns it
        // as `inner`) and transition to `Draining`. A batch resolves only while
        // `Reading`, so the pattern is irrefutable in practice.
        let BackfillIo::Reading(read) = backfill.io else {
            unreachable!("a batch resolves only while `Reading`");
        };
        let ready_read = match read::parse_lines_batch(
            &mut self.parser,
            &mut self.validators[self.reads[read_id].binding_index as usize],
            binding,
            &self.reads[read_id].journal,
            read,
            lines_batch,
            "transcoding backfill documents",
        ) {
            Ok(ready_read) => ready_read,
            Err(err) => {
                // The historical read was consumed, so the `Backfill` cannot be
                // restored for `Drop` accounting; count it as stopped here. The
                // triggering main read is counted separately at `Drop` via the
                // ready heap, where its trigger still sits.
                self.metrics.backfills_stopped.increment(1);
                return Err(err);
            }
        };
        backfill.io = BackfillIo::Draining(Box::new(ready_read));
        self.backfill = Some(backfill);

        Ok(())
    }
}

/// Await the next historical backfill batch, but only while a backfill is
/// actively `Reading`; otherwise this future is pending, so the `select!` arm
/// that drives it never fires. The read is left in place (borrowed, not moved),
/// so dropping this future on a losing `select!` branch is cancellation-safe and
/// re-polls the same stream next iteration. A resolved batch is handed to
/// `process_backfill_result`, which moves the read into a `Draining` cursor.
pub(super) async fn next_backfill_batch(
    backfill: &mut Option<Backfill>,
) -> Option<gazette::RetryResult<gazette::journal::read::LinesBatch>> {
    match backfill {
        Some(Backfill {
            io: BackfillIo::Reading(read),
            ..
        }) => read.next().await,
        // No backfill, or one that is `Draining` (drained by `try_drain_backfill`,
        // not re-polled until its batch is exhausted): never resolves.
        _ => std::future::pending().await,
    }
}

/// Advance a `Draining` batch to its next buffered document, or (when the batch
/// is exhausted) return the inner historical read to `Reading` so the `select!`
/// arm re-polls it — only now, so no batch is pending when the stream reaches its
/// end. Mirrors the main-read drain's tail advance, but for the cursor rather
/// than the heap. The just-consumed head is dropped. Consumes and returns `io`;
/// the caller must be in `Draining` (asserted).
fn advance_backfill_cursor(io: BackfillIo) -> BackfillIo {
    let BackfillIo::Draining(cursor) = io else {
        unreachable!("advance is called only while draining a batch");
    };
    let ReadyRead {
        inner: read,
        doc: _consumed_doc,
        meta: _consumed_meta,
        mut doc_tail,
        mut meta_tail,
    } = *cursor;

    match (doc_tail.next(), meta_tail.next()) {
        (Some((doc, _)), Some(meta)) => BackfillIo::Draining(Box::new(ReadyRead {
            doc,
            meta,
            doc_tail,
            meta_tail,
            inner: read,
        })),
        (None, None) => BackfillIo::Reading(read),
        _ => unreachable!("doc_tail and meta_tail have equal length"),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testing::test_binding;
    use proto_gazette::uuid::{Clock, Flags, Producer};

    const CONTINUE: Flags = Flags(proto_gazette::message_flags::CONTINUE_TXN as u16);
    const ACK: Flags = Flags(proto_gazette::message_flags::ACK_TXN as u16);

    fn producer(id: u8) -> Producer {
        Producer::from_bytes([id | 0x01, 0, 0, 0, 0, 0])
    }

    fn meta(
        producer: Producer,
        clock: Clock,
        flags: Flags,
        begin_offset: i64,
        end_offset: i64,
    ) -> Meta {
        Meta {
            producer,
            clock,
            flags,
            begin_offset,
            end_offset,
        }
    }

    #[test]
    fn test_backfill_read_request() {
        // The bounded historical read reads `[F, trigger_begin)` non-blocking,
        // with the main read's partition-filtered journal and `begin_mod_time`.
        let binding = test_binding(0, true, None, "/suffix");
        let req = backfill_read_request("test/journal", &binding, 300, 500);

        assert_eq!(
            req.journal,
            format!("test/journal;{}", binding.journal_read_suffix),
        );
        assert_eq!(req.offset, 300, "reads from F");
        assert_eq!(req.end_offset, 500, "exclusive end at ack begin");
        assert!(!req.block, "bounded historical read never blocks");
        assert!(req.do_not_proxy);
        assert_eq!(req.begin_mod_time, binding.not_before.to_unix().0 as i64);
        assert_eq!(req.min_etcd_revision, 0);
        assert!(
            req.header.is_none(),
            "no write-head probe for a bounded range"
        );
    }

    #[test]
    fn test_backfill_one_transaction_invariant() {
        // `try_drain_backfill` sequences target-producer documents against the
        // reconstructed span with `state::sequence_producer` and enforces the
        // one-transaction invariant inline: it bails when the sequenced document
        // commits, because a committing document inside `[F, trigger.begin)`
        // contradicts the recovered checkpoint's single open span (spec
        // §One-transaction invariant). This exercises that commit/no-commit
        // decision boundary the inline guard keys off.
        let binding = test_binding(0, true, None, "/suffix");
        let p = producer(0x01);

        // The reconstructed span installed at trigger time: F = 200.
        let span = ProducerState {
            last_commit: Clock::from_u64(100),
            max_continue: Clock::zero(),
            offset: 200,
            gapped: false,
        };

        // A CONTINUE extends the reconstructed span — not a commit, so the drain
        // does not bail and would append it.
        let s = state::sequence_producer(
            span,
            "test/journal",
            &binding,
            &meta(p, Clock::from_u64(150), CONTINUE, 200, 210),
        )
        .expect("sequencing a CONTINUE succeeds");
        assert!(!s.is_commit, "a CONTINUE within the range never bails");

        // A committing ACK within the range sets is_commit — the inline guard in
        // `try_drain_backfill` bails on exactly this.
        let s = state::sequence_producer(
            s.producer_state,
            "test/journal",
            &binding,
            &meta(p, Clock::from_u64(150), ACK, 210, 220),
        )
        .expect("sequencing an ACK succeeds");
        assert!(
            s.is_commit,
            "a committing ACK inside the range trips the one-transaction guard",
        );
    }
}
