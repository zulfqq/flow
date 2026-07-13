//! Gapped-producer recovery: state, pure transitions, and actor orchestration.
//!
//! On restart, a `(binding, journal)` read starts at the furthest journal
//! position justified by its checkpoint: the maximum offset magnitude `M`
//! across producer entries. An uncommitted producer span whose begin offset
//! `F` falls before `M` is *gapped*: the main read skips `[F, M)` and the
//! producer is frozen until it resolves. See
//! `plans/shuffle-gapped-restart.md` §Producer state machine.
//!
//! `ReadState::gaps` is a plain `producer -> F` map: a gapped producer's frozen
//! `ProducerState` lives in `settled` (its source of truth for `last_commit`)
//! and the map only pins the skipped span's begin offset `F`. The first newer
//! main-read document of a gapped producer (a CONTINUE or ACK with `clock >
//! last_commit`) is the *trigger*: the main read parks at it and the actor opens
//! a bounded historical read of `[F, trigger.begin)`. All live sequencing state
//! for the recovery is consolidated in a single actor-owned `Backfill`
//! (`SliceActor::backfill`). The gap entry is retained for the whole backfill
//! (its pinned `F` is read at completion for the range event); while parked it is
//! inert, as the main read produces no documents.
//!
//! A backfill blocks all main-read → Log I/O for the whole Slice until it
//! completes: triggers are discovered only by sequencing the ready heap's top,
//! and the heap does not drain while a backfill is active (see the gate in
//! `SliceActor::try_log_request_tx`). This makes **at most one backfill per
//! Slice, globally**, a structural invariant — no second trigger can be reached
//! while one is in flight. It parallels both the all-reads-tailing stalled-read
//! gate and the legacy conservative restart (which re-read `[F, …)` on a
//! non-tailing main read, blocking all draining anyway).
//!
//! This module is organized like `state.rs`: a pure decision layer
//! (`begin_backfill`, `backfill_read_request`, `sequence_backfill_document` —
//! directly unit-tested) and an orchestration layer (an `impl SliceActor`
//! extension block driving the backfill lifecycle around those pure routines).
//! A gapped producer's main-read documents are *classified* in
//! `state::sequence_document` (which returns `Sequenced::Park` for the trigger
//! and folds the other gapped outcomes into a plain `SequencedDoc`); the drain
//! loop applies that decision, calling `park_backfill` then `start_backfill` on
//! a trigger and resolving/removing the gap post-pop for a rollback or newer
//! OUTSIDE commit. The actor owns the IO objects (parked main read, in-flight
//! historical read, batch cursor); `ReadState` holds only the pinned `F`, which
//! keeps it snapshot-testable, following the crate's decomposition convention.
//!
//! Backfill *completion commits nothing* — the core simplification. When the
//! historical read reaches `trigger.begin`, the reconstructed span (`live`) is
//! installed into the read's `pending` map, the gap is removed, and the parked
//! trigger is re-presented to the ready heap with its head document unconsumed.
//! The drain loop re-pops it and sequences it through the wholly-normal path:
//! the producer is no longer gapped, so a CONTINUE trigger extends the span and
//! appends, while an ACK trigger commits it — causal hints, committed offset,
//! and flush — via the existing normal-path code. Installing `live` is safe
//! because its `offset` is the span begin `F`, identical to what the durable
//! checkpoint already records, so a flush carrying it changes nothing durably;
//! an open uncommitted span is a state every existing mechanism already handles,
//! and visibility stays gated by the absence of an ACK exactly as for any normal
//! mid-span producer. There is thus no atomic visibility boundary to enforce:
//! transaction atomicity is inherited from normal ACK-gated visibility, and a
//! crash before the eventual ACK flush recovers the unchanged positive `F`,
//! re-creates the gap, and repeats idempotently.
//!
//! Backfilled documents do NOT flow through the ready heap; they are appended
//! directly. The historical-vs-trigger direct-append justification is that the
//! trigger was sequenced to `Sequenced::Park` only after clearing the drain
//! loop's clock-delay gate, so — same binding read-delay, and strictly-ascending
//! per-producer clocks — every historical CONTINUE in `[F, trigger.begin)` has
//! an adjusted clock already in the past. Heap ordering, priority ordering, and
//! clock-delay gating are therefore provably no-ops for them; only the
//! Log-append semantics matter (per-producer journal order preserved, and the
//! one-transaction invariant enforced: no committing document appears inside the
//! historical range).
//!
//! Ordering *relative to other journals* is trivially preserved because the
//! Slice blocks all other draining for the backfill's duration: no other
//! journal's document is appended while the backfill runs, so none can overtake
//! the parked trigger. The only residual, user-visible relaxation is the one
//! inherent to any recovery — backfilled documents are older than documents
//! already appended before the restart — and that inversion window no longer
//! grows during the backfill.

use super::actor::{Buffers, SliceActor};
use super::heap::ReadyReadEntry;
use super::producer::ProducerState;
use super::read::{self, Meta, ReadyRead};
use super::state;
use futures::StreamExt;
use proto_flow::shuffle;
use proto_gazette::{broker, uuid};
use tokio::sync::mpsc;

/// All live state for the single active backfill of a gapped producer's pending
/// transaction, owned by the actor in `SliceActor::backfill`. Consolidates what
/// the heap-routed design split across three homes: the parked main read, the
/// live target-producer sequencing state, and the in-flight historical read.
/// At most one backfill exists per Slice, globally — a structural invariant, not
/// a per-read one: the heap does not drain while a backfill is active, so no
/// second trigger can be discovered until this one completes.
///
/// While the backfill runs the producer's `ReadState::gaps` entry is retained
/// but inert: the main read is parked, so no main-read document of the journal
/// arrives and the pinned `F` cannot change. It is removed only at completion,
/// rollback/OUTSIDE resolution, or release.
pub struct Backfill {
    /// Read id of the parked main read (index into `SliceActor::reads`).
    pub read_key: u32,
    /// The shelved main read, its head document the *trigger* (the gapped
    /// producer's first newer CONTINUE or ACK). Held here (outside
    /// `pending_reads`), so no later document of the journal is reachable while
    /// parked, and — because a backfill blocks all draining — no other journal's
    /// document is appended ahead of it. At completion it is re-presented to the
    /// ready heap with its head unconsumed (`parked.meta` IS the trigger) and
    /// sequenced through the normal path.
    pub parked: Box<ReadyRead>,
    /// The gapped producer whose newer document triggered this backfill.
    pub target: uuid::Producer,
    /// Live sequencing state for historical target-producer documents,
    /// reconstructed from the recovered `{last_commit, max_continue: 0, offset:
    /// F}`. At completion it is installed into the read's `pending` map as the
    /// reconstructed open span. That is safe because its `offset` stays at the
    /// span begin `F` (the durable checkpoint already records `F`), so any flush
    /// carrying it changes nothing durably and visibility stays gated by the
    /// absence of an ACK — exactly as for any normal mid-span producer.
    pub live: ProducerState,
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

/// Reconstruct a triggered backfill's live sequencing state from the recovered
/// checkpoint: the pending span begins at `F` (`gap_begin`) with the recovered
/// `last_commit` and `max_continue: 0`. Installed into `pending` at completion
/// as the reconstructed open span; its `offset` stays at `F`, so a flush
/// carrying it matches the durable checkpoint and changes nothing.
pub(super) fn begin_backfill(gap_begin: i64, recovered_last_commit: uuid::Clock) -> ProducerState {
    ProducerState {
        last_commit: recovered_last_commit,
        max_continue: uuid::Clock::zero(),
        offset: gap_begin,
    }
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

/// Sequence a historical backfill document of the target producer against the
/// gap's `live` state, enforcing the one-transaction invariant: the recovered
/// span holds no committed transaction boundary, so any commit inside
/// `[F, trigger.begin)` contradicts the recovered checkpoint. For a CONTINUE
/// trigger this holds because `[F, M)` has no target-producer ACK by checkpoint
/// closure and `[M, trigger.begin)` has no target document at all (the read
/// parked at the first one).
pub(super) fn sequence_backfill_document(
    live: ProducerState,
    journal: &str,
    binding: &crate::Binding,
    meta: &Meta,
) -> anyhow::Result<state::SequencedDoc> {
    let sequenced = state::sequence_producer(live, journal, binding, meta)?;

    // One-transaction invariant: the recovered span holds no committed
    // transaction boundary, so any commit inside `[F, trigger.begin)`
    // contradicts the recovered checkpoint.
    if sequenced.is_commit {
        anyhow::bail!(
            "backfill of journal {} (binding {}) hit an unexpected committing document at \
             offset {} for target producer {:?}: a distinct transaction boundary inside the \
             historical range contradicts the recovered checkpoint",
            journal,
            binding.state_key(),
            meta.begin_offset,
            meta.producer,
        );
    }

    Ok(sequenced)
}

/// The parked trigger and reconstructed sequencing state produced by
/// `park_backfill` and consumed by `start_backfill` to assemble the `Backfill`.
/// The drain loop calls the two back-to-back, so this never crosses an await;
/// it exists only because a `Backfill` cannot be constructed until
/// `start_backfill` opens the historical read that its `io` field owns.
pub(super) struct ParkedTrigger {
    /// The shelved main read; `parked.meta` is the trigger.
    parked: Box<ReadyRead>,
    /// The gapped producer whose newer document triggered the backfill.
    target: uuid::Producer,
    /// Reconstructed live sequencing state; `live.offset` is the span begin `F`.
    live: ProducerState,
}

impl SliceActor {
    /// Park the main read at a gapped producer's *trigger* (its first newer
    /// CONTINUE or ACK): pop and shelve the whole `ReadyRead` (its head IS the
    /// trigger, and no later document of the journal is reachable while parked),
    /// and reconstruct the live sequencing state. The trigger is NOT sequenced
    /// now; at completion it is re-presented to the ready heap and sequenced
    /// through the normal path against the reconstructed span (spec §Trigger and
    /// parking).
    ///
    /// Paired with `start_backfill`, which opens the historical read and installs
    /// the `Backfill`: the drain loop calls the two back-to-back. Because a
    /// backfill blocks all heap draining, `Sequenced::Park` is only ever reached
    /// while no backfill is active, so a fresh one can always be installed.
    pub(super) fn park_backfill(&mut self, read_key: u32, meta: &Meta) -> ParkedTrigger {
        let read_id = read_key as usize;

        let parked = self.ready_read_heap.pop().unwrap().inner.unwrap();

        // The gap entry stays in `ReadState::gaps` for the whole backfill (its
        // pinned `F` is read at completion for the range event; removed at
        // completion or on release); while parked it's inert because no main-read
        // document of the journal arrives.
        let gap_begin = *self.reads[read_id]
            .gaps
            .get(&meta.producer)
            .expect("producer is gapped");
        let recovered_last_commit = self.reads[read_id]
            .settled
            .get(&meta.producer)
            .map(|ps| ps.last_commit)
            .unwrap_or_default();

        let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];
        service_kit::event!(
            tracing::Level::INFO,
            "backfill",
            session = self.topology.session_id,
            read_id,
            binding = binding.index,
            journal = self.reads[read_id].journal.to_string(),
            producer = service_kit::event::debug(meta.producer),
            gap_begin, // F
            trigger_begin = meta.begin_offset,
            trigger_is_ack = meta.flags == uuid::Flags::ACK_TXN,
            "triggering backfill of gapped producer's pending transaction",
        );
        self.metrics.backfills_started.increment(1);

        // `live` reconstructs the pending span from the recovered state and is
        // installed into `pending` at completion.
        ParkedTrigger {
            parked,
            target: meta.producer,
            live: begin_backfill(gap_begin, recovered_last_commit),
        }
    }

    /// Open the historical read half of a triggered backfill parked by
    /// `park_backfill` and install the single actor-owned `Backfill`: a bounded,
    /// non-blocking read of `[F, trigger.begin)` held in `BackfillIo::Reading`.
    pub(super) fn start_backfill(
        &mut self,
        read_key: u32,
        parked: ParkedTrigger,
    ) -> anyhow::Result<()> {
        let read_id = read_key as usize;
        let ParkedTrigger {
            parked,
            target,
            live,
        } = parked;

        let gap_begin = live.offset; // F
        let trigger_begin = parked.meta.begin_offset;

        // The range is always non-empty: `F < M <= trigger_begin`. The main read
        // starts at M and parks at the first document it reaches, so the trigger
        // is at or after M, while a gapped span begin F is strictly below M.
        assert!(
            gap_begin < trigger_begin,
            "backfill range [{gap_begin}, {trigger_begin}) must be non-empty: F precedes M, \
             which is at-or-below the trigger",
        );

        // Start the bounded, non-blocking historical read. Same client, auth,
        // begin_mod_time, schema validation, and partition-filtered journal as
        // the main read; no write-head probe is needed for a bounded range. The
        // read carries the plain `read_key` as its id: the historical read never
        // shares the heap or `pending_reads` with main reads.
        let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];
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
            parked,
            target,
            live,
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
    /// cursor parks at that document). Backfill documents never touch the main
    /// read's offset baselines or `pending`/`settled` (spec §Read positions,
    /// §Completion).
    ///
    /// Called from `try_log_request_tx` after the flush-priority check and
    /// independent of the ready heap and its tailing gate (which the backfill
    /// blocks entirely). Returns `Some(tx)` when an Append channel lacked
    /// capacity — the caller wakes on `tx` and retries — or `None` otherwise.
    ///
    /// Sequences from the same `live` snapshot on each attempt, advancing `live`
    /// only after the append is sent, so a retry after a full channel doesn't
    /// double-sequence. Takes ownership of the single `Backfill` for the duration
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

        let read_state = &self.reads[backfill.read_key as usize];
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

            let sequenced = match sequence_backfill_document(
                backfill.live.clone(),
                &read_state.journal,
                binding,
                &meta,
            ) {
                Ok(sequenced) => sequenced,
                Err(err) => {
                    // Restore before fail-fast so teardown's `Drop` accounting
                    // still counts this backfill and its parked read as stopped.
                    self.backfill = Some(backfill);
                    return Err(err);
                }
            };

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
                    // Park the cursor here; `live` is unchanged so the wake
                    // re-sequences this document from the same snapshot.
                    self.backfill = Some(backfill);
                    return Ok(Some(tx));
                }
            }

            // Commit `live` forward (never `pending`/`settled`) and advance.
            backfill.live = sequenced.producer_state;
            backfill.io = advance_backfill_cursor(backfill.io);
        }

        self.backfill = Some(backfill);
        Ok(None)
    }

    /// Complete a backfill once its historical range has been fully read.
    /// Completion commits nothing (spec §Completion): it installs the
    /// reconstructed open span (`live`) into `pending`, removes the gap, and
    /// re-presents the parked trigger to the ready heap with its head document
    /// unconsumed. The drain loop re-pops it and sequences it through the
    /// wholly-normal path — a CONTINUE extends the span and appends, an ACK
    /// commits it (causal hints, committed offset, flush) — now that the
    /// producer is no longer gapped.
    fn complete_backfill(&mut self, backfill: Backfill) -> anyhow::Result<()> {
        let Backfill {
            read_key,
            parked,
            target,
            live,
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

        // Drop the gap now that its pending span is recovered. It was retained
        // for the whole backfill so the release paths stay unchanged; `gap_begin`
        // (F) is still needed for the range metric.
        let gap_begin = self.reads[read_id]
            .gaps
            .remove(&target)
            .expect("backfilling producer retains its gap");

        // Computed before `live` is installed. An empty reconstructed span (no
        // target-producer CONTINUEs found) is expected only when historical
        // content was unavailable or filtered — a suspiciously short backfill —
        // or, benignly, for a hint-only producer backfilled from F = 0.
        let span_empty = live.max_continue == uuid::Clock::zero();

        let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];
        let read_delay = binding.read_delay;
        let priority = binding.priority;

        // `parked.meta` is the trigger; copy it out (`Meta` is Copy) for both the
        // range metric and the re-presented heap entry's ordering keys.
        let trigger = parked.meta;

        // Install the reconstructed open span into `pending`. This is why there
        // is no atomic visibility boundary to enforce: `live.offset` is the span
        // begin `F`, identical to the durable checkpoint's record, so a flush
        // carrying it changes nothing durably, and visibility stays gated by the
        // absence of an ACK — exactly as for any normal mid-span producer. A
        // crash before the eventual ACK flush recovers the unchanged positive
        // `F`, re-creates the gap, and repeats idempotently. Completion arms no
        // flush and touches neither `read_offset` nor causal hints: those all
        // follow from the normal path when the trigger is re-sequenced below.
        _ = self.reads[read_id].pending.insert(target, live);

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
            range_bytes = trigger.begin_offset - gap_begin,
            physical_bytes,
            duration_ms = elapsed.as_millis() as u64,
            span_empty, // true flags a suspiciously short backfill (fragment loss)
            "completed backfill of gapped producer's transaction",
        );

        // Re-present the parked trigger to the ready heap with its head document
        // unconsumed. The clock-delay gate re-clears trivially (the trigger
        // cleared it once already), and the drain loop sequences it through the
        // normal path against the now-installed reconstructed span.
        self.ready_read_heap.push(ReadyReadEntry {
            priority,
            adjusted_clock: trigger.clock + read_delay,
            inner: Some(parked),
        });

        Ok(())
    }

    /// Process the active backfill's historical read resolution, yielded by the
    /// `select!` arm via `next_backfill_batch`. Mirrors `process_read_result` but
    /// never touches main-read offset baselines or `write_head`, counts physical
    /// bytes into the backfill counter, transitions a resolved batch to
    /// `BackfillIo::Draining` (drained by `try_drain_backfill`), and on stream
    /// end completes the backfill.
    ///
    /// Takes ownership of the `Backfill` for the duration; puts it back unless
    /// the stream ended (completion consumes it) or the journal was removed (a
    /// benign stop that also unblocks the Slice).
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
            // recovered Append precedes the final ACK flush on each Log channel.
            // Complete the backfill (which consumes `backfill`).
            return self.complete_backfill(backfill);
        };

        let lines_batch = match result {
            Err(gazette::RetryError {
                attempt,
                inner: err,
            }) => match read::classify_read_failure(err) {
                read::ReadFailure::JournalRemoved(status) => {
                    // Deletion or FULL suspension implies no fragments remain, so
                    // the range can never be recovered. This stops the backfill
                    // benignly, exactly as an EOF stops a main read — not a
                    // failure — and, because the backfill blocked the Slice, also
                    // unblocks it. Count it as completed (no longer in flight) and
                    // drop the `Backfill`, which releases the parked main read and
                    // (by dropping `io`) cancels the historical stream. The trigger
                    // is NOT re-presented and no progress is committed. The read is
                    // now discarded, so its `gaps` are left inert (as on a
                    // main-read EOF); its slot is never reused.
                    service_kit::event!(
                        tracing::Level::INFO,
                        "backfill",
                        read_id,
                        binding = binding.index,
                        journal = self.reads[read_id].journal.to_string(),
                        producer = service_kit::event::debug(backfill.target),
                        "backfill journal removed ({}); stopping backfill",
                        status.as_str_name(),
                    );
                    // The dropped `Backfill` releases the parked main read too,
                    // which would otherwise stop uncounted (its own removal is
                    // never observed: it is parked, not polled).
                    self.metrics.reads_stopped.increment(1);
                    self.metrics.backfills_stopped.increment(1);
                    return Ok(()); // `backfill` dropped here.
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
                    // backfill so `Drop` accounting still counts it (and its
                    // parked read) as stopped.
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
                // restored for `Drop` accounting; count it and its parked read
                // as stopped here, as a main read's terminal path does.
                self.metrics.reads_stopped.increment(1);
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
    fn test_begin_backfill() {
        // The open span is reconstructed from the recovered state: `live` starts
        // at `{recovered last_commit, max_continue: 0, offset: F}`, and is
        // installed into `pending` unchanged at completion.
        let last_commit = Clock::from_u64(100);
        let live = begin_backfill(300, last_commit);

        assert_eq!(live.last_commit, last_commit);
        assert_eq!(live.max_continue, Clock::zero());
        assert_eq!(live.offset, 300, "live offset reconstructs the span from F");
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
    fn test_backfill_intermediate_commit_is_detected() {
        // `sequence_backfill_document` sequences target-producer documents
        // against the gap's `live` state and bails when `sequence_producer`
        // reports a commit — a committing document inside the historical range
        // contradicts the recovered checkpoint's single open span (spec
        // §One-transaction invariant).
        let binding = test_binding(0, true, None, "/suffix");
        let p = producer(0x01);

        let live = ProducerState {
            last_commit: Clock::from_u64(100),
            max_continue: Clock::zero(),
            offset: 200,
        };
        // A CONTINUE extends the reconstructed span — not a commit.
        let s = sequence_backfill_document(
            live,
            "test/journal",
            &binding,
            &meta(p, Clock::from_u64(150), CONTINUE, 200, 210),
        )
        .expect("CONTINUE within the range is not a commit");
        assert!(!s.is_commit);

        // A committing ACK within the range is a terminal consistency error.
        let err = sequence_backfill_document(
            s.producer_state,
            "test/journal",
            &binding,
            &meta(p, Clock::from_u64(150), ACK, 210, 220),
        )
        .expect_err("a committing ACK inside the range is detected and bails");
        assert!(
            err.to_string().contains("unexpected committing document"),
            "unexpected error: {err}",
        );
    }
}
