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
//! for that one active recovery is consolidated in a `Backfill` (owned by the
//! actor in `SliceActor::backfills`, keyed by read id). The gap entry is
//! retained for the whole backfill (its pinned `F` is read at completion for the
//! range event); while parked it is inert, as the main read produces no
//! documents.
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
//! Backfilled documents do NOT flow through the ready heap. The trigger was
//! sequenced to `Sequenced::Park` only after clearing the drain loop's
//! clock-delay gate, so — same binding read-delay, and strictly-ascending
//! per-producer clocks — every historical CONTINUE in `[F, trigger.begin)` has
//! an adjusted clock already in the past. Heap ordering, priority ordering, and
//! clock-delay gating are therefore provably no-ops for them; only the
//! Log-append semantics matter (per-producer journal order preserved, and the
//! one-transaction invariant enforced: no committing document appears inside the
//! historical range).

use super::actor::{Buffers, SliceActor};
use super::heap::ReadyReadEntry;
use super::producer::ProducerState;
use super::read::{self, Meta, ReadyRead};
use super::state;
use futures::StreamExt;
use proto_flow::shuffle;
use proto_gazette::{broker, uuid};
use tokio::sync::mpsc;

/// All live state for one active backfill of a gapped producer's pending
/// transaction, owned by the actor in `SliceActor::backfills` and keyed by read
/// id. Consolidates what the heap-routed design split across three homes: the
/// parked main read, the live target-producer sequencing state, and the
/// in-flight historical read. At most one backfill is active per read (the read
/// parks at the first trigger, so no second trigger can arrive while parked).
///
/// While the backfill runs the producer's `ReadState::gaps` entry is retained
/// but inert: the main read is parked, so no main-read document of the journal
/// arrives and the pinned `F` cannot change. It is removed only at completion,
/// rollback/OUTSIDE resolution, or release.
pub struct Backfill {
    /// The shelved main read, its head document the *trigger* (the gapped
    /// producer's first newer CONTINUE or ACK). Held here (outside
    /// `pending_reads`), so it's exempt from the tailing gate and stall
    /// accounting and no later document of the journal is reachable while
    /// parked. At completion it is re-presented to the ready heap with its head
    /// unconsumed (`parked.meta` IS the trigger) and sequenced through the
    /// normal path.
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
    /// The historical batch currently being drained, if any. Its inner
    /// `ReadLines` is returned to `pending_backfills` (re-polled) only once this
    /// cursor is fully drained, so no cursor is ever pending when the stream
    /// reaches its end.
    pub cursor: Option<Box<ReadyRead>>,
    /// Trigger instant, for the backfill-duration histogram.
    pub started_at: std::time::Instant,
    /// Physical bytes fetched so far by the historical read, for the
    /// completion event.
    pub physical_bytes: u64,
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

impl SliceActor {
    /// Park the main read at a gapped producer's *trigger* (its first newer
    /// CONTINUE or ACK): pop and shelve the whole `ReadyRead` (its head IS the
    /// trigger, and no later document of the journal is reachable while parked),
    /// and consolidate all live recovery state in a `Backfill`. The trigger is
    /// NOT sequenced now; at completion it is re-presented to the ready heap and
    /// sequenced through the normal path against the reconstructed span (spec
    /// §Trigger and parking).
    ///
    /// Paired with `start_backfill`: the drain loop calls the two back-to-back,
    /// so the transient parked-but-unstarted `Backfill` never survives across an
    /// await.
    pub(super) fn park_backfill(&mut self, read_key: u32, meta: &Meta) {
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

        // All live backfill state is consolidated here; `live` reconstructs the
        // pending span from the recovered state and is installed into `pending`
        // at completion.
        _ = self.backfills.insert(
            read_key,
            Backfill {
                parked,
                target: meta.producer,
                live: begin_backfill(gap_begin, recovered_last_commit),
                cursor: None,
                started_at: std::time::Instant::now(),
                physical_bytes: 0,
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
            producer = service_kit::event::debug(meta.producer),
            gap_begin, // F
            trigger_begin = meta.begin_offset,
            trigger_is_ack = meta.flags == uuid::Flags::ACK_TXN,
            "triggering backfill of gapped producer's pending transaction",
        );
        self.metrics.backfills_started.increment(1);
    }

    /// Begin the historical read half of a triggered backfill parked by
    /// `park_backfill`: open the bounded, non-blocking historical read of
    /// `[F, trigger.begin)` in `pending_backfills`.
    pub(super) fn start_backfill(&mut self, read_key: u32) -> anyhow::Result<()> {
        let read_id = read_key as usize;

        // `F` and the trigger's begin are read back from the just-parked
        // `Backfill`: `live.offset` is still F (no historical document sequenced
        // yet) and `parked.meta` is the trigger.
        let backfill = self.backfills.get(&read_key).expect("backfill was parked");
        let gap_begin = backfill.live.offset; // F
        let trigger_begin = backfill.parked.meta.begin_offset;

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
        // read carries the plain `read_key` as its id: historical reads never
        // share the heap or `pending_reads` with main reads.
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
        self.pending_backfills.push(read.into_future());

        Ok(())
    }

    /// Drain every active backfill's batch cursor as far as it will go: append
    /// recovered target-producer documents in journal order and skip others,
    /// until each batch is exhausted (its historical read returns to
    /// `pending_backfills`) or an Append channel lacks capacity (the cursor parks
    /// at that document). Backfill documents never touch the main read's offset
    /// baselines or `pending`/`settled` (spec §Read positions, §Completion).
    ///
    /// Called from `try_log_request_tx` after the flush-priority check and
    /// independent of the ready heap and its tailing gate. Returns `Some(tx)`
    /// when an Append channel lacked capacity — the caller wakes on `tx` and
    /// retries — or `None` when all cursors are drained.
    ///
    /// Destructuring `self` into disjoint field borrows lets us mutate one
    /// `Backfill` in place (via `iter_mut`) while reading `reads`/`topology` and
    /// the Log channels — so no per-call key allocation is needed. Drain order
    /// among concurrent backfills of different reads is arbitrary; there is no
    /// cross-backfill ordering requirement.
    pub(super) fn try_drain_backfills(
        &mut self,
        buffers: &mut Buffers,
    ) -> anyhow::Result<Option<mpsc::Sender<shuffle::LogRequest>>> {
        let Self {
            backfills,
            reads,
            topology,
            log_prev_journal,
            log_request_tx,
            pending_backfills,
            ..
        } = self;

        for (&read_key, backfill) in backfills.iter_mut() {
            let read_state = &reads[read_key as usize];
            let binding = &topology.bindings[read_state.binding_index as usize];

            loop {
                // Peek the cursor's head, retained until its append succeeds so a
                // retry after a full channel doesn't drop it.
                let Some(cursor) = backfill.cursor.as_deref() else {
                    break; // No batch to drain (drained, or not yet fetched).
                };
                let meta = cursor.meta; // `Meta` is Copy.

                // Documents of other producers are already represented by
                // checkpoint state or belong to independent gaps: skip without
                // sequencing, state mutation, key extraction, or append.
                if meta.producer != backfill.target {
                    advance_backfill_cursor(backfill, pending_backfills);
                    continue;
                }

                // Sequence the target's document against `live` from the SAME
                // snapshot on each (re)attempt: `live` advances only after the
                // append is sent, so a retry after a full channel doesn't
                // double-sequence.
                let sequenced = sequence_backfill_document(
                    backfill.live.clone(),
                    &read_state.journal,
                    binding,
                    &meta,
                )?;

                if sequenced.is_append {
                    let cursor = backfill.cursor.as_deref().unwrap();
                    if let Err(tx) = Self::try_log_request_append_tx(
                        binding,
                        buffers,
                        &read_state.journal,
                        &topology.shards,
                        log_prev_journal,
                        log_request_tx,
                        cursor,
                    ) {
                        // Park the cursor here; `live` is unchanged so the wake
                        // re-sequences this document from the same snapshot.
                        return Ok(Some(tx));
                    }
                }

                // Commit `live` forward (never `pending`/`settled`) and advance.
                backfill.live = sequenced.producer_state;
                advance_backfill_cursor(backfill, pending_backfills);
            }
        }
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
    fn complete_backfill(&mut self, read_key: u32) -> anyhow::Result<()> {
        let read_id = read_key as usize;
        let Backfill {
            parked,
            target,
            live,
            cursor,
            started_at,
            physical_bytes,
        } = self
            .backfills
            .remove(&read_key)
            .expect("completing backfill has state");
        debug_assert!(
            cursor.is_none(),
            "the historical read reaches its end only after its final batch cursor is drained",
        );

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

    /// Process a historical backfill read's resolution. Mirrors
    /// `process_read_result` but never touches main-read offset baselines or
    /// `write_head`, counts physical bytes into the backfill counter, stashes a
    /// resolved batch as the backfill's cursor (drained by `try_drain_backfills`),
    /// and on stream end completes the backfill.
    pub(super) fn process_backfill_result(
        &mut self,
        result: Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
        read: super::ReadLines,
    ) -> anyhow::Result<()> {
        let read_key = read.id();
        let read_id = read_key as usize;
        let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];
        let journal = read.fragment().journal.clone();

        let Some(result) = result else {
            // The bounded stream reached `end_offset`. A batch cursor is returned
            // to `pending_backfills` only once fully drained, so no cursor is
            // pending here and every recovered Append precedes the final ACK
            // flush on each Log channel. Complete the backfill.
            return self.complete_backfill(read_key);
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
                    // failure. Count it as completed (no longer in flight) and
                    // drop the `Backfill`, which releases the parked main read
                    // and (by dropping `read`) cancels the historical stream. The
                    // trigger is NOT re-presented and no progress is committed.
                    // The read is now discarded, so its `gaps` are left inert (as
                    // on a main-read EOF); its slot is never reused.
                    let target = self.backfills[&read_key].target;
                    service_kit::event!(
                        tracing::Level::INFO,
                        "backfill",
                        read_id,
                        binding = binding.index,
                        journal,
                        producer = service_kit::event::debug(target),
                        "backfill journal removed ({}); stopping backfill",
                        status.as_str_name(),
                    );
                    _ = self.backfills.remove(&read_key);
                    self.metrics.backfills_stopped.increment(1);
                    return Ok(());
                }
                read::ReadFailure::Transient(err) => {
                    service_kit::event!(
                        tracing::Level::WARN,
                        "backfill",
                        read_id,
                        binding = binding.index,
                        journal,
                        attempt,
                        err = service_kit::event::debug(err),
                        "transient error during backfill read (will retry)",
                    );
                    // Retry: hand the read straight back to `pending_backfills`.
                    self.pending_backfills.push(read.into_future());
                    return Ok(());
                }
                read::ReadFailure::Terminal(err) => {
                    // Fail-fast: the whole session tears down. The teardown is
                    // the signal, so this is deliberately not counted.
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
        if let Some(backfill) = self.backfills.get_mut(&read_key) {
            backfill.physical_bytes += n;
        }

        let ready_read = read::parse_lines_batch(
            &mut self.parser,
            &mut self.validators[self.reads[read_id].binding_index as usize],
            binding,
            &self.reads[read_id].journal,
            read,
            lines_batch,
            "transcoding backfill documents",
        )?;
        // Stash the parsed batch as this backfill's cursor; `try_drain_backfills`
        // appends its target-producer documents on a later loop iteration. The
        // inner read is re-polled only once the cursor is fully drained.
        self.backfills
            .get_mut(&read_key)
            .expect("backfill has a parked main read")
            .cursor = Some(Box::new(ready_read));

        Ok(())
    }
}

/// Advance a backfill's cursor to its next buffered document, or (when the batch
/// is exhausted) return the inner historical read to `pending_backfills` and
/// clear the cursor. Mirrors the main-read drain's tail advance, but for the
/// cursor rather than the heap. The just-consumed head is dropped.
fn advance_backfill_cursor(
    backfill: &mut Backfill,
    pending_backfills: &mut futures::stream::FuturesUnordered<
        futures::stream::StreamFuture<super::ReadLines>,
    >,
) {
    let ReadyRead {
        inner: read,
        doc: _consumed_doc,
        meta: _consumed_meta,
        mut doc_tail,
        mut meta_tail,
    } = *backfill.cursor.take().expect("cursor present");

    match (doc_tail.next(), meta_tail.next()) {
        (Some((doc, _)), Some(meta)) => {
            backfill.cursor = Some(Box::new(ReadyRead {
                doc,
                meta,
                doc_tail,
                meta_tail,
                inner: read,
            }));
        }
        // The batch is fully drained: re-poll the historical stream only now, so
        // no cursor can be pending when it reaches its end.
        (None, None) => pending_backfills.push(read.into_future()),
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
