use proto_gazette::uuid::{Clock, Producer};

/// Per-producer sequencing state.
///
/// It's scoped to a single (binding, journal) tuple because an ACK_TXN in
/// journal J commits only that producer's preceding CONTINUE_TXN documents in J.
/// It does NOT commit the same producer's documents in other journals, which
/// will have their own ACKs. Cross-journal commit visibility is coordinated at
/// the Session level via causal hints extracted from ACK documents
/// (see [`extract_causal_hints`]).
///
/// It's additionally binding-scoped because we create an independent ReadState
/// for each (binding, journal) tuple, and separately track producer states
/// for each one.
///
/// `offset` encodes journal position using the same sign convention as the
/// wire format (`ProducerFrontier.offset`):
///   - Non-negative: Begin offset of first pending CONTINUE_TXN
///   - Negative: Negation of end offset of last committing ACK_TXN / OUTSIDE_TXN
/// Internal default state uses zero before any document has been observed.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProducerState {
    /// Clock of the last committing ACK_TXN or OUTSIDE_TXN.
    pub last_commit: Clock,
    /// Maximum Clock of an uncommitted CONTINUE_TXN, or zero if no pending span.
    ///
    /// Doubles as the in-memory *gapped* sentinel: a producer is gapped iff
    /// `max_continue == last_commit + 1` (see [`ProducerState::is_gapped`]). The
    /// sentinel can never arise organically. The clock-adjacency protocol axiom
    /// guarantees a real CONTINUE_TXN following its producer's ACK at `last_commit`
    /// carries a clock strictly greater than `last_commit + 1`, and every
    /// committing or rolling-back outcome in `uuid::sequence` zeroes `max_continue`
    /// — so no real pending span ever lands on `last_commit + 1`. Encoding gapped
    /// this way makes `uuid::sequence` the single, normative classifier for a
    /// gapped producer's documents: a newer CONTINUE/ACK extends or commits the
    /// span (the backfill trigger), an ACK at or below `last_commit` is a
    /// clean/deep rollback, and a duplicate passes through with `max_continue`
    /// unchanged (the gap survives). The one non-`uuid::sequence` row is a gapped
    /// OUTSIDE, which cannot sequence against the merely-presumed span:
    /// `sequence_producer` classifies it directly — a newer OUTSIDE is a backfill
    /// trigger, a duplicate drops with the gap intact.
    ///
    /// In-memory only: `ProducerFrontier` has no `max_continue` field, so the
    /// sentinel cannot leak into a durable checkpoint, and recovery re-derives it
    /// in `resolve_checkpoint`. While gapped, `offset` is the pinned gap begin `F`,
    /// and three resolutions clear it — a backfill trigger (the first newer
    /// CONTINUE, ACK, or OUTSIDE), a clean rollback, or a deep rollback (see
    /// `sequence_producer` and `plans/shuffle-gapped-restart.md` §Gapped state).
    ///
    /// The sentinel makes the state fully self-describing: a reconstructed-empty
    /// span `{L, 0, F}` is distinct from a gapped `{L, L+1, F}`, which is what
    /// prevents an infinite re-trigger loop after an empty backfill. Operator note:
    /// a sequencing-failure error context prints this synthetic `max_continue` one
    /// tick above `last_commit`.
    pub max_continue: Clock,
    /// Journal byte offset, sign-encoded (see struct docs).
    pub offset: i64,
}

impl ProducerState {
    /// Whether this producer is *gapped* (see [`ProducerState::max_continue`]): its
    /// uncommitted span begins at `offset` (`F`) below the restart position `M`, so
    /// the main read skipped `[F, M)` and the entry is frozen until it resolves.
    /// Encoded as the `max_continue == last_commit + 1` sentinel, which the
    /// clock-adjacency axiom guarantees a real pending span can never produce.
    pub fn is_gapped(&self) -> bool {
        // `wrapping_add` only to be total; a real clock is nowhere near u64::MAX,
        // so the wrap can never occur.
        self.max_continue.as_u64() == self.last_commit.as_u64().wrapping_add(1)
    }

    /// Mark this producer gapped by installing the `last_commit + 1` sentinel into
    /// `max_continue`. Called during checkpoint recovery for an uncommitted span
    /// whose begin `F` precedes `M`.
    pub fn mark_gapped(&mut self) {
        self.max_continue = Clock::from_u64(self.last_commit.as_u64() + 1);
    }
}
const _: () = assert!(std::mem::size_of::<ProducerState>() == 24);

/// Build a [`crate::Frontier`] by reducing read-derived producer state with
/// causal hints.
///
/// `reads` provides the journal name, binding index, and pending producers
/// for each active read. `hints` yields owned `((journal, binding),
/// Vec<(producer, hinted_clock)>)` entries, typically from a HashMap drain.
///
/// Both inputs may arrive in arbitrary order; outputs are sorted.
pub fn build_flush_frontier(
    reads: &mut [super::read::ReadState],
    hints: impl Iterator<Item = ((Box<str>, u16), Vec<(Producer, Clock)>)>,
    shard_count: usize,
) -> crate::Frontier {
    // Walk all journal reads to build their JournalFrontier.
    let mut journals: Vec<crate::JournalFrontier> = Vec::new();

    for read_state in reads.iter_mut() {
        if read_state.pending.is_empty() {
            // No reportable progress for this journal since the last flush.
            // We intentionally defer offset-based reporting as well:
            // the next reported deltas are computed from prev_read_offset
            // and prev_write_head, so reported values are eventually correct
            // even if offsets advanced meanwhile.
            continue;
        }
        let mut producers: Vec<_> = read_state
            .pending
            .iter()
            .map(|(producer, ps)| crate::ProducerFrontier {
                producer: *producer,
                // A real pending span begun at journal offset zero persists the
                // SPAN_AT_HEAD_MARKER (raw `last_commit = 1`) in place of
                // `Clock::zero()`, so recovery can distinguish it from a
                // hint-only placeholder `{0, 0}` and gap it. `last_commit` at
                // `offset == 0` is a load-bearing encoding — see the constant's
                // docs. This covers both a live span begun at offset zero
                // (ContinueBeginSpan) and a frozen gapped entry at `F = 0`
                // re-inserted by a duplicate-drop, whose recovered (normalized)
                // `last_commit` is likewise zero. The hints loop below never
                // emits the marker: hint-only entries keep `last_commit: 0`.
                last_commit: if ps.offset == 0 && ps.last_commit == Clock::zero() {
                    Clock::from_u64(crate::frontier::SPAN_AT_HEAD_MARKER)
                } else {
                    ps.last_commit
                },
                hinted_commit: Clock::zero(),
                offset: ps.offset,
            })
            .collect();
        producers.sort_by(|a, b| a.producer.cmp(&b.producer));

        let bytes_read_delta = read_state.read_offset - read_state.prev_read_offset;
        let bytes_behind_delta = (read_state.write_head - read_state.read_offset)
            - (read_state.prev_write_head - read_state.prev_read_offset);

        journals.push(crate::JournalFrontier {
            binding: read_state.binding_index,
            journal: read_state.journal.clone().into(),
            producers,
            bytes_read_delta,
            bytes_behind_delta,
        });

        // Update the baselines for the next delta computation.
        read_state.prev_read_offset = read_state.read_offset;
        read_state.prev_write_head = read_state.write_head;
        read_state.settled.extend(read_state.pending.drain());
    }

    journals.sort_by(|a, b| a.journal.cmp(&b.journal).then(a.binding.cmp(&b.binding)));

    let reads_frontier = crate::Frontier {
        unresolved_hints: 0, // By construction: only `last_commit` set.
        journals,
        flushed_lsn: vec![crate::log::Lsn::ZERO; shard_count],
    };

    // Build a Frontier from causal hints via single-pass iteration.
    let mut hint_journals: Vec<crate::JournalFrontier> = hints
        .map(|((journal, binding), producers)| {
            let mut producers: Vec<_> = producers
                .into_iter()
                .map(|(producer, hinted_clock)| crate::ProducerFrontier {
                    producer,
                    last_commit: Clock::zero(),
                    hinted_commit: hinted_clock,
                    offset: 0,
                })
                .collect();

            producers.sort_by(|a, b| a.producer.cmp(&b.producer));
            producers.dedup_by(|b, a| {
                a.producer == b.producer && {
                    a.hinted_commit = a.hinted_commit.max(b.hinted_commit);
                    true
                }
            });

            crate::JournalFrontier {
                binding,
                journal,
                producers,
                bytes_read_delta: 0,
                bytes_behind_delta: 0,
            }
        })
        .collect();

    // Sort to restore the sorted Frontier invariant
    // (entries must be unique since they come from HashMap keys).
    hint_journals.sort_by(|a, b| a.journal.cmp(&b.journal).then(a.binding.cmp(&b.binding)));

    // By construction every producer has `last_commit: zero` and a non-zero `hinted_commit`.
    let unresolved_hints = hint_journals.iter().map(|jf| jf.producers.len()).sum();
    reads_frontier.reduce(crate::Frontier {
        unresolved_hints,
        journals: hint_journals,
        flushed_lsn: vec![],
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::ProducerMap;

    fn producer(id: u8) -> Producer {
        Producer::from_bytes([id | 0x01, 0, 0, 0, 0, 0])
    }

    fn read_state(
        journal: &str,
        binding: u16,
        pending: &[(u8, u64, i64)],
    ) -> super::super::read::ReadState {
        read_state_with_bytes(journal, binding, pending, 0, 0, 0, 0)
    }

    fn read_state_with_bytes(
        journal: &str,
        binding: u16,
        pending: &[(u8, u64, i64)],
        prev_read_offset: i64,
        write_head: i64,
        read_offset: i64,
        prev_write_head: i64,
    ) -> super::super::read::ReadState {
        let mut map = ProducerMap::default();
        for &(id, last_commit, offset) in pending {
            map.insert(
                producer(id),
                ProducerState {
                    last_commit: Clock::from_u64(last_commit),
                    max_continue: Clock::zero(),
                    offset,
                },
            );
        }
        super::super::read::ReadState {
            binding_index: binding,
            journal: journal.into(),
            settled: ProducerMap::default(),
            pending: map,
            read_offset,
            prev_read_offset,
            write_head,
            prev_write_head,
        }
    }

    fn hint(
        journal: &str,
        binding: u16,
        producers: &[(u8, u64)],
    ) -> ((Box<str>, u16), Vec<(Producer, Clock)>) {
        (
            (journal.into(), binding),
            producers
                .iter()
                .map(|&(id, clock)| (producer(id), Clock::from_u64(clock)))
                .collect(),
        )
    }

    #[test]
    fn test_build_flush_frontier() {
        // (case_name, reads, hints)
        let cases: Vec<(
            &str,
            Vec<super::super::read::ReadState>,
            Vec<((Box<str>, u16), Vec<(Producer, Clock)>)>,
        )> = vec![
            // Both empty.
            ("empty", vec![], vec![]),
            // Reads only, reverse input order verifies sorting.
            // Non-zero byte tracking: journal/B is 5000 bytes behind
            // (write_head=50000, read_offset=45000, prev_write_head=43800, prev_read_offset=43800
            //  → behind_delta = 5000 - 0 = 5000),
            // with offset advancement of 1200 (45000-43800). journal/A is catching up (delta=-300).
            (
                "reads_only",
                vec![
                    read_state_with_bytes(
                        "journal/B",
                        0,
                        &[(0x03, 200, -1000)],
                        43800,
                        50000,
                        45000,
                        43800,
                    ),
                    read_state_with_bytes(
                        "journal/A",
                        0,
                        &[(0x01, 100, -500)],
                        8700,
                        10000,
                        9500,
                        9500,
                    ),
                    // No pending producers: not part of frontier, not modified.
                    read_state_with_bytes("journal/C", 0, &[], 0, 25000, 123, 456),
                ],
                vec![],
            ),
            // Hints only, reverse input order verifies sorting.
            (
                "hints_only",
                vec![],
                vec![
                    hint("journal/C", 1, &[(0x03, 300)]),
                    hint("journal/A", 0, &[(0x01, 150)]),
                ],
            ),
            // Empty-pending reads are skipped.
            (
                "empty_pending_skipped",
                vec![
                    read_state("journal/A", 0, &[(0x01, 100, -500)]),
                    read_state("journal/B", 0, &[]),
                ],
                vec![],
            ),
            // Reads and hints reduce: journal/A reads-only, journal/B merged
            // (producer 0x03 gets hint), journal/C hints-only.
            // Offset advancement: journal/A has 500 bytes_read_delta (19000-18500), journal/B has 2000 (75000-73000).
            // Hint-only journal/C gets 0 for both byte fields.
            (
                "reads_and_hints",
                vec![
                    read_state_with_bytes(
                        "journal/A",
                        0,
                        &[(0x01, 100, -500)],
                        18500,
                        20000,
                        19000,
                        19000,
                    ),
                    read_state_with_bytes(
                        "journal/B",
                        0,
                        &[(0x03, 200, -1000), (0x05, 50, -200)],
                        73000,
                        80000,
                        75000,
                        76000,
                    ),
                ],
                vec![
                    hint("journal/B", 0, &[(0x03, 300)]),
                    hint("journal/C", 1, &[(0x03, 300)]),
                ],
            ),
            // Same journal, different bindings: sorted by (journal, binding),
            // each binding's producers independent.
            (
                "same_journal_diff_bindings",
                vec![
                    read_state("journal/X", 2, &[(0x01, 100, -400)]),
                    read_state("journal/X", 0, &[(0x03, 50, -200)]),
                ],
                vec![hint("journal/X", 1, &[(0x05, 250)])],
            ),
            // Duplicate hint producers: same producer hinted twice with
            // different clocks (from two ACK documents). Should be deduped
            // to a single entry with the max clock.
            (
                "duplicate_hint_producers",
                vec![],
                vec![hint("journal/A", 0, &[(0x01, 100), (0x01, 200)])],
            ),
            // Duplicate hint producers merged with reads: the deduped hint
            // should merge cleanly with the read-derived entry.
            (
                "duplicate_hints_merged_with_reads",
                vec![read_state("journal/A", 0, &[(0x01, 50, -300)])],
                vec![hint("journal/A", 0, &[(0x01, 100), (0x01, 200)])],
            ),
            // Span-at-head marker: a pending span at journal offset zero with no
            // prior commit persists `last_commit = SPAN_AT_HEAD_MARKER` (raw 1),
            // so recovery can distinguish it from a hint-only `{0, 0}` entry.
            // Only the exact `{last_commit: 0, offset: 0}` shape is marked: a
            // span at `F > 0` and an offset-zero entry with a real `last_commit`
            // pass through, and hint-loop entries keep `last_commit: 0`.
            (
                "span_at_head_marker",
                vec![read_state(
                    "journal/A",
                    0,
                    &[(0x01, 0, 0), (0x03, 0, 700), (0x05, 90, 0)],
                )],
                vec![hint("journal/B", 0, &[(0x07, 150)])],
            ),
        ];

        let snap = cases
            .into_iter()
            .map(|(name, mut reads, hints)| {
                let f = build_flush_frontier(&mut reads, hints.into_iter(), 3);
                (name, f, reads)
            })
            .collect::<Vec<_>>();

        insta::assert_debug_snapshot!(snap);
    }
}
