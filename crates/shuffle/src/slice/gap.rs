//! Gapped-producer recovery state.
//!
//! On restart, a `(binding, journal)` read starts near the furthest journal
//! position justified by its checkpoint (`R`), allowing a bounded conservative
//! re-read window `B` behind the maximum magnitude `M`. An uncommitted producer
//! span whose begin offset `F` falls before `R` is *gapped*: the main read skips
//! `[F, R)` and the producer is frozen until it resolves. See
//! `plans/shuffle-gapped-restart.md` §Producer state machine.
//!
//! POD state only — the actor owns the IO objects (parked main reads, in-flight
//! historical reads). This keeps `ReadState` snapshot-testable and follows the
//! crate's state-machine decomposition convention.

use super::producer::ProducerState;
use super::read::Meta;

/// Gap state for one producer of a `(binding, journal)` read.
///
/// `normal → gapped` happens only at checkpoint recovery (in
/// `resolve_checkpoint`); the in-session transitions are `gapped → backfilling`
/// (on a committing ACK) and `backfilling → normal` (after the shelved ACK
/// flushes). `gapped → normal` also occurs on a clean/deep rollback or a newer
/// OUTSIDE commit, which remove the gap entirely.
#[derive(Debug)]
pub enum GapState {
    /// The skipped span begins at pinned offset `F` (`gap_begin`). Frozen:
    /// main-read documents of this producer must not mutate its producer state,
    /// advance the checkpoint, or become visible. In particular a suppressed
    /// `ContinueBeginSpan` must not overwrite `F` with a post-`R` offset.
    Gapped { gap_begin: i64 },
    /// Parked at a committing ACK; a historical read of `[gap_begin, ack.begin)`
    /// is in flight. `live` sequences historical target-producer documents,
    /// initialized to the recovered `{last_commit, max_continue: 0, offset:
    /// gap_begin}`. `ack` is the shelved trigger ACK's metadata; the ACK
    /// document itself is held in the actor's parked main read.
    ///
    /// No checkpoint advancement or transaction visibility is emitted between
    /// the trigger and the final ACK flush — `live` is never mirrored into
    /// `pending`/`settled`, so a partially-advanced positive offset can't leak
    /// into a durable base via max-abs Frontier reduction and break recovery
    /// idempotence.
    Backfilling {
        gap_begin: i64,
        live: ProducerState,
        ack: Meta,
    },
}

impl GapState {
    /// The pinned begin offset `F` of the skipped span, in either state.
    pub fn gap_begin(&self) -> i64 {
        match self {
            GapState::Gapped { gap_begin } | GapState::Backfilling { gap_begin, .. } => *gap_begin,
        }
    }
}
