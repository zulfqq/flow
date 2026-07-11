//! Per-binding backfill-truncation state: each binding tracks only its latest
//! truncation boundary clock, which serves as the combiner epoch tag. Documents
//! at or above it are the active epoch (all that drain emits); below are stale
//! (epoch 0). See [`doc::combine::Meta::epoch`].

use proto_gazette::uuid;

#[derive(Debug)]
pub(super) struct Epochs(Vec<Option<uuid::Clock>>);

impl Epochs {
    pub(super) fn new(n_bindings: usize) -> Self {
        Self(vec![None; n_bindings])
    }

    /// Observe `binding`'s backfill boundary; at or below the latest is a no-op
    /// (begins are re-delivered on every Load and only advance).
    pub(super) fn observe_begin(&mut self, binding: usize, begin: uuid::Clock) {
        let latest = &mut self.0[binding];
        if latest.is_some_and(|l| begin <= l) {
            return;
        }
        *latest = Some(begin);
    }

    /// `clock`'s epoch for `binding`: the boundary clock when `clock` is at or
    /// above it, else 0 (stale, or no boundary observed).
    pub(super) fn epoch_for_clock(&self, binding: usize, clock: uuid::Clock) -> u64 {
        match self.0[binding] {
            Some(boundary) if clock >= boundary => boundary.as_u64(),
            _ => 0,
        }
    }

    /// Whether `binding` has observed a truncation boundary. Without one every
    /// document is the active epoch regardless of clock, so a Loaded row's UUID
    /// clock is only needed once a binding is truncating.
    pub(super) fn has_boundary(&self, binding: usize) -> bool {
        self.0[binding].is_some()
    }

    /// Snapshot each binding's active epoch (boundary clock, or 0) for a drain;
    /// `None` when no binding has truncated.
    pub(super) fn active_epochs(&self) -> Option<Box<[u64]>> {
        if self.0.iter().all(Option::is_none) {
            return None;
        }
        Some(self.0.iter().map(|c| c.map_or(0, |c| c.as_u64())).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock(v: u64) -> uuid::Clock {
        uuid::Clock::from_u64(v)
    }

    #[test]
    fn no_boundary_is_epoch_0() {
        let e = Epochs::new(1);
        assert!(!e.has_boundary(0));
        for c in [1, 100, u64::MAX] {
            assert_eq!(e.epoch_for_clock(0, clock(c)), 0);
        }
        assert_eq!(e.active_epochs(), None); // no boundary → nothing to partition
    }

    #[test]
    fn boundary_tags_active_with_its_clock() {
        let mut e = Epochs::new(1);
        e.observe_begin(0, clock(500));
        assert!(e.has_boundary(0));
        assert_eq!(e.epoch_for_clock(0, clock(499)), 0); // stale
        assert_eq!(e.epoch_for_clock(0, clock(500)), 500); // active == boundary
        assert_eq!(e.epoch_for_clock(0, clock(999)), 500);
        assert_eq!(&*e.active_epochs().unwrap(), &[500]);

        // Advancing the boundary retires the prior active epoch.
        e.observe_begin(0, clock(800));
        assert_eq!(e.epoch_for_clock(0, clock(500)), 0);
        assert_eq!(e.epoch_for_clock(0, clock(800)), 800);
        assert_eq!(&*e.active_epochs().unwrap(), &[800]);
    }

    #[test]
    fn repeated_or_stale_begin_is_noop() {
        let mut e = Epochs::new(1);
        e.observe_begin(0, clock(500));
        e.observe_begin(0, clock(500)); // duplicate
        e.observe_begin(0, clock(300)); // below the latest
        assert_eq!(&*e.active_epochs().unwrap(), &[500]);
    }

    #[test]
    fn bindings_advance_independently() {
        let mut e = Epochs::new(2);
        e.observe_begin(0, clock(500));
        assert_eq!(&*e.active_epochs().unwrap(), &[500, 0]);
        assert_eq!(e.epoch_for_clock(1, clock(999)), 0);
    }
}
