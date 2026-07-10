# Gapped Shuffle Recovery

Behavioral requirements for replacing conservative shuffle restart reads with
checkpoint skip-ahead and targeted transaction backfill.

This document specifies externally observable behavior, state-machine
transitions, and correctness constraints. It does not prescribe an
implementation.

## Problem

A shuffle checkpoint tracks producer state independently for each
`(binding, journal)` read. A producer entry records:

- `last_commit`, the clock of its last committed transaction; and
- `offset`, using a signed encoding:
  - a non-negative value is the begin offset of an uncommitted span;
  - a negative value is the negated end offset of the last commit.

Today, restart chooses the minimum uncommitted begin offset of every tracked
producer. This is conservative: all documents of every pending span are read
again before a later ACK can be encountered. It is also expensive when a
producer died with an open span. A replacement producer can move the journal
far ahead while the dead producer's begin offset remains in the checkpoint,
causing every consumer restart to repeat a large read that will almost never
be committed.

Producer pruning limits checkpoint size, but it is deliberately conservative
and happens only during recovery. It should not be the mechanism that bounds
nominal restart I/O.

## Intended outcome

On restart, each journal begins near the furthest position justified by its
checkpoint, while allowing a configured, bounded amount of conservative
re-read. Producers whose pending spans begin before that bounded window are
remembered as *gapped* and suppressed. Old dead producers then cost no further
I/O. If one does later commit, shuffle parks at its ACK, re-reads only that
producer's pending transaction, and publishes the transaction atomically
through the normal log and Frontier machinery. Live producers clustered near
the frontier are recovered together by the main read.

The change has three primary goals:

1. A dead producer's open span does not force repeated whole-journal catch-up.
2. A tracked producer that later commits remains recoverable and is delivered
   exactly once, in producer-clock order, as one atomic source transaction.
3. No protocol, wire-format, or persistence-format change is required.

The change is scoped to the shuffle Slice and its observability surfaces.

## Terms and invariants

### Producer scope

Sequencing state is scoped to `(binding, journal, producer)`. An ACK in one
journal commits only the producer's preceding CONTINUE documents in that same
journal and binding. Causal hints coordinate visibility with related ACKs in
other journals.

### Checkpoint closure

A flushed checkpoint is closed over every journal position represented by one
of its producer entries. If an entry records offset magnitude `O`, the flush
which introduced that entry also captured the current state of every producer
document sequenced before `O`; cumulative base merging preserves those states.
Each such document is therefore reflected in its producer's checkpoint entry
or was classified as a duplicate. This is the invariant that makes
checkpoint-derived skip-ahead safe.

### Producer order

Within a shard log, clocks for one `(binding, journal, producer)` tuple are
strictly ascending. This invariant is required by log scanning and downstream
transaction reduction and MUST continue to hold.

### Source-transaction atomicity

Documents of one committed source transaction become visible together in a
Frontier that includes the committing ACK's producer progress and any causal
hints. No prefix of a backfilled transaction may become visible independently.

### Journal lifetime

A journal which disappears from a listing has been deleted or fully suspended.
As a platform invariant, FULL suspension occurs only after all journal
fragments have been removed. The journal therefore has no retained content,
and its active read state, including gaps, may be discarded. A later listing
appearance starts a new read with no obligation to recover historical producer
state from the removed read.

## Restart resolution

For each `(binding, journal)` checkpoint, define:

```text
M = max(magnitude(offset)) across all producer entries
B = configured maximum near-frontier re-read bytes
R = min({M} union {F | F is uncommitted and M - F <= B})
```

An empty checkpoint resolves to `M = R = 0`. `B` MUST be non-negative.

The main journal read starts at `R`, subject to the same broker
`begin_mod_time` and fragment-hole behavior as existing reads.

`B` controls the trade-off between one-pass conservative recovery and targeted
backfill:

- `B = 0` always chooses the maximum checkpoint position. Tests SHOULD use
  this setting to exercise the backfill path aggressively.
- A non-zero production value re-reads at most `B` bytes behind `M` and recovers
  all uncommitted producers in that window through the main read.
- An unbounded `B` degenerates to today's minimum-uncommitted-begin strategy.

The behavioral parameter is required, but its configuration surface is not
specified here. It need not be persisted in a checkpoint and MUST be applied
consistently within a Slice session.

Each recovered producer is classified as follows:

| Checkpoint entry | Classification |
|---|---|
| Committed offset `-O` | Normal, with `last_commit` and committed end `O` recovered as today. |
| Uncommitted begin `F`, where `F >= R` | Normal. The main read encounters the span from its beginning. |
| Uncommitted begin `F`, where `F < R` | Gapped. The main read skips `[F, R)`. |

Because `M` is the maximum magnitude, an uncommitted `F > M` is impossible.

An entry with `offset == 0` may be a hint-only producer or a real span that
began at journal offset zero. When `R > 0`, it is gapped with `F = 0`. A later
commit may therefore require a targeted read from the beginning of the
journal. This is no worse than today's conservative restart from zero.

Hint projection does not make this case distinguishable. A real pending span
at offset zero can also carry a non-zero `hinted_commit`, and Frontier reduction
erases whether an offset-zero entry came only from a hint or also from committed
state. Classifying apparent hint-only entries as normal would therefore risk
skipping a real span. The conservative `F = 0` classification is deliberate.
Although `[0, R)` is often dead weight for a true hint-only producer, the full
range continues through `ack_begin` and recovers any CONTINUE documents that
the main read suppressed in `[R, ack_begin)`.

The journal write head MUST NOT be used in place of `M`. Bytes between a
checkpoint-derived position and the write head may contain transactions that
no prior session sequenced.

### Why skip-ahead is safe

Consider an entry relative to `M` and `R`:

- A committed end `O <= M` needs no replay. If this producer had an unseen
  document in `(O, M)`, checkpoint closure would require its entry to reflect
  that document as either a later commit or an uncommitted begin.
- An uncommitted begin `F >= R` is encountered from its first document by the
  main read.
- An uncommitted begin `F < R` is the only case where documents are skipped.
  The exact missing lower bound is retained as gap start `F`.

The argument remains inductive across later sessions because a gapped
producer is frozen and omitted from progress until it resolves. Other
producers may advance the checkpoint and increase a future `M` and `R`, but the
gapped producer continues to pin its original `F`.

## Producer state machine

A producer is in one of three states:

```text
normal  -> gapped       only during checkpoint recovery
gapped -> normal        on clean/deep rollback or a newer OUTSIDE commit
gapped -> backfilling   on a newer committing ACK
backfilling -> normal   after the shelved ACK commits
```

Session failure discards in-memory transitions. Recovery reconstructs the
appropriate state from the last durable checkpoint.

### Gapped state

A gapped producer retains:

- its recovered `last_commit`;
- `max_continue = 0`; and
- its pinned uncommitted begin offset `F`.

While gapped:

- normal journal documents MUST NOT change its producer state;
- its documents MUST NOT be appended to any shard log;
- it MUST NOT advance checkpoint state or transaction visibility in a flush
  Frontier; and
- its recovered checkpoint entry MUST survive base-checkpoint merging
  unchanged.

An unrelated Slice-wide flush MAY include an unchanged producer entry carrying
the same `last_commit` and `F`. Requiring complete absence would unnecessarily
constrain how live backfill sequencing state is represented. Only advancement
or visibility is forbidden before resolution.

In particular, a suppressed `ContinueBeginSpan` MUST NOT overwrite `F` with a
post-`R` document offset.

### Main-read outcomes while gapped

The following table is normative. “Commit progress” means setting a committed
negative offset, extracting causal hints when the document is an ACK, and
causing a flush through the normal commit path.

| Document | Sequencing classification with frozen state | Required behavior |
|---|---|---|
| CONTINUE, clock `> last_commit` | `ContinueBeginSpan` | Suppress without state mutation. |
| CONTINUE, clock `<= last_commit` | `ContinueDuplicate` | Drop. |
| ACK, clock `> last_commit` | `AckEmpty` | Park at the ACK and begin backfill. Do not apply the speculative `AckEmpty` state. |
| ACK, clock `== last_commit` | `AckDuplicate` | Resolve as a clean rollback: discard the gap and emit commit progress with `offset = -ack_end`. Extract causal hints and flush. |
| ACK, clock `< last_commit` | `AckDuplicate` | Resolve directly as a deep rollback, without backfill: discard the gap, warn, set live `last_commit` to the ACK clock, set `offset = -ack_end`, extract causal hints, and flush. |
| OUTSIDE, clock `> last_commit` | `OutsideCommit` | Discard the gap, then process the OUTSIDE commit normally. |
| OUTSIDE, clock `<= last_commit` | `OutsideDuplicate` | Drop and retain the gap. |

Treating `ACK == last_commit` as ordinary `AckDuplicate` is insufficient: it
would clear only in-memory state while leaving positive offset `F` in the
durable checkpoint. Rollback resolution MUST be reportable and durable even
when no later producer document arrives.

Frozen `max_continue = 0` would ordinarily hide a deep rollback behind
`AckDuplicate`. The gap itself proves that a pending span exists, so an ACK
below `last_commit` has the same semantics that conservative re-reading would
derive after reconstructing `max_continue > 0`. No historical I/O is needed:
the span is rolled back and none of its documents can become visible.

Frontier reduction keeps `last_commit` monotonic, so a durable base may retain
the older, higher `last_commit` even though the live sequencing state regresses.
The newer negative `offset` still wins by magnitude and durably clears the gap.
This matches existing deep-rollback behavior and its producer-retirement
assumption; a producer MUST NOT publish new clocks after any rollback.

The OUTSIDE transition relies on the producer protocol: a compliant producer
does not issue OUTSIDE while it has a pending CONTINUE span. Once a newer
OUTSIDE is observed, the old span is not recoverable under protocol semantics.
Processing the OUTSIDE normally avoids the permanent
`OutsideWithPrecedingContinue` failure that a conservative re-read would
otherwise produce.

## Backfill transaction

### Trigger and parking

The first ACK with `clock > last_commit` encountered for a gapped producer is
the trigger.

The main read MUST park at the ACK before sequencing it:

- the ACK document and metadata are retained;
- the logical main-read position does not advance past the ACK;
- no later document of that journal is processed; and
- at most one backfill is active for a `(binding, journal)` read.

The parked main read MUST be held outside the Slice's pending-read tailing gate
and MUST NOT count as an ordinary stalled read. The historical read is likewise
exempt from the tailing gate and ordinary stall accounting. Intentional parking
is reported only through backfill-specific events and metrics.

Other journals may continue to make progress, subject to ordinary shared Log
back-pressure and flush coordination.

### Historical range

Backfill reads the half-open range `[F, ack_begin)` from the same journal and
binding as the main read. It uses the binding's normal authorization,
`begin_mod_time`, schema validation, partition filtering, and journal routing.
Fragments may be fetched from cold storage.

Every decoded document is inspected for its producer identity. Documents of
other producers are skipped without sequencing, state mutation, key
extraction, or log append. They are already represented by checkpoint state or
belong to independent gaps.

Documents of the target producer are sequenced and appended through the
normal path against live producer state initialized from the recovered
`last_commit`, `max_continue = 0`, and offset `F`.

### One-transaction invariant

The historical range reconstructs one pending source transaction:

- any state-changing ACK below `R` would already be reflected by checkpoint
  closure;
- clean and deep rollback ACKs encountered at or above `R` resolve directly;
- the first committing ACK above `last_commit` at or above `R` is the parked
  trigger; and
- therefore no distinct transaction boundary can occur inside
  `[F, ack_begin)`.

The range may contain at-least-once duplicates, which normal sequencing drops.
A distinct target-producer transaction boundary inside the range contradicts
the recovered checkpoint and is a terminal consistency error rather than an
intermediate transaction to expose.

### Completion

After the historical range reaches `ack_begin`:

1. The shelved ACK is sequenced against the reconstructed producer state.
2. Its causal hints are extracted normally.
3. The producer offset becomes `-ack_end`.
4. A flush makes all backfilled appends, producer progress, and causal hints
   durable together.
5. The gap is removed and the producer becomes normal.
6. The main read resumes strictly after the ACK.

The expected ACK outcome is `AckCommit`. `AckEmpty` is possible only when
historical documents were unavailable or filtered by existing journal-read
semantics; that exposure is addressed under accepted risks.

No target-producer checkpoint advancement or transaction visibility is emitted
between the trigger and the final ACK flush. An unchanged `(last_commit, F)`
entry may be included by an unrelated flush. The final ACK flush remains the
atomic visibility boundary of the backfill.

## Ordering and scheduling

Backfill is an explicit exception to shuffle's global
`(priority DESC, adjusted_clock ASC)` processing order.

A pending cold read has no document in the ready heap. Normal documents may be
emitted before an older or higher-priority backfill document arrives, and logs
may already contain later documents by the time backfill begins. Shuffle does
not attempt to restore global ordering across that boundary.

This relaxation is safe because:

- downstream processing does not require global producer-clock order;
- the complete backfilled source transaction becomes visible atomically in
  one Frontier;
- causal hints continue to gate cross-journal transaction visibility; and
- strict order for the target `(binding, journal, producer)` is preserved by
  suppressing its main-read documents and parking at its ACK.

The relaxation is nevertheless user-visible. An old backfilled document may be
processed after a newer same-key document written by another producer. A
last-write-wins reduction can therefore select the historical value until a
later update supersedes it. Downstream consumers are required to tolerate this
cross-producer inversion; source-transaction atomicity, not global value order,
is the guarantee retained by this design.

When a backfill document is ready, it retains the binding's normal priority
and read-delay metadata and follows the normal append path. Normal clock-delay
gating still applies to its adjusted clock; implementations MUST NOT assume
that every historical clock plus `read_delay` is already in the past.

The parked main read and pending historical read MUST both be exempt from the
Slice condition that every pending read be tailing before the ready heap can
drain. This prevents intentional parking and historical I/O from deliberately
blocking unrelated journals. It does not promise resource isolation: backfill
appends, Log channel capacity, flushes, and disk back-pressure remain shared
and may indirectly stall other work.

## Read positions and byte accounting

Logical main-read progress and physical backfill I/O are distinct.

During backfill:

- the main read's `read_offset`, previous offset baseline, write head, and
  bytes-behind baseline MUST NOT regress or be replaced by historical offsets;
- backfill MUST NOT create a second reportable `(binding, journal)` read
  Frontier;
- `bytes_read_delta` and `bytes_behind_delta` continue to describe only the
  forward main read; and
- all physical bytes fetched by the historical read, including bytes skipped
  for other producers, are counted by dedicated backfill metrics.

After the shelved ACK is consumed, the main read advances normally to
`ack_end`.

## Causal hints and recovery checkpoints

An unresolved hint (`hinted_commit > last_commit`) needs no special replay
path. If the hinted producer is gapped, the target journal reaches its
committing ACK, triggers backfill, and resolves the hint through the final
atomic Frontier.

The parked ACK cannot have been fully sequenced by a prior checkpoint;
otherwise its `last_commit` would already reflect it. The existing checkpoint
peek mechanism remains unchanged.

If a session ends before the final ACK flush, no progress for the backfilling
producer has been exposed. The next session recovers the unchanged positive
offset `F`, recreates the gap, and encounters the ACK again. Appends belonging
only to the failed session are discarded under the existing fail-fast log
model.

Unrelated producer progress may, independently, have advanced the durable
checkpoint while the backfill was running. That does not alter the gapped
producer's pinned `F`.

## Read and session lifecycle

### Session teardown

Teardown while a journal is parked or its backfill is in flight MUST cancel
the historical read and release the shelved main read without deadlock. No gap
state is persisted separately. Recovery is entirely derived from the producer
checkpoint entry.

### Journal removal

When a read stops because its journal was deleted or fully suspended, its
gapped and backfilling state is discarded with the rest of that read. A later
listing appearance is a new read and may start without the removed read's
producer checkpoint. Because deletion and FULL suspension imply that no
fragments remain, the new read has no historical span to recover.

Observability gauges MUST be decremented when either a session or an
individual read releases its gaps.

### Failure handling

Terminal historical-read, decoding, validation, sequencing, append, or flush
errors follow the existing fail-fast topology teardown. Transient journal I/O
uses the existing retry model. The main ACK remains unsequenced until the
backfill completes successfully.

## Pruning

Producer pruning remains a recovery-time operation performed before a Slice
receives its checkpoint. Consequently:

- an entry removed by pruning never becomes a live gap;
- an active gap is not resolved by pruning;
- pruning requires no notification or state transition in the Slice; and
- after skip-ahead, pruning primarily bounds persisted tracking-state size.

Pruning is the intentional expiration policy for unresolved gaps. A gap is
retained at least until both the clock and byte horizons are exceeded relative
to newer producers, and is removed on a later recovery scan. These horizons
are a retention floor rather than a fixed expiry deadline: an idle journal or
an absence of later recovery may retain the gap indefinitely.

The horizons no longer bound nominal restart read amplification. Changing them
is outside this work.

## Observability

Events SHOULD identify the session, binding, journal, and producer and MUST
avoid document content.

Required events are:

- gap creation, with `F`, `M`, `R`, and configured bound `B`;
- backfill trigger, with `F` and `ack_begin`;
- backfill completion, with range bytes, physical bytes read, and duration;
- gap resolution by clean or deep rollback, including which outcome occurred
  and confirmation that no historical I/O was issued;
- gap resolution by OUTSIDE;
- gap release because its read or session ended; and
- backfill failure, with the phase and error.

Required metrics are:

- gauge of unresolved gapped producers per Slice, including gaps currently
  backfilling until their final ACK is flushed;
- counters of backfills started, completed, and failed;
- counter of physical backfill bytes read; and
- histogram of backfill duration and requested range size.

Restart-resolution metrics SHOULD also report bytes conservatively re-read
(`M - R`) and counts of near-frontier producers recovered normally versus
far producers classified as gapped. These measurements inform production
tuning of `B`.

Existing forward-read and bytes-behind metrics MUST remain monotonic and MUST
exclude historical backfill I/O.

## Accepted risks and explicit non-goals

### Historical fragment loss

If fragments covering part of `[F, ack_begin)` have expired, a Gazette read may
fast-forward over the hole. The final ACK can then commit a partial span. This
is the same exposure as today's conservative read and remains accepted. The
event and byte metrics must make a suspiciously short backfill diagnosable.

### Zombie commit after pruning

A producer removed from the recovered checkpoint is no longer known to be
gapped. If it later commits, its old span is silently unavailable. This is
unchanged from current pruning semantics.

### Disk-limit wedge

A source transaction whose per-shard log footprint exceeds
`shuffle_disk_limit_bytes` can exhaust disk backlog before its ACK becomes
visible. Backfill inherits this pre-existing bound and does not attempt to
solve it.

### Parked journal

The triggering journal makes no forward progress while historical data is
read. This cost is paid only when a tracked producer actually commits and is
bounded by `[F, ack_begin)`.

### Global priority order

Backfilled documents may appear after documents that would sort later by
priority or adjusted clock. Restoring global order would require a historical
log class or reader-side merge and is explicitly not part of this design. In
particular, same-key last-write-wins processing across different producers may
temporarily or durably prefer the historical value until another update
arrives.

### Persisted journal watermark

No per-journal read watermark is added to the checkpoint. If the only recorded
event is the open span itself, then `M == R == F` and restart remains
conservative. A persisted watermark could improve that case but requires
protocol and persistence changes.

### Backfill amortization

A backfill for one producer does not sequence or close another producer's
overlapping gap. Filtering remains producer-specific.

## Correctness properties

An implementation satisfies these requirements only if all of the following
hold:

1. **No committed transaction is skipped.** Normal producers are read from a
   checkpoint-derived position; every skipped gapped span is recovered before
   a committing ACK is sequenced, or is durably discarded by rollback or a
   newer OUTSIDE commit.
2. **Exactly-once delivery is preserved.** Main-read documents of a gapped
   producer are suppressed, and the main read resumes after the ACK, so the
   backfill is the only append path for the recovered span.
3. **Producer order is preserved.** Historical documents are appended in
   journal order before the ACK, while later main-read documents remain parked.
4. **Transaction visibility is atomic.** No producer checkpoint advancement is
   emitted until every recovered append is flushed with the committing ACK and
   causal hints. Unchanged producer state in an unrelated Frontier is harmless.
5. **Rollback is durable.** An ACK at or below `last_commit` replaces positive
   `F` with committed `-ack_end`, so the gap does not return after restart.
   Deep rollback retains existing monotonic Frontier-reduction semantics for
   `last_commit`.
6. **Recovery is idempotent.** Failure before the final flush reconstructs the
   same gap and safely repeats the historical read in a new, empty log session.
7. **Main-read progress is coherent.** Historical offsets never regress or
   inflate forward-read checkpoint metrics.

## Validation

The scenario fuzz harness remains the primary acceptance gate for delivery,
per-producer order, causal hints, and atomic transaction visibility. Its
generator must add sessions in which:

- a checkpoint contains both a fresh committed producer and a stale open span;
- the gapped producer remains silent;
- the gapped producer commits after restart;
- the gapped producer cleanly rolls back at `last_commit`;
- the gapped producer deeply rolls back below `last_commit`, resolving without
  historical I/O;
- the gapped producer writes a newer OUTSIDE document;
- multiple live producers have clustered open spans in one journal, under both
  `B = 0` and a non-zero bounded re-read;
- multiple far-behind producers are gapped in one journal;
- a gapped transaction spans multiple journals and resolves causal hints; and
- teardown occurs before trigger handling, during historical I/O, after
  historical appends, and before the final flush completes.

Deterministic coverage must additionally verify:

- `M`, `R`, and `B` resolution and gap classification, including `B = 0`, an
  unbounded `B`, and `offset == 0`;
- the complete gapped outcome table;
- durable rollback followed immediately by restart;
- deep rollback while gapped clears the gap without a backfill and preserves
  monotonic reduced-Frontier semantics;
- rejection of a distinct intermediate target-producer ACK in a backfill;
- neither a parked main read nor a cold historical read engages the
  all-reads-tailing drain gate or ordinary stalled-read accounting;
- global priority/adjusted-clock inversion is tolerated for a backfill while
  per-producer order remains strict;
- FULL suspension releases gap state after fragments are gone, and a later
  appearance starts fresh;
- an ambiguous hinted producer with `offset == 0` is conservatively backfilled
  from zero and commits its post-`R` suppressed span;
- clustered live producers within `B` are recovered in one main-read pass, with
  conservative re-read bounded by `B`, rather than serialized backfills;
- main-read byte deltas remain monotonic and exclude physical backfill bytes;
- backfill trigger and completion interact correctly with `recovery_pending`
  and the first checkpoint's peek/gating; and
- backfill retry and cancellation release all parked state cleanly.

Acceptance requires the dominant stale-producer case to start within `B` bytes
of the fresh checkpoint maximum rather than at the old span begin. A later
commit must still deliver the complete source transaction exactly once, while
clean and deep rollback perform no historical I/O. With `B = 0`, the maximum
offset path and all applicable backfill transitions must be fully exercised.
