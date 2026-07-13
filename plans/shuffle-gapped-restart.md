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

On restart, each journal begins at the furthest position justified by its
checkpoint. Producers whose pending spans begin before that position are
remembered as *gapped* and skipped. Old dead producers then cost no further
I/O. If one later emits any newer document, shuffle parks at that document,
backfills only the skipped `[F, trigger)` range, then re-presents the parked
document to the normal read path — which extends the reconstructed span (a
CONTINUE trigger) or commits it (an ACK trigger) through the ordinary log and
Frontier machinery. No prefix of a still-uncommitted span becomes visible,
because visibility stays gated by the absence of a committing ACK exactly as it
is for any live open span.

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
```

An empty checkpoint resolves to `M = 0`.

The main journal read starts at `M`, subject to the same broker
`begin_mod_time` and fragment-hole behavior as existing reads.

Each recovered producer is classified as follows:

| Checkpoint entry | Classification |
|---|---|
| Committed offset `-O` | Normal, with `last_commit` and committed end `O` recovered as today. |
| Uncommitted begin `F == M` | Normal. The main read encounters the span from its beginning. |
| Uncommitted begin `F`, where `F < M` | Gapped. The main read skips `[F, M)`. |

Because `M` is the maximum magnitude, an uncommitted `F > M` is impossible.

An entry with `offset == 0` may be a hint-only producer or a real span that
began at journal offset zero. When `M > 0`, it is gapped with `F = 0`. A later
commit may therefore require a targeted read from the beginning of the
journal. This is no worse than today's conservative restart from zero.

Hint projection does not make this case distinguishable. A real pending span
at offset zero can also carry a non-zero `hinted_commit`, and Frontier reduction
erases whether an offset-zero entry came only from a hint or also from committed
state. Classifying apparent hint-only entries as normal would therefore risk
skipping a real span. The conservative `F = 0` classification is deliberate.
Although `[0, M)` is often dead weight for a true hint-only producer, that cost
is paid only if the producer later emits a real document (the trigger); the
backfill of `[0, trigger.begin)` then recovers its whole span, reading each byte
once. Nothing is re-read: the main read never sequences a gapped producer's
documents, so `[M, trigger.begin)` is scanned only by the backfill and the
trigger onward only by the main read.

The journal write head MUST NOT be used in place of `M`. Bytes between a
checkpoint-derived position and the write head may contain transactions that
no prior session sequenced.

### Why skip-ahead is safe

Consider an entry relative to `M`:

- A committed end `O <= M` needs no replay. If this producer had an unseen
  document in `(O, M)`, checkpoint closure would require its entry to reflect
  that document as either a later commit or an uncommitted begin.
- An uncommitted begin `F == M` is encountered from its first document by the
  main read.
- An uncommitted begin `F < M` is the only case where documents are skipped.
  The exact missing lower bound is retained as gap start `F`.

The argument remains inductive across later sessions because a gapped
producer is frozen and omitted from progress until it resolves. Other
producers may advance the checkpoint and increase a future `M`, but the
gapped producer continues to pin its original `F`.

## Producer state machine

A producer is in one of three states:

```text
normal  -> gapped        only during checkpoint recovery
gapped -> normal         on clean/deep rollback or a newer OUTSIDE commit
gapped -> backfilling    on the first newer non-duplicate document (CONTINUE or ACK)
backfilling -> normal    when the historical read completes — i.e. BEFORE the
                         trigger itself commits, for a CONTINUE or ACK trigger alike
```

The `backfilling -> normal` transition happens at historical-read completion,
not at commit: the reconstructed open span is installed, the gap is removed, and
the parked trigger is handed back to the normal read path. The trigger then
extends the span (CONTINUE) or commits it (ACK) as an ordinary main-read
document.

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

### Main-read outcomes while gapped

The following table is normative. “Commit progress” means setting a committed
negative offset, extracting causal hints when the document is an ACK, and
causing a flush through the normal commit path.

| Document | Required behavior |
|---|---|
| CONTINUE or ACK, clock `> last_commit` | Park the main read at this document (the *trigger*) and begin a backfill of `[F, trigger.begin)`. The trigger is not sequenced now; it is re-presented to the normal read path when the backfill completes. |
| CONTINUE, clock `<= last_commit` | Drop (duplicate) and retain the gap. |
| ACK, clock `== last_commit` | Resolve as a clean rollback: discard the gap and emit commit progress with `offset = -ack_end`. Extract causal hints and flush. No historical I/O. |
| ACK, clock `< last_commit` | Resolve directly as a deep rollback, without backfill: discard the gap, warn, set live `last_commit` to the ACK clock, set `offset = -ack_end`, extract causal hints, and flush. |
| OUTSIDE, clock `> last_commit` | Discard the gap, then process the OUTSIDE commit normally. No historical I/O. |
| OUTSIDE, clock `<= last_commit` | Drop and retain the gap. |

A gapped producer's frozen `max_continue = 0` makes ordinary `uuid::sequence`
misclassify these rows (rollbacks look like `AckDuplicate`, a newer CONTINUE
looks like `ContinueBeginSpan`), so a gapped document is classified by this
table rather than by the normal sequencer. A newer CONTINUE and a newer ACK are
deliberately merged into one trigger row: any live document voids the
presumption that the producer is dead, and for a CONTINUE the producer's
remaining span and committing ACK are almost certainly just ahead in the
journal.

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

The first newer document (a CONTINUE or ACK with `clock > last_commit`)
encountered for a gapped producer is the *trigger*. Both kinds trigger the same
backfill: a live document of any kind voids the presumption that the producer is
permanently dead, and for a CONTINUE the producer's remaining span and
committing ACK are almost certainly already in the journal just ahead.

The main read MUST park at the trigger before sequencing it:

- the trigger document and metadata are retained;
- the logical main-read position does not advance past the trigger;
- no later document of that journal is processed; and
- at most one backfill is active for a `(binding, journal)` read.

The parked main read MUST be held outside the Slice's pending-read tailing gate
and MUST NOT count as an ordinary stalled read. The historical read is likewise
exempt from the tailing gate and ordinary stall accounting. Intentional parking
is reported only through backfill-specific events and metrics.

Other journals may continue to make progress, subject to ordinary shared Log
back-pressure and flush coordination.

### Historical range

Backfill reads the half-open range `[F, trigger.begin)` from the same journal
and binding as the main read. It uses the binding's normal authorization,
`begin_mod_time`, schema validation, partition filtering, and journal routing.
Fragments may be fetched from cold storage.

This range is read exactly once, and the main read reads everything from the
trigger onward exactly once — the two ranges are disjoint. This is a strict I/O
improvement over a design that suppresses a gapped producer's post-`M`
main-read CONTINUEs and then re-reads `[M, trigger.begin)` in the backfill.

Every decoded document is inspected for its producer identity. Documents of
other producers are skipped without sequencing, state mutation, key
extraction, or log append. They are already represented by checkpoint state or
belong to independent gaps.

Documents of the target producer are sequenced and appended through the
normal path against live producer state initialized from the recovered
`last_commit`, `max_continue = 0`, and offset `F`.

### One-transaction invariant

The historical range reconstructs one open (uncommitted) source transaction and
holds no committing transaction boundary:

- any state-changing ACK below `M` would already be reflected by checkpoint
  closure;
- the trigger is the first target-producer document the main read reaches at or
  above `M`, so `[M, trigger.begin)` contains no target-producer document at
  all; and
- therefore no committing document can occur inside `[F, trigger.begin)`.

The range may contain at-least-once duplicates, which normal sequencing drops.
A committing document inside the range contradicts the recovered checkpoint and
is a terminal consistency error rather than an intermediate transaction to
expose.

### Completion

Completion commits nothing. After the historical range reaches `trigger.begin`:

1. The reconstructed producer state (`live`) is installed into the read's
   pending map as the recovered open span.
2. The gap is removed; the producer is now normal.
3. The parked trigger is re-presented to the ready heap with its head document
   unconsumed.

The drain loop then re-pops the trigger and sequences it through the wholly
normal path. Because the producer is no longer gapped and its pending state is
the reconstructed open span:

- a CONTINUE trigger sequences as `ContinueExtendSpan` (or `ContinueBeginSpan`
  when the backfill found nothing) and appends; and
- an ACK trigger sequences as `AckCommit` and commits — causal hints, committed
  offset, and flush — through the existing normal-path code.

Installing `live` is safe, and is why there is no longer an atomic visibility
boundary to enforce. `live.offset` is the span begin `F`, identical to what the
durable checkpoint already records, so a flush carrying it changes nothing
durably. (On fragment loss the first found document begins at `F' > F`; if that
`F'` leaks to a durable base and the session then crashes, recovery re-gaps at
`F'`, and `[F, F')` was unreadable anyway — self-consistent.) An open
uncommitted span is a state every existing mechanism already handles: visibility
stays gated by the absence of an ACK, exactly as for any normal mid-span
producer. A crash any time before the eventual ACK flush recovers the unchanged
positive `F`, re-creates the gap, and repeats idempotently.

An empty reconstructed span (no target CONTINUEs found) is possible only when
historical documents were unavailable or filtered by existing journal-read
semantics, or benignly for a hint-only producer backfilled from `F = 0`; the
completion event reports it as `span_empty`. Under a CONTINUE trigger the
re-presented document then sequences as `ContinueBeginSpan` and the span simply
begins at the trigger.

## Ordering and scheduling

Backfill is an explicit exception to shuffle's global
`(priority DESC, adjusted_clock ASC)` processing order.

A pending cold read has no document in the ready heap. Normal documents may be
emitted before an older or higher-priority backfill document arrives, and logs
may already contain later documents by the time backfill begins. Shuffle does
not attempt to restore global ordering across that boundary.

This relaxation is safe because:

- downstream processing does not require global producer-clock order;
- the recovered source transaction becomes visible atomically only when its
  committing ACK is sequenced by the normal path — inherited ACK-gated
  visibility, not a special boundary;
- causal hints continue to gate cross-journal transaction visibility; and
- strict order for the target `(binding, journal, producer)` is preserved: its
  main-read documents below the trigger are skipped, the backfill appends
  `[F, trigger.begin)` in journal order, and the re-presented trigger then
  continues in order from `trigger.begin` onward.

The relaxation is nevertheless user-visible. An old backfilled document may be
processed after a newer same-key document written by another producer. A
last-write-wins reduction can therefore select the historical value until a
later update supersedes it. Downstream consumers are required to tolerate this
cross-producer inversion; source-transaction atomicity, not global value order,
is the guarantee retained by this design.

Backfilled documents are appended directly as an atomic sequence, without
participating in the ready-heap ordering or the normal clock-delay gate. This
is justified as follows. The trigger document is sequenced only after it clears
the clock-delay gate — that is the definitive evidence used to begin backfill.
Every CONTINUE in `[F, trigger.begin)` shares the trigger's binding, hence its
`read_delay`, and per-producer clocks are strictly ascending, so each
historical document's clock is strictly below the trigger's. Its adjusted clock
(`clock + read_delay`) is therefore already in the past whenever the trigger's
is. Priority is likewise identical across the binding. Heap ordering, priority
ordering, and clock-delay gating are thus provable no-ops for backfilled
documents, and an implementation MAY append them directly, provided all such
appends precede the re-presented trigger's own append or commit on each shard
log.

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
  for other producers, are folded into the aggregate `bytes_read` counter but
  never into the forward-read Frontier deltas.

After the re-presented trigger is consumed by the normal path, the main read's
`read_offset` advances to the trigger's end offset, and the main read continues
past it.

## Causal hints and recovery checkpoints

An unresolved hint (`hinted_commit > last_commit`) needs no special replay
path. If the hinted producer is gapped, the target journal reaches a newer
document, triggers a backfill, and — once the backfill completes and the
re-presented ACK is sequenced by the normal path — resolves the hint through the
ordinary commit Frontier, exactly as a live ACK would. Hints are therefore
extracted by the normal path when the ACK is consumed, not by any backfill-
specific code.

The trigger cannot have been fully sequenced by a prior checkpoint; otherwise
its `last_commit` would already reflect it. The existing checkpoint peek
mechanism remains unchanged.

If a session ends before the trigger's eventual commit flush, no committed
progress for the backfilling producer has been exposed — an interim flush can
carry only the unchanged open span at `F`. The next session recovers the
positive offset `F`, recreates the gap, and encounters the trigger again.
Appends belonging only to the failed session are discarded under the existing
fail-fast log model.

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

A discarded read's in-memory `gaps` are left inert rather than actively
cleared: the read is never re-served (its slot is not reused) and nothing
downstream consumes them. Only an active backfill needs teardown, so that its
parked main read and in-flight historical read are released.

### Failure handling

Terminal historical-read, decoding, validation, sequencing, append, or flush
errors follow the existing fail-fast topology teardown. Transient journal I/O
uses the existing retry model. The parked trigger remains unsequenced until the
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

The structured events are the primary observability surface. Metrics are kept
to a deliberately small set: per-occurrence detail belongs in events, and each
metric is a tracked series that should earn its place.

Events SHOULD identify the session, binding, journal, and producer and MUST
avoid document content.

Required events are:

- gap creation, with `F` and `M`;
- backfill trigger, with `F`, `trigger_begin`, and whether the trigger is an ACK;
- backfill completion, with range bytes, physical bytes read, duration, and
  `span_empty` (a fragment-loss indicator, also benign for a hint-only producer
  backfilled from `F = 0`);
- gap resolution by clean or deep rollback, including which outcome occurred
  and confirmation that no historical I/O was issued;
- gap resolution by OUTSIDE; and
- a backfill stopped by journal removal, reported as a benign stop.

Terminal backfill errors (read, decode, validation, sequencing, append, or
flush) get no dedicated event: they fail-fast into the existing session
teardown, which is itself the signal.

Required metrics are:

- counter of backfills started; and
- counter of backfills stopped: the historical read completed (the trigger was
  re-presented), a benign journal removal stopped the backfill as an EOF stops a
  main read, or the session tore down while the backfill was in flight (counted
  on drop). `started - stopped`
  is the live in-flight count, deriving that gauge without gauge maintenance and
  mirroring the read `started` / `stopped` pair.

The count of uncommitted producers classified as gapped on restart is not
tracked as a metric; the per-producer gap-creation event (with `F` and `M`)
already records each occurrence.

Physical bytes fetched by historical reads are folded into the existing
`bytes_read` counter rather than tracked as a separate series.

The forward-read Frontier deltas (`bytes_read_delta`, `bytes_behind_delta`)
MUST remain monotonic and describe only forward main-read progress; they
exclude historical backfill I/O even though the aggregate `bytes_read` counter
includes it.

## Accepted risks and explicit non-goals

### Historical fragment loss

If fragments covering part of `[F, trigger.begin)` have expired, a Gazette read
may fast-forward over the hole. The eventual ACK can then commit a partial span.
This is the same exposure as today's conservative read and remains accepted. The
`span_empty` event flag and the byte metrics must make a suspiciously short
backfill diagnosable.

### Wasted backfill on a post-`M` rollback

A resumed producer that reopens and then *rolls back* its span above `M` still
triggers a backfill (the reopening CONTINUE is a newer document), whose work is
discarded when the rollback ACK is later sequenced. This is rare: normal Gazette
producer recovery writes its rollback ACK first, and that ACK — at or below
`last_commit` — hits the zero-I/O clean-rollback row before any CONTINUE
reopens a span. Accepted rather than special-cased.

### Uncommitted appends before commit certainty

Backfilled CONTINUE documents occupy shard logs before their transaction's
commit is certain, since completion installs the open span and appends its
documents ahead of the trigger's eventual ACK. This is the same exposure as any
live open span a Slice reads in real time, and the disk-limit bound below
applies identically.

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
read. This cost is paid only when a tracked producer emits a newer document and
is bounded by `[F, trigger.begin)`.

### Global priority order

Backfilled documents may appear after documents that would sort later by
priority or adjusted clock. Restoring global order would require a historical
log class or reader-side merge and is explicitly not part of this design. In
particular, same-key last-write-wins processing across different producers may
temporarily or durably prefer the historical value until another update
arrives.

### Persisted journal watermark

No per-journal read watermark is added to the checkpoint. If the only recorded
event is the open span itself, then `M == F` and restart remains
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
2. **Exactly-once delivery is preserved.** The recovered span's appends split
   across two disjoint ranges — the backfill covers `[F, trigger.begin)` and the
   main read covers the trigger onward. A gapped producer's main-read documents
   below the trigger are never sequenced, and per-producer sequencing drops any
   at-least-once duplicate across the boundary, so each document appends once.
3. **Producer order is preserved.** Historical documents are appended in journal
   order while later main-read documents remain parked; the re-presented trigger
   then continues strictly from `trigger.begin`.
4. **Transaction visibility is atomic — inherited from normal operation.** The
   backfill installs only the open span (offset `F`), which advances no
   visibility. The transaction becomes visible only when the re-presented ACK
   commits through the ordinary path, flushing every recovered append with the
   committing progress and causal hints together — the same ACK-gated atomicity
   as any live transaction. An interim flush carrying the unchanged open span is
   harmless.
5. **Rollback is durable.** An ACK at or below `last_commit` replaces positive
   `F` with committed `-ack_end`, so the gap does not return after restart.
   Deep rollback retains existing monotonic Frontier-reduction semantics for
   `last_commit`.
6. **Recovery is idempotent.** Failure before the trigger's eventual commit
   flush reconstructs the same gap (an interim flush can only carry the unchanged
   open span at `F`) and safely repeats the historical read in a new, empty log
   session.
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
- multiple live producers have clustered open spans in one journal;
- multiple far-behind producers are gapped in one journal;
- a gapped transaction spans multiple journals and resolves causal hints; and
- teardown occurs before trigger handling, during historical I/O, after
  historical appends, and before the trigger's commit flush completes.

Deterministic coverage must additionally verify:

- `M` resolution and gap classification, including `offset == 0`;
- the complete gapped outcome table;
- a CONTINUE-triggered backfill that later commits via the main-read ACK;
- an ACK-triggered backfill (all CONTINUEs below `M`) that commits when the
  trigger is re-presented on completion;
- an empty backfill under a CONTINUE trigger reports `span_empty` and the span
  begins at the trigger;
- a flush landing between completion and the parked trigger's consumption leaves
  durable state at the recovered `(last_commit, F)`;
- durable rollback followed immediately by restart;
- deep rollback while gapped clears the gap without a backfill and preserves
  monotonic reduced-Frontier semantics;
- rejection of a committing document inside a backfill's historical range;
- neither a parked main read nor a cold historical read engages the
  all-reads-tailing drain gate or ordinary stalled-read accounting;
- global priority/adjusted-clock inversion is tolerated for a backfill while
  per-producer order remains strict;
- FULL suspension releases gap state after fragments are gone, and a later
  appearance starts fresh;
- an ambiguous hinted producer with `offset == 0` is conservatively backfilled
  from zero and commits its post-`M` skipped span;
- main-read byte deltas remain monotonic and exclude physical backfill bytes;
- backfill trigger and completion interact correctly with `recovery_pending`
  and the first checkpoint's peek/gating; and
- backfill retry and cancellation release all parked state cleanly.

Acceptance requires the dominant stale-producer case to start at the fresh
checkpoint maximum rather than at the old span begin. A later
commit must still deliver the complete source transaction exactly once, while
clean and deep rollback perform no historical I/O.
