use super::{
    gap::Backfill,
    heap::{ReadyReadEntry, ReadyReadHeap},
    read::{
        Meta, ReadFailure, ReadState, ReadyRead, classify_read_failure, map_read_error,
        parse_lines_batch, probe_read_start,
    },
    routing,
    state::{self, FlushState, ProgressState, Topology},
};
use crate::log;
use anyhow::Context;
use futures::{FutureExt, StreamExt, future, stream};
use proto_flow::shuffle;
use proto_gazette::{broker, uuid};
use tokio::sync::mpsc;

/// SliceActor implements the main event loop of a shuffle Slice RPC.
#[allow(dead_code)]
pub struct SliceActor {
    /// Immutable slice configuration: topology, bindings, journal clients.
    pub topology: Topology,
    /// Per-binding schema validators, indexed by binding index.
    pub validators: Vec<doc::Validator>,
    /// Per-read producer tracking
    pub reads: Vec<ReadState>,
    /// Causal hints accumulated from consumed ACK documents. Drained during flush.
    pub causal_hints: super::CausalHints,
    /// State machine for tracking flush cycles with Log shards.
    pub flush: FlushState,
    /// State machine for tracking progress reporting with the Session.
    pub progress: ProgressState,
    /// Channel for sends to parent Session.
    pub slice_response_tx: mpsc::Sender<tonic::Result<shuffle::SliceResponse>>,
    /// Channels for sends to shard Log RPCs, indexed by shard index.
    pub log_request_tx: Vec<mpsc::Sender<shuffle::LogRequest>>,
    /// Previous journal name sent to each Log shard, for delta encoding.
    pub log_prev_journal: Vec<String>,
    /// Pending Journal read-start probes for newly started reads.
    /// Each resolves to `(start_offset, read)`, where `start_offset` is the
    /// read's fast-forwarded starting offset used to seed its `ReadState`.
    pub pending_probes: stream::FuturesUnordered<
        future::BoxFuture<'static, anyhow::Result<(i64, super::ReadLines)>>,
    >,
    /// Reads that are awaiting more data from Gazette brokers.
    pub pending_reads: stream::FuturesUnordered<stream::StreamFuture<super::ReadLines>>,
    /// Historical backfill reads awaiting broker/storage I/O. A read lives here only
    /// while no batch of it is being drained; once a batch resolves it becomes the
    /// owning `Backfill`'s cursor, returning here only after the cursor drains.
    pub pending_backfills: stream::FuturesUnordered<stream::StreamFuture<super::ReadLines>>,
    /// Number of pending reads that are caught up to their journal write head.
    /// We defer sending Append requests until all pending reads are tailing,
    /// ensuring no pending read has content that could preempt the current heap top.
    pub tailing_reads: usize,
    /// Read IDs currently pending AND non-tailing: parked awaiting broker I/O
    /// while still behind their journal write head. This is exactly the set that
    /// head-of-line-blocks heap draining (see the gate in `try_log_request_tx`).
    pub stalled_reads: std::collections::HashSet<u32>,
    /// Shard parser for transcoding documents from LinesBatch.
    pub parser: simd_doc::SimdParser,
    /// Ordered heap of reads with ready documents.
    pub ready_read_heap: ReadyReadHeap,
    /// Active backfills of gapped producers' pending transactions, keyed by read
    /// id. Each consolidates the parked main read (holding the trigger ACK head,
    /// its undrained tail, and the inner `ReadLines`), the live target-producer
    /// sequencing state, and the current historical batch cursor.
    pub backfills: std::collections::HashMap<u32, Backfill>,
    /// Per-task metrics counters and gauges.
    pub metrics: super::Metrics,
}

impl Drop for SliceActor {
    /// Balance the `*_started` / `*_stopped` counters at session teardown (clean
    /// Session EOF or task cancellation). Reads and backfills still running here
    /// stop by teardown rather than through their normal per-journal paths;
    /// counting them keeps `started - stopped` a live count that returns to zero
    /// across sessions sharing a `shard_id` label.
    fn drop(&mut self) {
        let live_reads = self.pending_probes.len()
            + self.pending_reads.len()
            + self.ready_read_heap.len()
            + self.backfills.len();
        self.metrics.reads_stopped.increment(live_reads as u64);
        self.metrics
            .backfills_stopped
            .increment(self.backfills.len() as u64);
    }
}

pub(super) struct Buffers {
    packed_key: bytes::BytesMut,
    targets: Vec<usize>,
    permits: Vec<mpsc::Permit<'static, shuffle::LogRequest>>,
}

impl SliceActor {
    #[tracing::instrument(
        level = "debug",
        ret,
        err(Debug, level = "warn"),
        skip_all,
        fields(
            session = self.topology.session_id,
            shard_id = %self.topology.shards[self.topology.slice_shard_index as usize].id,
        )
    )]
    pub async fn serve<R>(
        mut self,
        mut slice_request_rx: R,
        log_response_rx: Vec<stream::BoxStream<'static, tonic::Result<shuffle::LogResponse>>>,
    ) -> anyhow::Result<()>
    where
        R: futures::Stream<Item = tonic::Result<shuffle::SliceRequest>> + Send + Unpin + 'static,
    {
        let cancel = tokens::CancellationToken::new();
        let _drop_guard = cancel.clone().drop_guard();

        // Build a Stream over receive Futures for every Log RPC.
        let mut log_response_rx: stream::FuturesUnordered<_> = log_response_rx
            .into_iter()
            .enumerate()
            .map(next_log_rx)
            .collect();

        // Await Start from the Session RPC.
        let verify = crate::verify(
            "SliceRequest",
            "Start",
            &self.topology.shards[0].endpoint,
            0,
        );
        match verify.not_eof(slice_request_rx.next().await)? {
            shuffle::SliceRequest {
                start: Some(shuffle::slice_request::Start {}),
                ..
            } => (),
            request => return Err(verify.fail(request)),
        };

        // Spawn tasks that watch journal listings of assigned bindings.
        let mut listing_tasks: stream::FuturesUnordered<
            tokio::task::JoinHandle<Option<anyhow::Error>>,
        > = self.spawn_listings(&cancel);

        // Re-usable scratch buffers.
        let mut buffers = Buffers {
            packed_key: bytes::BytesMut::new(),
            targets: Vec::new(),
            permits: Vec::new(),
        };

        // Measure of wall-clock time, used to gate delayed reads.
        let mut now = uuid::Clock::zero();

        let mut ticker = tokio::time::interval(crate::ACTOR_TICKER_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut loop_count: u64 = 0;
        loop {
            loop_count += 1;
            tracing::trace!(
                loop_count,
                total_reads = self.reads.len(),
                tailing_reads = self.tailing_reads,
                stalled_reads = self.stalled_reads.len(),
                pending_probes = self.pending_probes.len(),
                pending_reads = self.pending_reads.len(),
                ready_heap = self.ready_read_heap.len(),
                flush = ?self.flush,
                progress = ?self.progress,
                "SliceActor::serve iteration"
            );
            // First, attempt non-blocking sends.
            let wake_log_request_tx = self.try_log_request_tx(&mut buffers, &mut now)?;
            let wake_slice_response_tx = self.try_slice_response_tx()?;

            // Then, wait for a blocking future to resolve.
            tokio::select! {
                biased;

                // First priority is receiving messages.
                slice_request = slice_request_rx.next() => {
                    match slice_request {
                        Some(result) => self.on_slice_request(result)?,
                        None => break,
                    }
                }
                Some((shard_index, log_response, rx)) = log_response_rx.next() => {
                    self.on_log_response(shard_index, log_response)?;
                    log_response_rx.push(next_log_rx((shard_index, rx)));
                }

                // Next priority is draining ready-to-send messages.
                true = wake_log_request_tx => {}
                true = wake_slice_response_tx => {}

                // Lowest priority is processing journal listings and reads.
                Some(probe_result) = self.pending_probes.next() => {
                    let (start_offset, read) = match probe_result {
                        Ok(ok) => ok,
                        Err(err) => {
                            self.metrics.reads_stopped.increment(1);
                            return Err(err);
                        }
                    };
                    // Seed the ReadState's offsets at the probe's resolved start
                    // before parking, so byte deltas exclude the filtered range
                    // the read skipped.
                    self.reads[read.id() as usize].start_at(start_offset);
                    self.park_or_process(read)?;
                }
                Some(listing_result) = listing_tasks.next() => {
                    self.on_listing_task_done(listing_result)?;
                }
                Some((result, read)) = self.pending_reads.next() => {
                    self.on_pending_read_resolved(result, read)?;
                }
                Some((result, read)) = self.pending_backfills.next() => {
                    self.process_backfill_result(result, read)?;
                }

                // Periodic tick ensures tracing fires even when idle.
                _ = ticker.tick() => {}
            }
        }

        service_kit::event!(
            tracing::Level::DEBUG,
            "session",
            loop_count,
            total_reads = self.reads.len(),
            flush_cycle = self.flush.cycle,
            "SliceActor::serve exiting on Session EOF"
        );
        self.log_request_tx.clear(); // Drop all tx handles to close.

        // Read clean EOF from all Log RPCs.
        while let Some((shard_index, slice_response, rx)) = log_response_rx.next().await {
            let verify = crate::verify(
                "LogResponse",
                "EOF",
                &self.topology.shards[shard_index].endpoint,
                shard_index,
            );
            match slice_response {
                None => (), // Clean EOF.
                Some(Ok(_ignored)) => log_response_rx.push(next_log_rx((shard_index, rx))),
                Some(Err(status)) => return Err(verify.fail_status(status)),
            }
        }

        Ok(())
    }

    // Start tasks that watch journal listings of assigned bindings.
    fn spawn_listings(
        &self,
        cancel: &tokens::CancellationToken,
    ) -> stream::FuturesUnordered<tokio::task::JoinHandle<Option<anyhow::Error>>> {
        let out = stream::FuturesUnordered::new();

        for binding in &self.topology.bindings {
            // Use modulo round-robin to assign bindings to slice shards.
            if binding.index % self.topology.shards.len() as u16
                != self.topology.slice_shard_index as u16
            {
                continue;
            }
            let join_handle = super::listing::spawn_listing(
                binding,
                (*self.topology.journal_clients[binding.index as usize]).clone(),
                self.slice_response_tx.clone(),
                cancel.clone(),
            );
            out.push(join_handle);
        }
        out
    }

    fn on_listing_task_done(
        &mut self,
        listing_result: Result<Option<anyhow::Error>, tokio::task::JoinError>,
    ) -> anyhow::Result<()> {
        match listing_result {
            Err(err) => Err(anyhow::Error::new(err).context("listing task panicked")),
            Ok(None) => anyhow::bail!("listing task canceled before SliceActor::serve exited"),
            Ok(Some(err)) => Err(err),
        }
    }

    fn on_slice_request(
        &mut self,
        slice_request: tonic::Result<shuffle::SliceRequest>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify(
            "SliceRequest",
            "Progress or StartRead",
            &self.topology.shards[0].endpoint,
            0,
        );

        match verify.ok(slice_request)? {
            shuffle::SliceRequest {
                progress: Some(shuffle::slice_request::Progress {}),
                ..
            } => {
                service_kit::event!(
                    tracing::Level::DEBUG,
                    "session",
                    "received Progress request"
                );
                self.progress.request()
            }

            shuffle::SliceRequest {
                start_read: Some(start_read),
                ..
            } => self.on_start_read(start_read),

            request => Err(verify.fail(request)),
        }
    }

    pub fn on_start_read(
        &mut self,
        start_read: shuffle::slice_request::StartRead,
    ) -> anyhow::Result<()> {
        let shuffle::slice_request::StartRead {
            binding: binding_index,
            spec,
            create_revision,
            mod_revision: _,
            route,
            checkpoint,
        } = start_read;

        let binding = self
            .topology
            .bindings
            .get(binding_index as usize)
            .context("StartRead invalid binding")?;

        let binding_state_key = binding.state_key().to_string();
        let client = (*self.topology.journal_clients[binding.index as usize]).clone();
        let spec = spec.context("StartRead missing spec")?;
        let journal = spec.name.into_boxed_str();
        let read_id = self.reads.len() as u32;

        // Resolve the checkpoint into producer state, gapped producers, and
        // the start offset M: the checkpoint's furthest justified position.
        let state::ResolvedCheckpoint {
            offset,
            producers,
            gaps,
        } = state::resolve_checkpoint(checkpoint);

        // Emit a gap-creation event per gapped producer (F, M). All gap and
        // backfill lifecycle events share the "backfill" track so an operator
        // sees creation → trigger → completion/rollback as one ordered series.
        for (producer, gap_begin) in gaps.iter() {
            service_kit::event!(
                tracing::Level::DEBUG,
                "backfill",
                session = self.topology.session_id,
                read_id,
                binding = binding.index,
                journal = journal.to_string(),
                producer = service_kit::event::debug(*producer),
                gap_begin = *gap_begin, // F
                start_offset = offset,  // M
                "classified producer as gapped on restart",
            );
        }

        let mut request = broker::ReadRequest {
            // Add `journal_read_suffix` as a metadata component to the journal name.
            // This helps identify the sources of reads from the perspective of a gazette broker.
            journal: format!("{journal};{}", binding.journal_read_suffix),

            begin_mod_time: binding.not_before.to_unix().0 as i64,
            block: true,
            do_not_proxy: true,
            end_offset: 0, // No end offset.
            metadata_only: false,
            offset,
            min_etcd_revision: create_revision,

            // `route` is a hint which directs us to the right broker.
            // This is an optimization and isn't required for correctness.
            header: route.map(|r| broker::Header {
                route: Some(r),
                ..Default::default()
            }),
        };

        service_kit::event!(
            tracing::Level::DEBUG,
            "read",
            read_id,
            binding = binding.index,
            journal = journal.to_string(),
            begin_mod_time = request.begin_mod_time,
            n_producers = producers.len(),
            offset,
            "starting journal read",
        );
        self.metrics.reads_started.increment(1);

        self.reads.push(ReadState::recovered(
            binding_index as u16,
            journal,
            producers,
            gaps,
        ));

        self.pending_probes.push(Box::pin(async move {
            // Probe where this read effectively begins (after `begin_mod_time`
            // fast-forwarding) and the journal's current write head.
            let (start_offset, write_head, probe_header) = probe_read_start(
                client.clone(),
                &request.journal,
                &binding_state_key,
                request.header.take(),
                create_revision,
                offset,
                request.begin_mod_time,
            )
            .await?;

            // Begin the read at the fast-forwarded offset rather than the stale
            // checkpoint `offset`: the skipped range precedes `begin_mod_time` and
            // would be filtered regardless, and starting here lets a read that's
            // caught up past all filtered content be classified as tailing.
            request.offset = start_offset;
            request.header = probe_header;
            let tailing = start_offset >= write_head;

            service_kit::event!(
                tracing::Level::DEBUG,
                "read",
                read_id,
                binding = binding_index,
                journal = request.journal.clone(),
                offset = start_offset,
                tailing,
                write_head,
                "probed journal read start",
            );

            Ok((
                start_offset,
                Box::pin(gazette::journal::read::ReadLines::new(
                    client.read(request).boxed(),
                    read_id,
                    tailing,
                )) as super::ReadLines,
            ))
        }));

        Ok(())
    }

    /// (Re)-introduce `read` into the actor. If its next batch (or terminal status)
    /// is already available, process it immediately. Otherwise the read must await
    /// broker I/O: park it in `pending_reads`, classifying its membership
    /// (a tailing read bumps the `tailing_reads` count, while a non-tailing read
    /// joins `stalled_reads` and emits a `stall` event). Note that
    /// `on_pending_read_resolved` performs an exactly-inverted de-classification
    /// on read resolution.
    ///
    /// Pre-condition: `read` is *not* in `pending_reads`, and carries no
    /// classification membership to undo (that happens in `on_pending_read_resolved`).
    fn park_or_process(&mut self, mut read: super::ReadLines) -> anyhow::Result<()> {
        if let Some(result) = read.next().now_or_never() {
            return self.process_read_result(result, read);
        }

        if read.tailing() {
            self.tailing_reads += 1;
            self.metrics.tailing_reads.set(self.tailing_reads as f64);
        } else {
            // `read` isn't in `pending_reads`, and `stalled_reads` is a strict
            // subset of `pending_reads`.
            let is_new = self.stalled_reads.insert(read.id());
            debug_assert!(
                is_new,
                "read {} was parked while already stalled",
                read.id()
            );

            self.metrics
                .stalled_reads
                .set(self.stalled_reads.len() as f64);
            self.emit_stall_event(
                read.id(),
                "read has stalled heap drain (pending and !tailing)",
            );
        }
        self.pending_reads.push(read.into_future());
        Ok(())
    }

    /// Handle a resolution yielded by `pending_reads`: the read has *left*
    /// `pending_reads`, so de-classify its membership (the inverse of the
    /// classification in `park_or_process`) before processing.
    fn on_pending_read_resolved(
        &mut self,
        result: Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
        read: super::ReadLines,
    ) -> anyhow::Result<()> {
        if self.stalled_reads.remove(&read.id()) {
            self.metrics
                .stalled_reads
                .set(self.stalled_reads.len() as f64);
            self.emit_stall_event(read.id(), "read is no longer stalled");
        } else {
            // `read` wasn't stalled, so must have been tailing.
            self.tailing_reads = self.tailing_reads.strict_sub(1);
            self.metrics.tailing_reads.set(self.tailing_reads as f64);
        }
        self.process_read_result(result, read)
    }

    fn emit_stall_event(&self, read_id: u32, message: &'static str) {
        let read_state = &self.reads[read_id as usize];
        service_kit::event!(
            tracing::Level::DEBUG,
            "stall",
            read_id,
            binding = read_state.binding_index,
            journal = read_state.journal.to_string(),
            read_offset = read_state.read_offset,
            write_head = read_state.write_head,
            "{}",
            message,
        );
    }

    /// Parse a LinesBatch into documents and push a ReadyRead onto the heap, or
    /// handle a terminal/transient status from the underlying ReadLines stream.
    fn process_read_result(
        &mut self,
        result: Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
        read: super::ReadLines,
    ) -> anyhow::Result<()> {
        let read_state = &mut self.reads[read.id() as usize];
        let binding = &self.topology.bindings[read_state.binding_index as usize];
        let journal = read.fragment().journal.clone();

        let Some(result) = result else {
            service_kit::event!(
                tracing::Level::INFO,
                "read",
                read_id = read.id(),
                binding = binding.index,
                journal,
                "stopped journal read (EOF)",
            );
            self.metrics.reads_stopped.increment(1);
            return Ok(());
        };

        let lines_batch = match result {
            Err(gazette::RetryError {
                attempt,
                inner: err,
            }) => match classify_read_failure(err) {
                ReadFailure::JournalRemoved(status) => {
                    self.metrics.reads_stopped.increment(1);
                    // A dead main read has no active backfill (a backfilling read
                    // is parked outside `pending_reads`) and its `gaps` are inert,
                    // so no release is needed here.
                    service_kit::event!(
                        tracing::Level::INFO,
                        "read",
                        read_id = read.id(),
                        binding = binding.index,
                        journal,
                        "stopped journal read ({})",
                        status.as_str_name(),
                    );
                    return Ok(());
                }
                ReadFailure::Transient(err) => {
                    service_kit::event!(
                        tracing::Level::WARN,
                        "read",
                        read_id = read.id(),
                        binding = binding.index,
                        journal,
                        attempt,
                        err = service_kit::event::debug(err),
                        "transient error reading from journal (will retry)",
                    );
                    return self.park_or_process(read);
                }
                ReadFailure::Terminal(err) => {
                    self.metrics.reads_stopped.increment(1);
                    return Err(map_read_error(
                        err,
                        &read_state.journal,
                        binding.state_key(),
                        "reading next lines",
                    ));
                }
            },
            Ok(lines_batch) => lines_batch,
        };

        read_state.write_head = read.write_head();

        service_kit::event!(
            tracing::Level::TRACE,
            "read",
            read_id = read.id(),
            binding = binding.index,
            journal,
            offset = lines_batch.offset,
            length = lines_batch.content.len(),
            tailing = lines_batch.tailing,
            n_tailing = self.tailing_reads,
            "received LinesBatch",
        );

        let ready_read = parse_lines_batch(
            &mut self.parser,
            &mut self.validators[read_state.binding_index as usize],
            binding,
            &read_state.journal,
            read,
            lines_batch,
            "transcoding documents",
        )?;

        self.ready_read_heap.push(ReadyReadEntry {
            priority: binding.priority,
            adjusted_clock: ready_read.meta.clock + binding.read_delay,
            inner: Some(Box::new(ready_read)),
        });

        Ok(())
    }

    fn on_log_response(
        &mut self,
        shard_index: usize,
        log_response: Option<tonic::Result<shuffle::LogResponse>>,
    ) -> anyhow::Result<()> {
        let verify = crate::verify(
            "LogResponse",
            "Flushed",
            &self.topology.shards[shard_index].endpoint,
            shard_index,
        );
        let log_response = verify.not_eof(log_response)?;

        match log_response {
            shuffle::LogResponse {
                flushed: Some(shuffle::log_response::Flushed { cycle, flushed_lsn }),
                ..
            } if cycle == self.flush.cycle => {
                let flushed_lsn = log::Lsn::from_u64(flushed_lsn);

                if let Some(completed) = self.flush.on_flushed(shard_index, flushed_lsn)? {
                    self.progress.on_flush_completed(completed);
                }
                Ok(())
            }

            response => Err(verify.fail(response)),
        }
    }

    fn try_log_request_tx(
        &mut self,
        buffers: &mut Buffers,
        now: &mut uuid::Clock,
    ) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Closure for mapping an OwnedPermit Result to Ok (our "poll again" signal).
        // On Err (channel closed), we don't wake and rely on rx of a causal error / fail-fast teardown.
        let ok = |result: Result<_, _>| result.is_ok();
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(future::Either::Right(std::future::ready(false)));

        loop {
            // A flush cycle takes priority over sending Append requests.
            // We'll await capacity for Flushes even if the next Append shard has capacity.
            if self.flush.should_flush() {
                if let Err(tx) = self.try_log_request_flush_tx(buffers) {
                    return Ok(future::Either::Left(tx.reserve_owned().map(ok)));
                }
            }

            // Drain any historical backfill batch cursors: recovered
            // target-producer documents are appended directly, without heap
            // ordering or clock-delay gating. This is sound because the trigger
            // already cleared the clock-delay gate below, and same-binding
            // read-delay plus strictly-ascending per-producer clocks put every
            // historical document's adjusted clock in the past. Runs after the
            // flush check but independent of the heap and its tailing gate; all
            // backfill appends precede the re-presented trigger's own processing.
            if let Some(tx) = self.try_drain_backfills(buffers)? {
                return Ok(future::Either::Left(tx.reserve_owned().map(ok)));
            }

            // Defer draining if any read could still resolve to content that
            // preempts the current heap top: a parked non-tailing (stalled) read,
            // or a newly-started read still probing its write head (parked in
            // `pending_probes`, not yet classified as tailing/stalled). Parked
            // main reads (`backfills`) and historical backfill reads
            // (`pending_backfills`) are deliberately outside `pending_reads`, so
            // they neither contribute to nor are blocked by this gate.
            if self.tailing_reads != self.pending_reads.len() || !self.pending_probes.is_empty() {
                return Ok(idle);
            }

            // Do we have a document ready for append?
            let Some(ReadyReadEntry {
                adjusted_clock,
                inner: ready_read,
                ..
            }) = self.ready_read_heap.peek()
            else {
                return Ok(idle);
            };
            let adjusted_clock = *adjusted_clock;
            let ready_read = ready_read.as_deref().unwrap();
            let read_id = ready_read.inner.id() as usize;
            let meta = ready_read.meta; // `Meta` is Copy.

            // Gate on the adjusted clock: sleep until wall-clock time catches up.
            // This is how read delays impose cross-transform ordering. Only main
            // reads reach here; backfill documents are drained above.
            if let Some(wait) = state::clock_delay(&adjusted_clock, now, crate::now_clock) {
                return Ok(future::Either::Right(future::Either::Left(
                    tokio::time::sleep(wait).map(|()| true),
                )));
            }

            // Sequence the head document against the read's producer state. This
            // is a pure decision — over gapped or normal state; it mutates
            // nothing — so the caller performs the I/O and, for a gapped
            // producer, resolves the gap and reports the outcome only after the
            // document is irrevocably consumed below. Sequencing stays AFTER the
            // clock-delay gate: a backfill's direct historical appends are sound
            // only because the trigger ACK cleared that gate first.
            let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];
            let (sequenced, gapped) =
                match state::sequence_document(&self.reads[read_id], binding, &meta)? {
                    state::Sequenced::Park => {
                        // A gapped producer's first newer document (CONTINUE or
                        // ACK, the heap head): park the main read at it, then
                        // open its bounded historical backfill. The trigger is
                        // not sequenced now; on completion it is re-presented to
                        // the heap and sequenced through this same normal path.
                        self.park_backfill(read_id as u32, &meta);
                        self.start_backfill(read_id as u32)?;
                        continue;
                    }
                    state::Sequenced::Doc { doc, gapped } => (doc, gapped),
                };

            let read_state = &mut self.reads[read_id];
            let binding = &self.topology.bindings[read_state.binding_index as usize];
            let ready_read = self
                .ready_read_heap
                .peek()
                .unwrap()
                .inner
                .as_deref()
                .unwrap();

            // If this is an Append, attempt to send it to the appropriate shard(s).
            if sequenced.is_append {
                if let Err(tx) = Self::try_log_request_append_tx(
                    binding,
                    buffers,
                    &read_state.journal,
                    &self.topology.shards,
                    &mut self.log_prev_journal,
                    &self.log_request_tx,
                    ready_read,
                ) {
                    return Ok(future::Either::Left(tx.reserve_owned().map(ok)));
                }
            }

            // Pop the heap entry now that any Append requests have been sent.
            // Crucially: we now cannot fail to consume this document.
            let ReadyReadEntry {
                priority,
                inner: ready_read,
                ..
            } = self.ready_read_heap.pop().unwrap();
            let mut ready_read = ready_read.unwrap();

            let ReadyRead {
                inner: read,
                meta:
                    Meta {
                        end_offset,
                        producer,
                        clock,
                        flags,
                        ..
                    },
                doc,
                mut doc_tail,
                mut meta_tail,
            } = *ready_read;

            // Resolve a gapped producer's gap now that its document is
            // irrevocably consumed. This MUST fire post-pop, exactly once: an
            // OutsideResolve with is_append=true can hit a full Log channel
            // before the pop and be re-sequenced on wake, and because sequencing
            // is pure (no gap mutation) that retry re-derives the identical
            // outcome — so gap removal and its event cannot run before the pop.
            // Invariant: a gapped producer's gap is removed exactly when its
            // document commits (rollback or newer OUTSIDE); a Drop — and a
            // non-gapped producer (`None`) — retain it, the latter having none.
            // (A TriggerBackfill never reaches here: it returned `Park` above.)
            match gapped {
                Some(
                    outcome @ (state::GappedOutcome::CleanRollback
                    | state::GappedOutcome::DeepRollback),
                ) => {
                    let deep = matches!(outcome, state::GappedOutcome::DeepRollback);
                    read_state.gaps.remove(&producer);
                    if deep {
                        tracing::warn!(
                            binding=%binding.state_key(),
                            clock=?clock,
                            journal=%read_state.journal,
                            producer=?producer,
                            "gapped producer deep-rolled back prior to its last committed clock (possible loss of exactly-once guarantees)",
                        );
                    }
                    service_kit::event!(
                        tracing::Level::INFO,
                        "backfill",
                        session = self.topology.session_id,
                        read_id,
                        binding = read_state.binding_index,
                        journal = read_state.journal.to_string(),
                        producer = service_kit::event::debug(producer),
                        clock = service_kit::event::debug(clock),
                        deep_rollback = deep,
                        "resolved gap by rollback (no historical I/O)",
                    );
                }
                Some(state::GappedOutcome::OutsideResolve) => {
                    read_state.gaps.remove(&producer);
                    service_kit::event!(
                        tracing::Level::INFO,
                        "backfill",
                        session = self.topology.session_id,
                        read_id,
                        binding = read_state.binding_index,
                        journal = read_state.journal.to_string(),
                        producer = service_kit::event::debug(producer),
                        clock = service_kit::event::debug(clock),
                        "resolved gap via newer OUTSIDE commit (no historical I/O)",
                    );
                }
                _ => {}
            }

            // Track maximum forward progress of the read.
            read_state.read_offset = end_offset;

            if sequenced.is_commit {
                if flags == uuid::Flags::ACK_TXN {
                    // This ACK is (binding, journal)-scoped: it commits only
                    // this producer's documents in this binding's read of this journal.
                    // But, it may contain causal hints of *other* journals which
                    // committed with this one. Extract and project so we can propagate
                    // to the Session, which is tasked with gating checkpoints for
                    // atomic cross-journal visibility.
                    state::extract_causal_hints(
                        &self.topology.hint_index,
                        &read_state.journal,
                        binding.cohort,
                        read_state.binding_index,
                        producer,
                        clock,
                        doc.get(),
                        &mut self.causal_hints,
                    )?;
                }
                self.flush.set_ready();
            }

            // Step producer state forward to reflect the append.
            _ = read_state
                .pending
                .insert(producer, sequenced.producer_state);

            // Copy so the `binding` borrow ends here, freeing &mut self for re-borrow.
            let read_delay = binding.read_delay;

            // Advance doc_tail and meta_tail in lock-step (guaranteed equal length).
            match (doc_tail.next(), meta_tail.next()) {
                (Some((doc, _)), Some(meta)) => {
                    // Re-structure into the existing Box to re-use it.
                    *ready_read = ReadyRead {
                        doc,
                        meta,
                        doc_tail,
                        meta_tail,
                        inner: read,
                    };
                    self.ready_read_heap.push(ReadyReadEntry {
                        priority,
                        adjusted_clock: ready_read.meta.clock + read_delay,
                        inner: Some(ready_read),
                    })
                }
                // This read's batch is fully drained, and we must await I/O.
                (None, None) => self.park_or_process(read)?,
                _ => unreachable!("doc_tail and meta_tail have equal length"),
            }
        }
    }

    /// Try to send Flush requests to all log channels (all-or-nothing).
    /// Returns `Err(tx)` with the sender that lacked capacity.
    fn try_log_request_flush_tx(
        &mut self,
        buffers: &mut Buffers,
    ) -> Result<(), mpsc::Sender<shuffle::LogRequest>> {
        let Buffers { permits, .. } = buffers;

        // Safety: `permits` is always empty on return (retaining only capacity).
        let permits: &mut Vec<_> =
            unsafe { std::mem::transmute::<&mut Vec<_>, &mut Vec<_>>(permits) };

        // Collect permits to send to all log channels (all-or-nothing).
        for tx in &self.log_request_tx {
            let Ok(permit) = tx.try_reserve() else {
                permits.clear();
                return Err(tx.clone());
            };
            permits.push(permit);
        }

        // Build the frontier from pending producers and causal hints,
        // draining pending→settled and resetting byte accumulators.
        let frontier = super::producer::build_flush_frontier(
            &mut self.reads,
            self.causal_hints.drain(),
            self.topology.shards.len(),
        );
        let flush_cycle = self.flush.start(self.log_request_tx.len(), frontier);

        for permit in permits.drain(..) {
            permit.send(shuffle::LogRequest {
                flush: Some(shuffle::log_request::Flush { cycle: flush_cycle }),
                ..Default::default()
            });
        }

        service_kit::event!(
            tracing::Level::DEBUG,
            "log",
            cycle = flush_cycle,
            "broadcast Flush request",
        );
        self.metrics.flushes.increment(1);

        Ok(())
    }

    /// Try to send Append requests to target log channels (all-or-nothing).
    /// Returns `Err(tx)` with the sender that lacked capacity.
    pub(super) fn try_log_request_append_tx(
        binding: &crate::Binding,
        buffers: &mut Buffers,
        journal: &str,
        shards: &[shuffle::Shard],
        log_prev_journal: &mut [String],
        log_request_tx: &[mpsc::Sender<shuffle::LogRequest>],
        ready_read: &ReadyRead,
    ) -> Result<(), mpsc::Sender<shuffle::LogRequest>> {
        let Buffers {
            packed_key,
            permits,
            targets,
        } = buffers;

        let ReadyRead {
            doc,
            meta:
                Meta {
                    begin_offset,
                    end_offset,
                    clock,
                    producer,
                    ..
                },
            ..
        } = ready_read;

        // Extract into `packed_key` and hash to route the document.
        // Compute shard index `targets` to receive an Append of this document.
        packed_key.clear();
        doc::Extractor::extract_all(
            doc.get(),
            &binding.key_extractors,
            doc::Encoding::Packed,
            packed_key,
            None,
        );

        let key_hash = doc::Extractor::packed_hash(packed_key);
        let r_clock = routing::rotate_clock(*clock);

        targets.clear();
        targets.extend(routing::route_to_shards(
            key_hash,
            r_clock,
            binding.filter_r_clocks,
            shards,
        ));

        tracing::trace!(
            %journal,
            binding = binding.state_key(),
            ?producer,
            ?clock,
            begin_offset,
            key_hash,
            flags = ready_read.meta.flags.0,
            r_clock,
            ?targets,
            "routed document Append to Log RPC shards"
        );

        // Safety: `permits` is always cleared prior to return (retaining only capacity).
        let permits: &mut Vec<_> =
            unsafe { std::mem::transmute::<&mut Vec<_>, &mut Vec<_>>(permits) };

        // All-or-nothing: reserve permits for every target channel.
        for &target in targets.iter() {
            let Ok(permit) = log_request_tx[target].try_reserve() else {
                permits.clear();
                return Err(log_request_tx[target].clone());
            };
            permits.push(permit);
        }
        // All channels reserved. At this point, a send is infallible.

        let packed_key = packed_key.split().freeze();

        for (&target, permit) in targets.iter().zip(permits.drain(..)) {
            let prev_journal = &mut log_prev_journal[target];

            let (journal_name_truncate_delta, journal_name_suffix) =
                gazette::delta::encode(prev_journal, journal);
            let journal_name_suffix = journal_name_suffix.to_string();

            // Update `prev_journal` for next iteration.
            gazette::delta::decode(
                &mut log_prev_journal[target],
                journal_name_truncate_delta,
                &journal_name_suffix,
            );

            permit.send(shuffle::LogRequest {
                append: Some(shuffle::log_request::Append {
                    journal_name_truncate_delta,
                    journal_name_suffix,
                    binding: binding.index as u32,
                    priority: binding.priority,
                    read_delay: binding.read_delay.as_u64(),
                    producer: producer.as_i64(),
                    clock: clock.as_u64(),
                    flags: ready_read.meta.flags.0 as u32,
                    packed_key: packed_key.clone(),
                    doc_archived: doc.bytes().clone(),
                    source_byte_length: (end_offset - begin_offset).try_into().unwrap(),
                }),
                ..Default::default()
            });
        }

        Ok(())
    }

    fn try_slice_response_tx(&mut self) -> anyhow::Result<impl Future<Output = bool> + 'static> {
        // Future which represent an absence of an awake signal.
        let idle = future::Either::Right(std::future::ready(false));

        if !self.progress.has_progressed() {
            return Ok(idle);
        }
        // Reserve capacity *before* taking the Frontier — otherwise an absent
        // permit would discard progress that hasn't been emitted yet.
        let Ok(permit) = self.slice_response_tx.try_reserve() else {
            return Ok(future::Either::Left(
                self.slice_response_tx.clone().reserve_owned().map(|_| true),
            ));
        };

        let frontier = self.progress.take_progressed();
        let (journals, journal_producers, bytes_read_delta, bytes_behind_delta) =
            frontier.measures();

        permit.send(Ok(shuffle::SliceResponse {
            progressed: Some(frontier.encode()),
            ..Default::default()
        }));

        service_kit::event!(
            tracing::Level::DEBUG,
            "session",
            bytes_behind_delta,
            bytes_read_delta,
            journal_producers,
            journals,
            "sent Progressed",
        );
        self.metrics
            .bytes_read
            .increment(bytes_read_delta.max(0) as u64);

        Ok(idle)
    }
}

// Helper which builds a future that yields the next response from a shard's Log RPC.
async fn next_log_rx(
    (shard_index, mut rx): (
        usize,
        stream::BoxStream<'static, tonic::Result<shuffle::LogResponse>>,
    ),
) -> (
    usize,                                                           // Shard index.
    Option<tonic::Result<shuffle::LogResponse>>,                     // Response.
    stream::BoxStream<'static, tonic::Result<shuffle::LogResponse>>, // Stream.
) {
    (shard_index, rx.next().await, rx)
}
