use super::{
    heap::{ReadyReadEntry, ReadyReadHeap},
    read::{Meta, ReadState, ReadyRead, map_read_error, probe_read_start},
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
    /// Main reads parked at a gapped producer's trigger ACK, keyed by read id.
    /// The stashed `ReadyRead` holds the ACK document (head), its undrained
    /// tail, and the inner `ReadLines` — so no later document of the journal is
    /// reachable while parked, and nothing re-enters `pending_reads`. Held
    /// outside `pending_reads`/`pending_probes`, so parking is automatically
    /// exempt from the tailing gate and stalled-read accounting.
    pub parked_mains: std::collections::HashMap<u32, ParkedMain>,
    /// Historical backfill reads awaiting broker/storage I/O, keyed by the
    /// backfill-tagged read id. Deliberately separate from `pending_reads`:
    /// exempt from the tailing gate and stall accounting.
    pub pending_backfills: stream::FuturesUnordered<stream::StreamFuture<super::ReadLines>>,
    /// Per-task metrics counters and gauges.
    pub metrics: super::Metrics,
}

/// A main read parked at a gapped producer's trigger ACK while its backfill
/// runs. At most one per read (the read parks at the first trigger, so no
/// second trigger can arrive while parked).
pub struct ParkedMain {
    /// The shelved `ReadyRead`, with the trigger ACK as its head document.
    pub ready: Box<ReadyRead>,
    /// The gapped producer whose ACK triggered the backfill.
    pub target: uuid::Producer,
    /// Trigger instant, for the backfill-duration histogram.
    pub started_at: std::time::Instant,
    /// Physical bytes fetched so far by the historical read, for the
    /// completion event.
    pub physical_bytes: u64,
}

/// Backfill reads are tagged by setting this high bit on their `ReadLines` id;
/// `id & !BACKFILL_ID_BIT` indexes `self.reads`. This keeps `ReadyRead`/heap
/// plumbing identical for main and backfill documents while letting the drain
/// path route them differently.
const BACKFILL_ID_BIT: u32 = 1 << 31;

struct Buffers {
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
                    let (start_offset, read) = probe_result?;
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

        // Release any gapped/backfilling state and cancel in-flight historical
        // reads. Gauges are keyed by shard_id and outlive the session, so zero
        // the gapped-producers gauge explicitly. Recovery is derived purely from
        // durable producer checkpoints, so nothing here is persisted.
        for read_id in 0..self.reads.len() {
            self.release_read_gaps(read_id, "read released (session ended); releasing gap");
        }
        self.parked_mains.clear();
        self.pending_backfills.clear();
        self.metrics.gapped_producers.set(0.0);

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

        // Resolve the checkpoint into producer state, gapped producers, and a
        // start offset R that re-reads at most `reread_bound_bytes` (B) behind
        // the checkpoint's furthest justified position M.
        let reread_bound_bytes = self.topology.reread_bound_bytes;
        let state::ResolvedCheckpoint {
            offset,
            producers,
            gaps,
            max_offset,
        } = state::resolve_checkpoint(checkpoint, reread_bound_bytes);

        // Restart-resolution metrics: bytes conservatively re-read (M - R) and
        // the split of uncommitted producers recovered normally vs gapped.
        self.metrics
            .restart_reread_bytes
            .increment((max_offset - offset).max(0) as u64);
        let normal_uncommitted = producers
            .iter()
            .filter(|(producer, ps)| ps.offset >= 0 && !gaps.contains_key(*producer))
            .count();
        self.metrics
            .restart_normal_producers
            .increment(normal_uncommitted as u64);
        self.metrics
            .restart_gapped_producers
            .increment(gaps.len() as u64);

        // Emit a gap-creation event per gapped producer (F, M, R, B).
        for (producer, gap) in gaps.iter() {
            service_kit::event!(
                tracing::Level::INFO,
                "gap",
                session = self.topology.session_id,
                read_id,
                binding = binding.index,
                journal = journal.to_string(),
                producer = service_kit::event::debug(*producer),
                gap_begin = gap.gap_begin(), // F
                max_offset,                  // M
                start_offset = offset,       // R
                reread_bound_bytes,          // B
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
        self.refresh_gapped_gauge();

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
        // Historical backfill reads live in `pending_backfills`, exempt from the
        // tailing gate and stall accounting (spec §Ordering and scheduling).
        if read.id() & BACKFILL_ID_BIT != 0 {
            if let Some(result) = read.next().now_or_never() {
                return self.process_backfill_result(result, read);
            }
            self.pending_backfills.push(read.into_future());
            return Ok(());
        }

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

    /// Recompute and publish the unresolved-gapped-producers gauge from live
    /// read state. Called after any gap is created, resolved, or released. The
    /// gauge outlives the session (keyed by shard_id), so the actor also zeroes
    /// it on exit.
    fn refresh_gapped_gauge(&self) {
        let total: usize = self.reads.iter().map(|r| r.gaps.len()).sum();
        self.metrics.gapped_producers.set(total as f64);
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
        mut read: super::ReadLines,
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
            self.release_read_gaps(read.id() as usize, "read stopped (EOF); releasing gap");
            return Ok(());
        };

        let mut lines_batch = match result {
            Err(gazette::RetryError {
                attempt,
                inner: err,
            }) => match err {
                gazette::Error::BrokerStatus(broker::Status::JournalNotFound) => {
                    service_kit::event!(
                        tracing::Level::INFO,
                        "read",
                        read_id = read.id(),
                        binding = binding.index,
                        journal,
                        "stopped journal read (JOURNAL_NOT_FOUND)",
                    );
                    self.metrics.reads_stopped.increment(1);
                    self.release_read_gaps(
                        read.id() as usize,
                        "read stopped (JOURNAL_NOT_FOUND); releasing gap",
                    );
                    return Ok(());
                }
                gazette::Error::BrokerStatus(broker::Status::Suspended) => {
                    service_kit::event!(
                        tracing::Level::INFO,
                        "read",
                        read_id = read.id(),
                        binding = binding.index,
                        journal,
                        "stopped journal read (SUSPENDED)",
                    );
                    self.metrics.reads_stopped.increment(1);
                    self.release_read_gaps(
                        read.id() as usize,
                        "read stopped (SUSPENDED); releasing gap",
                    );
                    return Ok(());
                }
                err if err.is_transient() => {
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
                err => {
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

        let transcoded = match simd_doc::transcode_many(
            &mut self.parser,
            &mut lines_batch.content,
            &mut lines_batch.offset,
            Default::default(),
        ) {
            Err((err, location)) => {
                return Err(map_read_error(
                    gazette::Error::Parsing { err, location },
                    &read_state.journal,
                    binding.state_key(),
                    "transcoding documents",
                ));
            }
            Ok(transcoded) => transcoded,
        };

        // There may be a remainder if we failed to parse partway through.
        // Put it back to handle it next time.
        if !lines_batch.content.is_empty() {
            read.as_mut().put_back(lines_batch.content.into());
        }

        let metas = super::read::extract_metas(
            &transcoded,
            &binding.source_uuid_ptr,
            &mut self.validators[read_state.binding_index as usize],
            &read_state.journal,
        )?;

        // Consume into owned documents and pair with pre-extracted metadata.
        let mut doc_tail = transcoded.into_iter();
        let mut meta_tail = metas.into_iter();

        let (doc, _) = doc_tail.next().expect("non-empty transcoded");
        let meta = meta_tail.next().expect("non-empty metas");

        let ready_read = ReadyRead {
            doc,
            meta,
            doc_tail,
            meta_tail,
            inner: read,
        };

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

            // Defer draining if any read could still resolve to content that
            // preempts the current heap top: a parked non-tailing (stalled) read,
            // or a newly-started read still probing its write head (parked in
            // `pending_probes`, not yet classified as tailing/stalled). Parked
            // main reads (`parked_mains`) and historical backfill reads
            // (`pending_backfills`) are deliberately outside `pending_reads`, so
            // they neither contribute to nor are blocked by this gate.
            if self.heap_drain_blocked() {
                return Ok(idle);
            }

            // Do we have a document ready for append?
            let Some(ReadyReadEntry {
                adjusted_clock,
                priority,
                inner: ready_read,
            }) = self.ready_read_heap.peek()
            else {
                return Ok(idle);
            };
            let adjusted_clock = *adjusted_clock;
            let priority = *priority;
            let ready_read = ready_read.as_deref().unwrap();
            let raw_id = ready_read.inner.id();
            let meta = ready_read.meta; // `Meta` is Copy.

            // Gate on the adjusted clock: sleep until wall-clock time catches up.
            // Applies uniformly to main and historical backfill documents —
            // implementations must not assume historical clocks are already past.
            if let Some(wait) = state::clock_delay(&adjusted_clock, now, crate::now_clock) {
                return Ok(future::Either::Right(future::Either::Left(
                    tokio::time::sleep(wait).map(|()| true),
                )));
            }

            // Route historical backfill documents through their own path (spec
            // §Backfill transaction). They index `self.reads` by the masked id.
            if raw_id & BACKFILL_ID_BIT != 0 {
                if let Some(tx) =
                    self.drain_backfill_entry(buffers, raw_id & !BACKFILL_ID_BIT, priority, &meta)?
                {
                    return Ok(future::Either::Left(tx.reserve_owned().map(ok)));
                }
                continue;
            }
            let read_id = raw_id as usize;

            // A gapped producer's main-read documents are classified against its
            // frozen recovered state, not the speculative `uuid::sequence` (whose
            // `max_continue == 0` misreports rollbacks). See spec §Main-read
            // outcomes while gapped.
            if self.reads[read_id].gaps.contains_key(&meta.producer) {
                let gap_last_commit = self.reads[read_id]
                    .settled
                    .get(&meta.producer)
                    .map(|ps| ps.last_commit)
                    .unwrap_or_default();

                match state::sequence_gapped(gap_last_commit, &meta) {
                    state::GappedOutcome::Suppress | state::GappedOutcome::Drop => {
                        // Advance the main read past the document without any
                        // producer-state mutation (a suppressed ContinueBeginSpan
                        // must not overwrite the pinned F) and without append.
                        let ready_read = self.ready_read_heap.pop().unwrap().inner.unwrap();
                        self.reads[read_id].read_offset = meta.end_offset;
                        let read_delay = self.topology.bindings
                            [self.reads[read_id].binding_index as usize]
                            .read_delay;
                        self.continue_ready_read(ready_read, priority, read_delay)?;
                        continue;
                    }
                    outcome @ (state::GappedOutcome::CleanRollback
                    | state::GappedOutcome::DeepRollback) => {
                        let deep = matches!(outcome, state::GappedOutcome::DeepRollback);
                        self.resolve_gapped_rollback(read_id, priority, &meta, deep)?;
                        continue;
                    }
                    state::GappedOutcome::TriggerBackfill => {
                        self.trigger_backfill(read_id as u32, &meta)?;
                        continue;
                    }
                    state::GappedOutcome::OutsideResolve => {
                        // Discard the gap, then fall through to process the
                        // OUTSIDE commit through the normal path in this same
                        // iteration (its frozen `max_continue == 0` lets
                        // `OutsideCommit` proceed).
                        self.reads[read_id].gaps.remove(&meta.producer);
                        self.refresh_gapped_gauge();
                        let read_state = &self.reads[read_id];
                        service_kit::event!(
                            tracing::Level::INFO,
                            "gap",
                            session = self.topology.session_id,
                            read_id,
                            binding = read_state.binding_index,
                            journal = read_state.journal.to_string(),
                            producer = service_kit::event::debug(meta.producer),
                            clock = service_kit::event::debug(meta.clock),
                            "resolved gap via newer OUTSIDE commit (no historical I/O)",
                        );
                    }
                }
            }

            let read_state = &mut self.reads[read_id];
            let binding = &self.topology.bindings[read_state.binding_index as usize];
            let ready_read = self
                .ready_read_heap
                .peek()
                .unwrap()
                .inner
                .as_deref()
                .unwrap();

            let sequenced = state::sequence_document(read_state, binding, &meta)?;

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

    /// Whether the ready heap must not drain yet: some pending read could still
    /// resolve to content that preempts the current heap top. Parked main reads
    /// and historical backfill reads are outside `pending_reads`, so they do not
    /// participate (spec §Ordering and scheduling).
    fn heap_drain_blocked(&self) -> bool {
        self.tailing_reads != self.pending_reads.len() || !self.pending_probes.is_empty()
    }

    /// Advance a popped `ReadyRead` to its next buffered document: re-push it
    /// onto the ready heap, or (when its batch is drained) hand the inner read
    /// back to `park_or_process` to await more I/O. Shared by the main, gapped,
    /// and backfill drain paths. The just-consumed head document is dropped.
    fn continue_ready_read(
        &mut self,
        mut ready_read: Box<ReadyRead>,
        priority: u32,
        read_delay: uuid::Clock,
    ) -> anyhow::Result<()> {
        let ReadyRead {
            inner: read,
            doc: _consumed_doc,
            meta: _consumed_meta,
            mut doc_tail,
            mut meta_tail,
        } = *ready_read;

        match (doc_tail.next(), meta_tail.next()) {
            (Some((doc, _)), Some(meta)) => {
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
                });
                Ok(())
            }
            (None, None) => self.park_or_process(read),
            _ => unreachable!("doc_tail and meta_tail have equal length"),
        }
    }

    /// Resolve a gapped producer's ACK at or below `last_commit` as a durable
    /// rollback (clean when `deep == false`, deep otherwise). The rollback is
    /// made durable — a plain in-memory `AckDuplicate` would leave the positive
    /// gap offset `F` in the checkpoint forever — by emitting a committed
    /// `pending` entry `{last_commit: ack_clock, offset: -ack_end}`. No
    /// historical I/O is issued: the gap proves a pending span exists, and an
    /// ACK at or below `last_commit` rolls all of it back.
    fn resolve_gapped_rollback(
        &mut self,
        read_id: usize,
        priority: u32,
        meta: &Meta,
        deep: bool,
    ) -> anyhow::Result<()> {
        let ready_read = self.ready_read_heap.pop().unwrap().inner.unwrap();
        let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];

        if deep {
            tracing::warn!(
                binding=%binding.state_key(),
                clock=?meta.clock,
                journal=%self.reads[read_id].journal,
                producer=?meta.producer,
                "gapped producer deep-rolled back prior to its last committed clock (possible loss of exactly-once guarantees)",
            );
        }

        // ACKs are never appended. Extract causal hints from the ACK document.
        if meta.flags == uuid::Flags::ACK_TXN {
            state::extract_causal_hints(
                &self.topology.hint_index,
                &self.reads[read_id].journal,
                binding.cohort,
                self.reads[read_id].binding_index,
                meta.producer,
                meta.clock,
                ready_read.doc.get(),
                &mut self.causal_hints,
            )?;
        }
        let read_delay = binding.read_delay;
        let cohort_binding_index = self.reads[read_id].binding_index;

        // Durable committed entry: the negative offset wins by magnitude under
        // Frontier reduction even if a base retains a higher (monotonic)
        // last_commit — matching existing deep-rollback semantics.
        let read_state = &mut self.reads[read_id];
        read_state.read_offset = meta.end_offset;
        read_state.gaps.remove(&meta.producer);
        _ = read_state.pending.insert(
            meta.producer,
            super::producer::ProducerState {
                last_commit: meta.clock,
                max_continue: uuid::Clock::zero(),
                offset: -meta.end_offset,
            },
        );
        self.flush.set_ready();

        service_kit::event!(
            tracing::Level::INFO,
            "gap",
            session = self.topology.session_id,
            read_id,
            binding = cohort_binding_index,
            journal = self.reads[read_id].journal.to_string(),
            producer = service_kit::event::debug(meta.producer),
            clock = service_kit::event::debug(meta.clock),
            deep_rollback = deep,
            "resolved gap by rollback (no historical I/O)",
        );
        self.refresh_gapped_gauge();
        self.continue_ready_read(ready_read, priority, read_delay)
    }

    /// Park the main read at a gapped producer's trigger ACK and begin a
    /// historical backfill of `[F, ack.begin)` (spec §Trigger and parking).
    /// The speculative `AckEmpty` state is intentionally NOT applied; the ACK
    /// is sequenced only at completion, against the reconstructed span.
    fn trigger_backfill(&mut self, read_key: u32, meta: &Meta) -> anyhow::Result<()> {
        let read_id = read_key as usize;

        // Pop and stash the whole ReadyRead — its head is the trigger ACK and no
        // later document of the journal is reachable while parked. The main
        // read's logical offset stays at the ACK's begin.
        let ready = self.ready_read_heap.pop().unwrap().inner.unwrap();

        let gap_begin = self.reads[read_id]
            .gaps
            .get(&meta.producer)
            .expect("producer is gapped")
            .gap_begin();
        let recovered_last_commit = self.reads[read_id]
            .settled
            .get(&meta.producer)
            .map(|ps| ps.last_commit)
            .unwrap_or_default();

        // Transition Gapped → Backfilling. `live` reconstructs the pending span
        // from the recovered state; it never mirrors into pending/settled.
        _ = self.reads[read_id].gaps.insert(
            meta.producer,
            super::gap::GapState::Backfilling {
                gap_begin,
                live: super::producer::ProducerState {
                    last_commit: recovered_last_commit,
                    max_continue: uuid::Clock::zero(),
                    offset: gap_begin,
                },
                ack: *meta,
            },
        );
        _ = self.parked_mains.insert(
            read_key,
            ParkedMain {
                ready,
                target: meta.producer,
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
            ack_begin = meta.begin_offset,
            "triggering backfill of gapped producer's pending transaction",
        );
        self.metrics.backfills_started.increment(1);
        self.refresh_gapped_gauge();

        // Empty range (e.g. M == R == F, or a hint-only producer whose ACK
        // begins at F): no historical read; go straight to completion.
        if gap_begin == meta.begin_offset {
            return self.complete_backfill(read_key);
        }

        // Start the bounded, non-blocking historical read. Same client, auth,
        // begin_mod_time, schema validation, and partition-filtered journal as
        // the main read; no write-head probe is needed for a bounded range.
        let client = (*self.topology.journal_clients[binding.index as usize]).clone();
        let request = broker::ReadRequest {
            journal: format!(
                "{};{}",
                self.reads[read_id].journal, binding.journal_read_suffix
            ),
            begin_mod_time: binding.not_before.to_unix().0 as i64,
            block: false,
            do_not_proxy: true,
            end_offset: meta.begin_offset, // exclusive
            metadata_only: false,
            offset: gap_begin, // F
            min_etcd_revision: 0,
            header: None,
        };
        let read: super::ReadLines = Box::pin(gazette::journal::read::ReadLines::new(
            client.read(request).boxed(),
            read_key | BACKFILL_ID_BIT,
            false, // Never tailing: the range is bounded and historical.
        ));
        self.pending_backfills.push(read.into_future());

        Ok(())
    }

    /// Drain one historical backfill document from the ready heap. Returns
    /// `Some(tx)` when an Append channel lacked capacity (the caller wakes on
    /// it and retries), `None` otherwise. Backfill documents never touch the
    /// main read's offset baselines or `pending`/`settled` (spec §Read
    /// positions, §Completion).
    fn drain_backfill_entry(
        &mut self,
        buffers: &mut Buffers,
        read_key: u32,
        priority: u32,
        meta: &Meta,
    ) -> anyhow::Result<Option<mpsc::Sender<shuffle::LogRequest>>> {
        let read_id = read_key as usize;
        let target = self
            .parked_mains
            .get(&read_key)
            .expect("backfill has a parked main read")
            .target;

        let read_delay =
            self.topology.bindings[self.reads[read_id].binding_index as usize].read_delay;

        // Documents of other producers are already represented by checkpoint
        // state or belong to independent gaps: skip without sequencing, state
        // mutation, key extraction, or append.
        if meta.producer != target {
            let ready_read = self.ready_read_heap.pop().unwrap().inner.unwrap();
            self.continue_ready_read(ready_read, priority, read_delay)?;
            return Ok(None);
        }

        // Sequence the target producer's historical document against the gap's
        // `live` state (initialized from the recovered checkpoint).
        let live = match self.reads[read_id].gaps.get(&target) {
            Some(super::gap::GapState::Backfilling { live, .. }) => live.clone(),
            _ => unreachable!("target producer is backfilling"),
        };
        let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];
        let sequenced =
            state::sequence_producer(live, &self.reads[read_id].journal, binding, meta)?;

        // One-transaction invariant: the parked trigger ACK is the only
        // legitimate commit, sequenced at completion. Any commit inside
        // `[F, ack.begin)` contradicts the recovered checkpoint.
        if sequenced.is_commit {
            anyhow::bail!(
                "backfill of journal {} (binding {}) hit an unexpected committing document at \
                 offset {} for target producer {:?}: a distinct transaction boundary inside the \
                 historical range contradicts the recovered checkpoint",
                self.reads[read_id].journal,
                binding.state_key(),
                meta.begin_offset,
                target,
            );
        }

        if sequenced.is_append {
            let ready_read = self
                .ready_read_heap
                .peek()
                .unwrap()
                .inner
                .as_deref()
                .unwrap();
            if let Err(tx) = Self::try_log_request_append_tx(
                binding,
                buffers,
                &self.reads[read_id].journal,
                &self.topology.shards,
                &mut self.log_prev_journal,
                &self.log_request_tx,
                ready_read,
            ) {
                return Ok(Some(tx));
            }
        }

        // Commit `live` forward (never `pending`/`settled`) and continue.
        let ready_read = self.ready_read_heap.pop().unwrap().inner.unwrap();
        if let Some(super::gap::GapState::Backfilling { live, .. }) =
            self.reads[read_id].gaps.get_mut(&target)
        {
            *live = sequenced.producer_state;
        }
        self.continue_ready_read(ready_read, priority, read_delay)?;
        Ok(None)
    }

    /// Complete a backfill once its historical range has been fully read: the
    /// single atomic visibility boundary of the recovered transaction (spec
    /// §Completion). Sequences the shelved ACK against the reconstructed span,
    /// extracts causal hints, emits committed producer progress, flushes, and
    /// resumes the main read strictly after the ACK.
    fn complete_backfill(&mut self, read_key: u32) -> anyhow::Result<()> {
        let read_id = read_key as usize;
        let ParkedMain {
            ready,
            target,
            started_at,
            physical_bytes,
        } = self
            .parked_mains
            .remove(&read_key)
            .expect("completing backfill has a parked main read");

        let (gap_begin, live, ack) = match self.reads[read_id].gaps.remove(&target) {
            Some(super::gap::GapState::Backfilling {
                gap_begin,
                live,
                ack,
            }) => (gap_begin, live, ack),
            _ => unreachable!("completing backfill has a Backfilling gap"),
        };

        // AckEmpty (no reconstructed CONTINUEs) is only expected when historical
        // content was unavailable or filtered — a suspiciously short backfill.
        let ack_empty = live.max_continue == uuid::Clock::zero();

        let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];
        let sequenced =
            state::sequence_producer(live, &self.reads[read_id].journal, binding, &ack)?;

        if ack.flags == uuid::Flags::ACK_TXN {
            state::extract_causal_hints(
                &self.topology.hint_index,
                &self.reads[read_id].journal,
                binding.cohort,
                self.reads[read_id].binding_index,
                ack.producer,
                ack.clock,
                ready.doc.get(),
                &mut self.causal_hints,
            )?;
        }
        let read_delay = binding.read_delay;
        let priority = binding.priority;

        // The atomic visibility boundary: committed producer progress, causal
        // hints, and every backfilled append become durable together in the
        // next flush.
        _ = self.reads[read_id]
            .pending
            .insert(target, sequenced.producer_state);
        self.reads[read_id].read_offset = ack.end_offset;
        self.flush.set_ready();

        let elapsed = started_at.elapsed();
        self.metrics.backfills_completed.increment(1);
        self.metrics
            .backfill_duration_seconds
            .record(elapsed.as_secs_f64());
        self.metrics
            .backfill_range_bytes
            .record((ack.begin_offset - gap_begin) as f64);

        service_kit::event!(
            tracing::Level::INFO,
            "backfill",
            session = self.topology.session_id,
            read_id,
            binding = self.reads[read_id].binding_index,
            journal = self.reads[read_id].journal.to_string(),
            producer = service_kit::event::debug(target),
            range_bytes = ack.begin_offset - gap_begin,
            physical_bytes,
            duration_ms = elapsed.as_millis() as u64,
            ack_empty, // true flags a suspiciously short backfill (fragment loss)
            "completed backfill of gapped producer's transaction",
        );
        self.refresh_gapped_gauge();

        // Resume the main read strictly after the ACK.
        self.continue_ready_read(ready, priority, read_delay)
    }

    /// Process a historical backfill read's resolution. Mirrors
    /// `process_read_result` but never touches main-read offset baselines or
    /// `write_head`, counts physical bytes into the backfill counter, and on
    /// stream end completes the backfill.
    fn process_backfill_result(
        &mut self,
        result: Option<gazette::RetryResult<gazette::journal::read::LinesBatch>>,
        mut read: super::ReadLines,
    ) -> anyhow::Result<()> {
        let read_key = read.id() & !BACKFILL_ID_BIT;
        let read_id = read_key as usize;
        let binding = &self.topology.bindings[self.reads[read_id].binding_index as usize];
        let journal = read.fragment().journal.clone();

        let Some(result) = result else {
            // The bounded stream reached `end_offset`: every prior batch was
            // fully drained from the heap, so all historical Appends precede the
            // final ACK flush on each Log channel. Complete the backfill.
            return self.complete_backfill(read_key);
        };

        let mut lines_batch = match result {
            Err(gazette::RetryError {
                attempt,
                inner: err,
            }) => match err {
                gazette::Error::BrokerStatus(
                    broker::Status::JournalNotFound | broker::Status::Suspended,
                ) => {
                    // Deletion or FULL suspension implies no fragments remain:
                    // release the read's gap and parked main (dropping `read`
                    // cancels the historical stream).
                    service_kit::event!(
                        tracing::Level::INFO,
                        "backfill",
                        read_id,
                        binding = binding.index,
                        journal,
                        "backfill journal removed (JOURNAL_NOT_FOUND/SUSPENDED); releasing gap",
                    );
                    self.metrics.backfills_failed.increment(1);
                    self.release_read_gaps(read_id, "journal removed during backfill");
                    return Ok(());
                }
                err if err.is_transient() => {
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
                    return self.park_or_process(read);
                }
                err => {
                    self.metrics.backfills_failed.increment(1);
                    return Err(map_read_error(
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
        // Do NOT update main-read `write_head` or offset baselines.
        let n = lines_batch.content.len() as u64;
        self.metrics.backfill_bytes_read.increment(n);
        if let Some(pm) = self.parked_mains.get_mut(&read_key) {
            pm.physical_bytes += n;
        }

        let transcoded = match simd_doc::transcode_many(
            &mut self.parser,
            &mut lines_batch.content,
            &mut lines_batch.offset,
            Default::default(),
        ) {
            Err((err, location)) => {
                self.metrics.backfills_failed.increment(1);
                return Err(map_read_error(
                    gazette::Error::Parsing { err, location },
                    &self.reads[read_id].journal,
                    binding.state_key(),
                    "transcoding backfill documents",
                ));
            }
            Ok(transcoded) => transcoded,
        };

        if !lines_batch.content.is_empty() {
            read.as_mut().put_back(lines_batch.content.into());
        }

        let metas = super::read::extract_metas(
            &transcoded,
            &binding.source_uuid_ptr,
            &mut self.validators[self.reads[read_id].binding_index as usize],
            &self.reads[read_id].journal,
        )?;

        let mut doc_tail = transcoded.into_iter();
        let mut meta_tail = metas.into_iter();
        let (doc, _) = doc_tail.next().expect("non-empty transcoded");
        let meta = meta_tail.next().expect("non-empty metas");

        let ready_read = ReadyRead {
            doc,
            meta,
            doc_tail,
            meta_tail,
            inner: read,
        };
        self.ready_read_heap.push(ReadyReadEntry {
            priority: binding.priority,
            adjusted_clock: ready_read.meta.clock + binding.read_delay,
            inner: Some(Box::new(ready_read)),
        });

        Ok(())
    }

    /// Release a read's gapped and backfilling state (its journal was removed,
    /// or the session is ending). Drops any parked main read and emits a
    /// release event per gap. Any in-flight historical read is cancelled by the
    /// caller dropping its stream.
    fn release_read_gaps(&mut self, read_id: usize, reason: &'static str) {
        let read_key = read_id as u32;
        let had_parked = self.parked_mains.remove(&read_key).is_some();

        if self.reads[read_id].gaps.is_empty() && !had_parked {
            return;
        }

        let session_id = self.topology.session_id;
        let read_state = &self.reads[read_id];
        for (producer, gap) in read_state.gaps.iter() {
            service_kit::event!(
                tracing::Level::INFO,
                "gap",
                session = session_id,
                read_id,
                binding = read_state.binding_index,
                journal = read_state.journal.to_string(),
                producer = service_kit::event::debug(*producer),
                gap_begin = gap.gap_begin(),
                "{}",
                reason,
            );
        }
        self.reads[read_id].gaps.clear();
        self.refresh_gapped_gauge();
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
    fn try_log_request_append_tx(
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
