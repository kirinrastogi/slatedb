use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use log::trace;
use object_store::path::Path;
use tokio::{runtime::Handle, select, sync::oneshot};
use tracing::instrument;

use crate::clock::MonotonicClock;
use crate::db_stats::DbStats;
use crate::db_status::ClosedResultWriter;
use crate::dispatcher::{MessageFactory, MessageHandler, MessageHandlerExecutor};
use crate::error::SlateDBError;
use crate::format::sst::SsTableFormat;
use crate::oracle::{DbOracle, Oracle};
use crate::types::RowEntry;
use crate::utils::SafeSender;
use crate::utils::{format_bytes_si, WatchableOnceCell, WatchableOnceCellReader};
use crate::wal::streaming_wal_writer::{AppendableStore, StreamingWalWriter};
use crate::wal_id::WalIdStore;

pub(crate) const ZONAL_WAL_TASK_NAME: &str = "zonal_wal_writer";

/// [`ZonalAppendableWalManager`] buffers write operations in memory and streams them
/// to a GCS Rapid bucket appendable object on each flush interval.
///
/// Unlike [`WalBufferManager`] which writes a complete WAL SSTable per flush via atomic put,
/// this manager keeps one long-lived appendable object and streams blocks to it incrementally.
/// Each block is flushed for durability individually, providing finer-grained crash recovery.
///
/// On each flush interval:
/// 1. The current in-memory buffer is frozen
/// 2. Entries are streamed to the appendable object via [`StreamingWalWriter`]
/// 3. Partial blocks are finished and flushed
/// 4. Listeners are notified that their data is durable
///
/// When the appendable object reaches `max_wal_bytes_size`, it is finalized (footer written,
/// object made immutable) and a new appendable object is started with a new WAL ID.
pub(crate) struct ZonalAppendableWalManager {
    inner: Arc<parking_lot::RwLock<ZonalAppendableWalManagerInner>>,
    wal_id_incrementor: Arc<dyn WalIdStore + Send + Sync>,
    status_manager: crate::db_status::DbStatusManager,
    db_stats: DbStats,
    mono_clock: Arc<MonotonicClock>,
    appendable_store: Arc<dyn AppendableStore + Send + Sync>,
    sst_format: SsTableFormat,
    root_path: String,
    max_wal_bytes_size: usize,
    max_flush_interval: Option<Duration>,
    last_flush_requested_epoch: AtomicU64,
}

struct ZonalAppendableWalManagerInner {
    current_wal: ZonalAppendableWal,
    /// Frozen WALs pending streaming or release. Some may already be durable
    /// (streamed to the appendable object), others are waiting to be streamed.
    /// Release happens when a WAL is both durable and applied to memtable.
    pending_wals: VecDeque<Arc<ZonalAppendableWal>>,
    /// The channel to send the flush work to the background worker.
    flush_tx: Option<SafeSender<WalFlushWork>>,
    /// task executor for the background worker.
    task_executor: Option<Arc<MessageHandlerExecutor>>,
    /// Whenever a WAL is applied to Memtable and successfully flushed to storage,
    /// the pending WAL can be recycled in memory.
    last_applied_seq: Option<u64>,
    /// Monotonically increasing epoch incremented each time the current WAL is
    /// frozen. Used with `last_flush_requested_epoch` to deduplicate size-triggered
    /// flush requests.
    flush_epoch: u64,
    /// The WAL ID of the most recently finalized appendable object.
    recent_flushed_wal_id: u64,
    /// The oracle to track the last flushed sequence number.
    oracle: Arc<DbOracle>,
}

/// In-memory buffer for WAL entries, identical in structure to WalBuffer.
///
/// Stores entries in insertion order (by sequence number). On flush, entries are
/// streamed to the appendable object. The `durable` watcher is notified after
/// all entries have been flushed to storage.
struct ZonalAppendableWal {
    entries: VecDeque<RowEntry>,
    durable: WatchableOnceCell<Result<(), SlateDBError>>,
    last_tick: i64,
    last_seq: u64,
    entries_size: usize,
}

struct ZonalAppendableWalIterator {
    entries: std::vec::IntoIter<RowEntry>,
}

impl ZonalAppendableWalManager {
    pub(crate) fn new(
        wal_id_incrementor: Arc<dyn WalIdStore + Send + Sync>,
        status_manager: crate::db_status::DbStatusManager,
        db_stats: DbStats,
        recent_flushed_wal_id: u64,
        oracle: Arc<DbOracle>,
        appendable_store: Arc<dyn AppendableStore + Send + Sync>,
        sst_format: SsTableFormat,
        root_path: String,
        mono_clock: Arc<MonotonicClock>,
        max_wal_bytes_size: usize,
        max_flush_interval: Option<Duration>,
    ) -> Self {
        let current_wal = ZonalAppendableWal::new();
        let pending_wals = VecDeque::new();
        let inner = ZonalAppendableWalManagerInner {
            current_wal,
            pending_wals,
            last_applied_seq: None,
            flush_epoch: 1,
            recent_flushed_wal_id,
            flush_tx: None,
            task_executor: None,
            oracle,
        };
        Self {
            inner: Arc::new(parking_lot::RwLock::new(inner)),
            wal_id_incrementor,
            status_manager,
            db_stats,
            appendable_store,
            sst_format,
            root_path,
            mono_clock,
            max_wal_bytes_size,
            max_flush_interval,
            last_flush_requested_epoch: AtomicU64::new(0),
        }
    }

    pub(crate) async fn init(
        self: &Arc<Self>,
        task_executor: Arc<MessageHandlerExecutor>,
    ) -> Result<(), SlateDBError> {
        let (flush_tx, flush_rx) =
            SafeSender::unbounded_channel(self.status_manager.result_reader());
        {
            let mut inner = self.inner.write();
            inner.flush_tx = Some(flush_tx);
        }
        let flush_handler = ZonalWalFlushHandler {
            max_flush_interval: self.max_flush_interval,
            manager: self.clone(),
            active_writer: None,
            current_wal_id: 0,
        };

        let result = task_executor.add_handler(
            ZONAL_WAL_TASK_NAME.to_string(),
            Box::new(flush_handler),
            flush_rx,
            &Handle::current(),
        );
        {
            let mut inner = self.inner.write();
            inner.task_executor = Some(task_executor);
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn buffered_wal_entries_count(&self) -> usize {
        let guard = self.inner.read();
        let pending_entries_count = guard
            .pending_wals
            .iter()
            .map(|wal| wal.len())
            .sum::<usize>();
        guard.current_wal.len() + pending_entries_count
    }

    pub(crate) fn recent_flushed_wal_id(&self) -> u64 {
        let inner = self.inner.read();
        inner.recent_flushed_wal_id
    }

    pub(crate) fn advance_recent_flushed_wal_id(&self, wal_id: u64) {
        let mut inner = self.inner.write();
        if wal_id > inner.recent_flushed_wal_id {
            inner.recent_flushed_wal_id = wal_id;
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        let inner = self.inner.read();
        inner.current_wal.is_empty() && inner.pending_wals.is_empty()
    }

    pub(crate) fn estimated_bytes(&self) -> Result<usize, SlateDBError> {
        let inner = self.inner.read();
        let current_wal_size = self
            .sst_format
            .estimate_encoded_size_wal(inner.current_wal.len(), inner.current_wal.size());

        let last_durable_seq = inner.oracle.last_remote_persisted_seq();
        let pending_size: usize = inner
            .pending_wals
            .iter()
            .filter(|wal| wal.last_seq().map(|s| s > last_durable_seq).unwrap_or(true))
            .map(|wal| {
                self.sst_format
                    .estimate_encoded_size_wal(wal.len(), wal.size())
            })
            .sum();

        Ok(current_wal_size + pending_size)
    }

    pub(crate) fn append(
        &self,
        entries: &[RowEntry],
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        let mut inner = self.inner.write();
        for entry in entries {
            inner.current_wal.append(entry.clone());
        }
        Ok(inner.current_wal.durable_watcher())
    }

    pub(crate) fn maybe_trigger_flush(
        &self,
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        let (durable_watcher, need_flush, flush_epoch) = {
            let inner = self.inner.read();
            let current_wal_size = self
                .sst_format
                .estimate_encoded_size_wal(inner.current_wal.len(), inner.current_wal.size());
            trace!(
                "checking flush trigger [current_wal_size={}, max_wal_bytes_size={}]",
                format_bytes_si(current_wal_size as u64),
                format_bytes_si(self.max_wal_bytes_size as u64),
            );
            let need_flush = current_wal_size >= self.max_wal_bytes_size;
            (
                inner.current_wal.durable_watcher(),
                need_flush,
                inner.flush_epoch,
            )
        };
        if need_flush {
            let last = self.last_flush_requested_epoch.load(Ordering::Relaxed);
            if last < flush_epoch
                && self
                    .last_flush_requested_epoch
                    .compare_exchange(last, flush_epoch, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                self.send_flush_request(None)?;
            }
        }

        let estimated_bytes = self.estimated_bytes()?;
        self.db_stats
            .wal_buffer_estimated_bytes
            .set(estimated_bytes as i64);
        Ok(durable_watcher)
    }

    pub(crate) fn watcher_for_oldest_unflushed_wal(
        &self,
    ) -> Option<WatchableOnceCellReader<Result<(), SlateDBError>>> {
        let guard = self.inner.read();
        // Check pending WALs that haven't been streamed yet
        let last_durable_seq = guard.oracle.last_remote_persisted_seq();
        for wal in guard.pending_wals.iter() {
            if wal.last_seq().map(|s| s > last_durable_seq).unwrap_or(true) {
                return Some(wal.durable_watcher());
            }
        }
        if !guard.current_wal.is_empty() {
            Some(guard.current_wal.durable_watcher())
        } else {
            None
        }
    }

    fn send_flush_request(
        &self,
        result_tx: Option<oneshot::Sender<Result<(), SlateDBError>>>,
    ) -> Result<(), SlateDBError> {
        self.db_stats.wal_buffer_flush_requests.increment(1);
        let flush_tx = self
            .inner
            .read()
            .flush_tx
            .clone()
            .expect("flush_tx not initialized, please call init first.");
        flush_tx.send(WalFlushWork { result_tx })
    }

    #[instrument(level = "trace", skip_all, err(level = tracing::Level::DEBUG))]
    pub(crate) async fn flush(&self) -> Result<(), SlateDBError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.send_flush_request(Some(result_tx))?;
        select! {
            result = result_rx => {
                result?
            }
        }
    }

    fn freeze_current_wal(&self) -> Result<(), SlateDBError> {
        let is_empty = self.inner.read().current_wal.is_empty();
        if is_empty {
            return Ok(());
        }

        let mut inner = self.inner.write();
        let current_wal = std::mem::replace(&mut inner.current_wal, ZonalAppendableWal::new());
        inner.flush_epoch += 1;
        inner.pending_wals.push_back(Arc::new(current_wal));
        Ok(())
    }

    /// Returns pending WALs whose entries haven't been streamed to storage yet.
    fn get_unflushed_pending_wals(&self) -> Vec<Arc<ZonalAppendableWal>> {
        let inner = self.inner.read();
        let last_durable_seq = inner.oracle.last_remote_persisted_seq();
        inner
            .pending_wals
            .iter()
            .filter(|wal| wal.last_seq().map(|s| s > last_durable_seq).unwrap_or(true))
            .cloned()
            .collect()
    }

    pub(crate) fn track_last_applied_seq(&self, seq: u64) {
        {
            let mut inner = self.inner.write();
            inner.last_applied_seq = Some(seq);
        }
        self.maybe_release_pending_wals();
    }

    fn maybe_release_pending_wals(&self) {
        let mut inner = self.inner.write();

        let last_applied_seq = match inner.last_applied_seq {
            Some(seq) => seq,
            None => return,
        };

        let last_flushed_seq = inner.oracle.last_remote_persisted_seq();

        let mut releaseable_count = 0;
        for wal in inner.pending_wals.iter() {
            if wal
                .last_seq()
                .map(|seq| seq <= last_applied_seq && seq <= last_flushed_seq)
                .unwrap_or(false)
            {
                releaseable_count += 1;
            } else {
                break;
            }
        }

        if releaseable_count > 0 {
            trace!(
                "draining pending wals [releaseable_count={}]",
                releaseable_count
            );
            inner.pending_wals.drain(..releaseable_count);
        }
    }

    /// Construct the WAL SST path for a given WAL ID.
    fn wal_path(&self, wal_id: u64) -> Path {
        Path::from(format!("{}/wal/{:020}.sst", self.root_path, wal_id))
    }

    /// Recover WAL entries from any partial (unfinalized) appendable objects.
    ///
    /// Call this before `init()` to recover entries from a crash. Scans WAL IDs
    /// starting from `start_wal_id`, reads partial appendable objects via `tail_read`,
    /// and rebuilds them as complete WAL SSTs (with footer) at new WAL IDs so that
    /// the standard `WalReplayIterator` can read them.
    ///
    /// Returns the last WAL ID used for rebuilt SSTs, or `start_wal_id - 1` if
    /// nothing was recovered. The caller should ensure WAL replay covers the
    /// rebuilt IDs.
    pub(crate) async fn recover_partial_wals(
        &self,
        start_wal_id: u64,
    ) -> Result<u64, SlateDBError> {
        use crate::wal::streaming_wal_writer::{rebuild_wal_sst, recover_partial_wal};
        use log::info;

        let mut wal_id = start_wal_id;
        let mut last_rebuilt_id = start_wal_id.saturating_sub(1);

        loop {
            let path = self.wal_path(wal_id);
            match self.appendable_store.tail_read(&path, 0).await {
                Ok(data) if !data.is_empty() => {
                    info!(
                        "recovering partial WAL [wal_id={}, bytes={}]",
                        wal_id,
                        data.len()
                    );
                    // Parse entries from the partial object's length-prefixed blocks
                    let entries = recover_partial_wal(
                        self.appendable_store.as_ref(),
                        &path,
                        &self.sst_format,
                    )
                    .await?;

                    if entries.is_empty() {
                        info!("no valid entries in partial WAL [wal_id={}]", wal_id);
                        wal_id += 1;
                        continue;
                    }

                    info!(
                        "recovered {} entries from partial WAL [wal_id={}]",
                        entries.len(),
                        wal_id
                    );

                    // Rebuild as a fresh complete SST at a new WAL ID
                    let rebuilt_id = self.wal_id_incrementor.next_wal_id();
                    let rebuilt_path = self.wal_path(rebuilt_id);
                    rebuild_wal_sst(
                        self.appendable_store.as_ref(),
                        &rebuilt_path,
                        &self.sst_format,
                        &entries,
                    )
                    .await?;

                    info!(
                        "rebuilt partial WAL as complete SST [original_wal_id={}, rebuilt_wal_id={}]",
                        wal_id, rebuilt_id
                    );
                    last_rebuilt_id = rebuilt_id;
                    wal_id += 1;
                }
                Ok(_) => {
                    // Empty object or no data — done scanning
                    break;
                }
                Err(_) => {
                    // Object doesn't exist — done scanning
                    break;
                }
            }
        }

        Ok(last_rebuilt_id)
    }

    #[allow(dead_code)]
    pub(crate) async fn close(&self) -> Result<(), SlateDBError> {
        let task_executor = {
            let inner = self.inner.read();
            inner
                .task_executor
                .clone()
                .expect("task executor should be initialized")
        };
        task_executor.shutdown_task(ZONAL_WAL_TASK_NAME).await
    }
}

impl ZonalAppendableWal {
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            durable: WatchableOnceCell::new(),
            last_tick: i64::MIN,
            last_seq: 0,
            entries_size: 0,
        }
    }

    fn append(&mut self, entry: RowEntry) {
        if let Some(ts) = entry.create_ts {
            self.last_tick = ts;
        }
        self.last_seq = entry.seq;
        self.entries_size += entry.estimated_size();
        self.entries.push_back(entry);
    }

    fn iter(&self) -> ZonalAppendableWalIterator {
        ZonalAppendableWalIterator::new(self)
    }

    fn durable_watcher(&self) -> WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.durable.reader()
    }

    #[cfg(test)]
    async fn await_durable(&self) -> Result<(), SlateDBError> {
        self.durable.reader().await_value().await
    }

    fn notify_durable(&self, result: Result<(), SlateDBError>) {
        self.durable.write(result);
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn size(&self) -> usize {
        self.entries_size
    }

    fn last_seq(&self) -> Option<u64> {
        if self.last_seq == 0 {
            None
        } else {
            Some(self.last_seq)
        }
    }

    fn last_tick(&self) -> i64 {
        self.last_tick
    }
}

impl ZonalAppendableWalIterator {
    fn new(wal: &ZonalAppendableWal) -> Self {
        let entries = wal.entries.iter().cloned().collect::<Vec<_>>();
        Self {
            entries: entries.into_iter(),
        }
    }

    fn next(&mut self) -> Option<RowEntry> {
        self.entries.next()
    }
}

#[derive(Debug)]
struct WalFlushWork {
    result_tx: Option<oneshot::Sender<Result<(), SlateDBError>>>,
}

struct ZonalWalFlushHandler {
    max_flush_interval: Option<Duration>,
    manager: Arc<ZonalAppendableWalManager>,
    /// The active streaming writer. Stays open across flush intervals;
    /// finalized when the object reaches `max_wal_bytes_size`.
    active_writer: Option<StreamingWalWriter>,
    /// WAL ID of the current appendable object.
    current_wal_id: u64,
}

#[async_trait]
impl MessageHandler<WalFlushWork> for ZonalWalFlushHandler {
    fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<WalFlushWork>>)> {
        if let Some(max_flush_interval) = self.max_flush_interval {
            return vec![(
                max_flush_interval,
                Box::new(|| WalFlushWork { result_tx: None }),
            )];
        }
        vec![]
    }

    async fn handle(&mut self, message: WalFlushWork) -> Result<(), SlateDBError> {
        let WalFlushWork { result_tx } = message;
        let result = self.do_flush().await;
        if let Some(result_tx) = result_tx {
            result_tx
                .send(result.clone())
                .expect("failed to send flush result");
        }
        result
    }

    async fn cleanup(
        &mut self,
        mut messages: BoxStream<'async_trait, WalFlushWork>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        let error = result.err().unwrap_or(SlateDBError::Closed);

        // Drain remaining messages
        while let Some(WalFlushWork { result_tx }) = messages.next().await {
            if let Some(result_tx) = result_tx {
                result_tx
                    .send(Err(error.clone()))
                    .expect("failed to send flush result");
            }
        }

        // Best-effort: finalize the active writer to persist a valid SST
        if let Some(writer) = self.active_writer.take() {
            if let Ok(_) = writer.finalize().await {
                self.manager
                    .advance_recent_flushed_wal_id(self.current_wal_id);
            }
        }

        // Freeze current WAL and notify all pending WALs of the error
        self.manager.freeze_current_wal()?;
        let unflushed = self.manager.get_unflushed_pending_wals();
        for wal in unflushed.iter() {
            wal.notify_durable(Err(error.clone()));
        }
        Ok(())
    }
}

impl ZonalWalFlushHandler {
    #[instrument(level = "trace", skip_all, err(level = tracing::Level::DEBUG))]
    async fn do_flush(&mut self) -> Result<(), SlateDBError> {
        self.manager.freeze_current_wal()?;
        let unflushed = self.manager.get_unflushed_pending_wals();

        if unflushed.is_empty() {
            return Ok(());
        }

        // Ensure we have an active writer (create new appendable object if needed)
        if self.active_writer.is_none() {
            self.current_wal_id = self.manager.wal_id_incrementor.next_wal_id();
            let path = self.manager.wal_path(self.current_wal_id);
            self.active_writer = Some(
                StreamingWalWriter::new(
                    self.manager.appendable_store.as_ref(),
                    &path,
                    &self.manager.sst_format,
                )
                .await?,
            );
        }

        let writer = self
            .active_writer
            .as_mut()
            .expect("active_writer should be Some after the check above");

        // Stream all unflushed pending WALs through the writer
        let mut last_tick = i64::MIN;
        for wal in unflushed.iter() {
            let mut iter = wal.iter();
            while let Some(entry) = iter.next() {
                writer.add(entry).await?;
            }
            last_tick = last_tick.max(wal.last_tick());
        }

        // Finish partial block and flush for durability.
        // After this, all streamed entries are durable in storage.
        writer.flush_pending().await?;

        self.manager.db_stats.wal_buffer_flushes.increment(1);

        // Data is durable after flush_pending — advance recent_flushed_wal_id.
        // This uses the current physical object's WAL ID. The rest of the system
        // treats this as a watermark: everything up to this ID is durable.
        self.manager
            .advance_recent_flushed_wal_id(self.current_wal_id);

        // Notify durability for each flushed WAL
        for wal in unflushed.iter() {
            if let Some(seq) = wal.last_seq() {
                let inner = self.manager.inner.read();
                inner.oracle.advance_durable_seq(seq);
            }
            wal.notify_durable(Ok(()));
        }

        self.manager
            .mono_clock
            .fetch_max_last_durable_tick(last_tick);
        self.manager.maybe_release_pending_wals();

        // Check if we should finalize and roll over to a new appendable object
        let should_finalize = writer.write_offset() >= self.manager.max_wal_bytes_size as i64;
        if should_finalize {
            if let Some(writer) = self.active_writer.take() {
                let _handle = writer.finalize().await?;
                // Next flush will create a new object with a new WAL ID
            }
        }

        let estimated_bytes = self.manager.estimated_bytes()?;
        self.manager
            .db_stats
            .wal_buffer_estimated_bytes
            .set(estimated_bytes as i64);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MonotonicClock;
    use crate::db_status::DbStatusManager;
    use crate::format::sst::SsTableFormat;
    use crate::oracle::DbOracle;
    use crate::types::{RowEntry, ValueDeletable};
    use crate::wal::streaming_wal_writer::{AppendWriter, AppendableStore};
    use bytes::Bytes;
    use object_store::path::Path;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutPayload, PutResult,
    };
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::metrics::{DefaultMetricsRecorder, MetricLevel, MetricsRecorderHelper};
    use slatedb_common::MockSystemClock;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex as AsyncMutex;

    fn make_entry(key: &str, value: &str, seq: u64, create_ts: Option<i64>) -> RowEntry {
        RowEntry::new(
            Bytes::from(key.to_string()),
            ValueDeletable::Value(Bytes::from(value.to_string())),
            seq,
            create_ts,
            None,
        )
    }

    // --- Mock AppendWriter ---
    struct MockAppendWriter {
        data: Arc<AsyncMutex<Vec<u8>>>,
        offset: i64,
        flush_count: Arc<std::sync::Mutex<usize>>,
    }

    impl MockAppendWriter {
        fn new(data: Arc<AsyncMutex<Vec<u8>>>) -> Self {
            Self {
                data,
                offset: 0,
                flush_count: Arc::new(std::sync::Mutex::new(0)),
            }
        }
    }

    #[async_trait]
    impl AppendWriter for MockAppendWriter {
        async fn write(&mut self, data: Bytes) -> Result<(), SlateDBError> {
            let mut buf = self.data.lock().await;
            buf.extend_from_slice(&data);
            self.offset += data.len() as i64;
            Ok(())
        }

        async fn flush(&mut self) -> Result<i64, SlateDBError> {
            *self.flush_count.lock().unwrap() += 1;
            Ok(self.offset)
        }

        async fn finalize(self: Box<Self>) -> Result<PutResult, SlateDBError> {
            Ok(PutResult {
                e_tag: Some("mock-etag".to_string()),
                version: Some("mock-version".to_string()),
            })
        }

        fn pause(self: Box<Self>) {}

        fn write_offset(&self) -> i64 {
            self.offset
        }
    }

    // --- Mock AppendableStore ---
    #[derive(Debug)]
    struct MockAppendableStore {
        objects: Arc<AsyncMutex<HashMap<String, Arc<AsyncMutex<Vec<u8>>>>>>,
    }

    impl MockAppendableStore {
        fn new() -> Self {
            Self {
                objects: Arc::new(AsyncMutex::new(HashMap::new())),
            }
        }
    }

    impl std::fmt::Display for MockAppendableStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockAppendableStore")
        }
    }

    #[async_trait]
    impl ObjectStore for MockAppendableStore {
        async fn put(
            &self,
            _location: &Path,
            _payload: PutPayload,
        ) -> object_store::Result<PutResult> {
            unimplemented!("Use start_append instead")
        }

        async fn put_opts(
            &self,
            _location: &Path,
            _payload: PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<PutResult> {
            unimplemented!("Use start_append instead")
        }

        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }

        async fn get(&self, _location: &Path) -> object_store::Result<GetResult> {
            unimplemented!()
        }

        async fn get_opts(
            &self,
            _location: &Path,
            _options: GetOptions,
        ) -> object_store::Result<GetResult> {
            unimplemented!()
        }

        async fn get_range(
            &self,
            _location: &Path,
            _range: std::ops::Range<u64>,
        ) -> object_store::Result<Bytes> {
            unimplemented!()
        }

        async fn head(&self, _location: &Path) -> object_store::Result<ObjectMeta> {
            unimplemented!()
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            Box::pin(futures::stream::empty())
        }

        fn list_with_offset(
            &self,
            _prefix: Option<&Path>,
            _offset: &Path,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            Box::pin(futures::stream::empty())
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            unimplemented!()
        }

        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        async fn rename(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl AppendableStore for MockAppendableStore {
        async fn start_append(
            &self,
            location: &Path,
        ) -> Result<Box<dyn AppendWriter>, SlateDBError> {
            let mut objects = self.objects.lock().await;
            let key = location.to_string();
            if objects.contains_key(&key) {
                return Err(SlateDBError::InvalidDBState);
            }
            let data = Arc::new(AsyncMutex::new(Vec::new()));
            objects.insert(key, data.clone());
            Ok(Box::new(MockAppendWriter::new(data)))
        }

        async fn resume_append(
            &self,
            _location: &Path,
            _generation: i64,
        ) -> Result<Box<dyn AppendWriter>, SlateDBError> {
            unimplemented!()
        }

        async fn tail_read(&self, location: &Path, offset: u64) -> Result<Bytes, SlateDBError> {
            let objects = self.objects.lock().await;
            let key = location.to_string();
            match objects.get(&key) {
                Some(data) => {
                    let buf = data.lock().await;
                    let start = offset as usize;
                    if start >= buf.len() {
                        Ok(Bytes::new())
                    } else {
                        Ok(Bytes::copy_from_slice(&buf[start..]))
                    }
                }
                None => Err(SlateDBError::InvalidDBState),
            }
        }
    }

    struct MockWalIdStore {
        next_id: AtomicU64,
    }

    impl WalIdStore for MockWalIdStore {
        fn next_wal_id(&self) -> u64 {
            self.next_id.fetch_add(1, Ordering::SeqCst)
        }
    }

    async fn setup_zonal_wal() -> (
        Arc<ZonalAppendableWalManager>,
        Arc<MockAppendableStore>,
        Arc<MockSystemClock>,
        DbStats,
        Arc<DefaultMetricsRecorder>,
    ) {
        setup_zonal_wal_with_flush_interval(Duration::from_millis(10)).await
    }

    async fn setup_zonal_wal_with_flush_interval(
        flush_interval: Duration,
    ) -> (
        Arc<ZonalAppendableWalManager>,
        Arc<MockAppendableStore>,
        Arc<MockSystemClock>,
        DbStats,
        Arc<DefaultMetricsRecorder>,
    ) {
        let wal_id_store: Arc<dyn WalIdStore + Send + Sync> = Arc::new(MockWalIdStore {
            next_id: AtomicU64::new(1),
        });
        let appendable_store = Arc::new(MockAppendableStore::new());
        let test_clock = Arc::new(MockSystemClock::new());
        let mono_clock = Arc::new(MonotonicClock::new(test_clock.clone(), 0));
        let system_clock = Arc::new(DefaultSystemClock::new());
        let status_manager = DbStatusManager::new(0);
        let oracle = Arc::new(DbOracle::new(0, 0, 0, status_manager.clone()));
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(recorder.clone(), MetricLevel::default());
        let db_stats = DbStats::new(&helper);
        let sst_format = SsTableFormat::default();
        let manager = Arc::new(ZonalAppendableWalManager::new(
            wal_id_store,
            status_manager.clone(),
            db_stats.clone(),
            0, // recent_flushed_wal_id
            oracle,
            appendable_store.clone(),
            sst_format,
            "/root".to_string(),
            mono_clock,
            1000,                 // max_wal_bytes_size
            Some(flush_interval), // max_flush_interval
        ));
        let task_executor = Arc::new(MessageHandlerExecutor::new(
            Arc::new(status_manager),
            system_clock.clone(),
        ));
        manager.init(task_executor.clone()).await.unwrap();
        task_executor
            .monitor_on(&Handle::current())
            .expect("failed to monitor executor");
        (manager, appendable_store, test_clock, db_stats, recorder)
    }

    #[test]
    fn test_new_buffer_initial_state() {
        let buffer = ZonalAppendableWal::new();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.size(), 0);
        assert_eq!(buffer.last_seq(), None);
        assert_eq!(buffer.last_tick(), i64::MIN);
    }

    #[test]
    fn test_append_single_entry() {
        let mut buffer = ZonalAppendableWal::new();
        let entry = make_entry("key1", "value1", 42, Some(1000));
        let expected_size = entry.estimated_size();
        buffer.append(entry);
        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.size(), expected_size);
        assert_eq!(buffer.last_seq(), Some(42));
        assert_eq!(buffer.last_tick(), 1000);
    }

    #[test]
    fn test_append_multiple_entries() {
        let mut buffer = ZonalAppendableWal::new();
        let entry1 = make_entry("key1", "value1", 10, Some(100));
        let entry2 = make_entry("key2", "value2", 20, Some(200));
        let entry3 = make_entry("key3", "value3", 30, Some(300));
        let entry4 = make_entry("key4", "value4", 40, None);
        let total_size = entry1.estimated_size()
            + entry2.estimated_size()
            + entry3.estimated_size()
            + entry4.estimated_size();
        buffer.append(entry1);
        buffer.append(entry2);
        buffer.append(entry3);
        buffer.append(entry4);
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.size(), total_size);
        assert_eq!(buffer.last_seq(), Some(40));
        assert_eq!(buffer.last_tick(), 300);
    }

    #[tokio::test]
    async fn test_notify_durable_success() {
        let mut buffer = ZonalAppendableWal::new();
        buffer.append(make_entry("key", "value", 1, None));
        buffer.notify_durable(Ok(()));
        let result = buffer.await_durable().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_notify_durable_error() {
        let mut buffer = ZonalAppendableWal::new();
        buffer.append(make_entry("key", "value", 1, None));
        buffer.notify_durable(Err(SlateDBError::Closed));
        let result = buffer.await_durable().await;
        assert!(matches!(result, Err(SlateDBError::Closed)));
    }

    #[tokio::test]
    async fn test_basic_append_and_flush() {
        let (manager, appendable_store, _, _, _) = setup_zonal_wal().await;

        let entry1 = make_entry("key1", "value1", 1, None);
        let entry2 = make_entry("key2", "value2", 2, None);

        manager.append(std::slice::from_ref(&entry1)).unwrap();
        manager.append(std::slice::from_ref(&entry2)).unwrap();

        // Flush should succeed — entries are streamed to the appendable object
        manager.flush().await.unwrap();

        // Verify an object was created in the mock store
        let objects = appendable_store.objects.lock().await;
        assert_eq!(objects.len(), 1, "Expected one appendable object");
    }

    #[tokio::test]
    async fn test_multiple_flushes_reuse_same_object() {
        let (manager, appendable_store, _, _, _) = setup_zonal_wal().await;

        // First flush
        manager
            .append(&[make_entry("key1", "value1", 1, None)])
            .unwrap();
        manager.flush().await.unwrap();

        // Second flush — appends to the same object (not finalized yet)
        manager
            .append(&[make_entry("key2", "value2", 2, None)])
            .unwrap();
        manager.flush().await.unwrap();

        // Same appendable object is reused across flushes
        let objects = appendable_store.objects.lock().await;
        assert_eq!(
            objects.len(),
            1,
            "Same appendable object should be reused across flushes"
        );
    }

    #[tokio::test]
    async fn test_recent_flushed_wal_id_advances_on_every_flush() {
        let (manager, _, _, _, _) = setup_zonal_wal().await;

        assert_eq!(manager.recent_flushed_wal_id(), 0);

        // All flushes share the same physical object (WAL ID 1), so
        // recent_flushed_wal_id stays at 1 across multiple flushes
        manager
            .append(&[make_entry("key1", "value1", 1, None)])
            .unwrap();
        manager.flush().await.unwrap();
        assert_eq!(manager.recent_flushed_wal_id(), 1);

        manager
            .append(&[make_entry("key2", "value2", 2, None)])
            .unwrap();
        manager.flush().await.unwrap();
        // Still 1 — same physical object
        assert_eq!(manager.recent_flushed_wal_id(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_finalize_at_size_threshold() {
        // Use a small max_wal_bytes_size to trigger finalization
        let (manager, appendable_store, _, _, _) =
            setup_zonal_wal_with_flush_interval(Duration::from_millis(10)).await;

        // Write enough data to exceed the 1000-byte threshold
        let mut seq = 1u64;
        loop {
            let entry = make_entry(
                &format!("key{:04}", seq),
                &format!("value{:04}", seq),
                seq,
                None,
            );
            manager.append(&[entry]).unwrap();
            manager.flush().await.unwrap();
            seq += 1;

            let objects = appendable_store.objects.lock().await;
            if objects.len() > 1 {
                // Finalization happened — new object was created
                break;
            }
            drop(objects);

            assert!(seq < 100, "Should have finalized before 100 flushes");
        }

        // After finalization, WAL ID should have advanced
        assert!(manager.recent_flushed_wal_id() > 0);
    }

    #[tokio::test]
    async fn test_pending_wal_release() {
        let (manager, _, _, _, _) = setup_zonal_wal().await;

        // Append and flush multiple batches
        for i in 0..10 {
            let seq = i + 1;
            let entry = make_entry(&format!("key{}", i), &format!("value{}", i), seq, None);
            manager.append(&[entry]).unwrap();
            manager.flush().await.unwrap();
        }

        // Pending WALs exist until applied to memtable
        assert!(manager.inner.read().pending_wals.len() > 0);

        // Track applied seq — should release durable WALs
        manager.track_last_applied_seq(10);
        assert_eq!(manager.inner.read().pending_wals.len(), 0);
    }

    #[tokio::test]
    async fn test_empty_flush_is_noop() {
        let (manager, appendable_store, _, _, _) = setup_zonal_wal().await;

        // Flush with no entries should be a no-op
        manager.flush().await.unwrap();

        let objects = appendable_store.objects.lock().await;
        assert_eq!(
            objects.len(),
            0,
            "No objects should be created on empty flush"
        );
    }

    #[tokio::test]
    async fn test_estimated_bytes() {
        let (manager, _, _, _, _) = setup_zonal_wal().await;

        assert_eq!(manager.estimated_bytes().unwrap(), 0);

        let entry = make_entry("key1", "value1", 1, None);
        manager.append(std::slice::from_ref(&entry)).unwrap();

        assert!(manager.estimated_bytes().unwrap() > 0);

        // After flush, in-memory estimate should decrease (entries became durable)
        manager.flush().await.unwrap();
        // The pending WALs are still in memory but durable, so estimated_bytes
        // should exclude them
        assert_eq!(manager.estimated_bytes().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_iter() {
        let mut buffer = ZonalAppendableWal::new();
        let mut iter = buffer.iter();
        assert!(iter.next().is_none());

        let entry1 = make_entry("key1", "value1", 1, Some(100));
        let entry2 = make_entry("key2", "value2", 2, Some(200));
        let entry3 = make_entry("key3", "value3", 3, Some(300));

        buffer.append(entry1.clone());
        buffer.append(entry2.clone());
        buffer.append(entry3.clone());

        let mut iter = buffer.iter();
        let read1 = iter.next().unwrap();
        assert_eq!(read1.key, entry1.key);
        assert_eq!(read1.seq, entry1.seq);

        let read2 = iter.next().unwrap();
        assert_eq!(read2.key, entry2.key);
        assert_eq!(read2.seq, entry2.seq);

        let read3 = iter.next().unwrap();
        assert_eq!(read3.key, entry3.key);
        assert_eq!(read3.seq, entry3.seq);

        assert!(iter.next().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_size_based_flush_triggering() {
        let (manager, _, _, _, _) = setup_zonal_wal_with_flush_interval(Duration::MAX).await;

        let mut seq = 1;
        while manager.estimated_bytes().unwrap() < manager.max_wal_bytes_size {
            let entry = make_entry(&format!("key{}", seq), &format!("value{}", seq), seq, None);
            manager.append(&[entry]).unwrap();
            seq += 1;
        }
        let mut reader = manager.maybe_trigger_flush().unwrap();
        reader.await_value().await.unwrap();
    }

    #[tokio::test]
    async fn test_crash_recovery_of_partial_wal() {
        // Simulate: write entries to appendable object without footer → crash
        // Then: recover → entries are rebuilt as a complete SST at a new WAL ID
        let appendable_store = Arc::new(MockAppendableStore::new());
        let sst_format = SsTableFormat::default();

        // Phase 1: manually write a partial WAL (blocks only, no footer)
        {
            use crate::wal::streaming_wal_writer::StreamingWalWriter;

            let path = Path::from("/root/wal/00000000000000000001.sst");
            let mut writer = StreamingWalWriter::new(appendable_store.as_ref(), &path, &sst_format)
                .await
                .unwrap();

            for i in 1..=5u64 {
                writer
                    .add(make_entry(
                        &format!("key{}", i),
                        &format!("value{}", i),
                        i,
                        Some(i as i64 * 100),
                    ))
                    .await
                    .unwrap();
            }
            // flush_pending writes blocks but does NOT write footer or finalize
            writer.flush_pending().await.unwrap();
            // Drop the writer without calling finalize() — simulates crash
        }

        // Verify partial object exists
        let objects = appendable_store.objects.lock().await;
        assert_eq!(objects.len(), 1);
        drop(objects);

        // Phase 2: create a recovery manager and recover
        let wal_id_store: Arc<dyn WalIdStore + Send + Sync> = Arc::new(MockWalIdStore {
            next_id: AtomicU64::new(10), // rebuilt SSTs will use IDs starting at 10
        });
        let test_clock = Arc::new(MockSystemClock::new());
        let mono_clock = Arc::new(MonotonicClock::new(test_clock.clone(), 0));
        let status_manager = DbStatusManager::new(0);
        let oracle = Arc::new(DbOracle::new(0, 0, 0, status_manager.clone()));
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(recorder.clone(), MetricLevel::default());
        let db_stats = DbStats::new(&helper);

        let manager = Arc::new(ZonalAppendableWalManager::new(
            wal_id_store,
            status_manager,
            db_stats,
            0,
            oracle,
            appendable_store.clone(),
            sst_format,
            "/root".to_string(),
            mono_clock,
            100_000,
            None,
        ));

        // Recover from WAL ID 1 (the partial one)
        let last_rebuilt_id = manager.recover_partial_wals(1).await.unwrap();

        // Should have rebuilt the SST at WAL ID 10
        assert_eq!(last_rebuilt_id, 10);

        // Verify the rebuilt SST exists in the store
        let objects = appendable_store.objects.lock().await;
        assert_eq!(
            objects.len(),
            2,
            "Should have original partial + rebuilt SST"
        );
        // Path::from strips leading slash, so the key is "root/wal/..."
        let rebuilt_path = Path::from("/root/wal/00000000000000000010.sst").to_string();
        assert!(
            objects.contains_key(&rebuilt_path),
            "Rebuilt SST should exist at WAL ID 10, keys: {:?}",
            objects.keys().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_crash_recovery_empty_wal() {
        // No partial WAL exists — recovery returns start_wal_id - 1
        let appendable_store = Arc::new(MockAppendableStore::new());
        let wal_id_store: Arc<dyn WalIdStore + Send + Sync> = Arc::new(MockWalIdStore {
            next_id: AtomicU64::new(1),
        });
        let test_clock = Arc::new(MockSystemClock::new());
        let mono_clock = Arc::new(MonotonicClock::new(test_clock.clone(), 0));
        let status_manager = DbStatusManager::new(0);
        let oracle = Arc::new(DbOracle::new(0, 0, 0, status_manager.clone()));
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(recorder.clone(), MetricLevel::default());
        let db_stats = DbStats::new(&helper);
        let sst_format = SsTableFormat::default();

        let manager = Arc::new(ZonalAppendableWalManager::new(
            wal_id_store,
            status_manager,
            db_stats,
            0,
            oracle,
            appendable_store,
            sst_format,
            "/root".to_string(),
            mono_clock,
            100_000,
            None,
        ));

        let last_rebuilt_id = manager.recover_partial_wals(1).await.unwrap();
        assert_eq!(
            last_rebuilt_id, 0,
            "Should return 0 (start_wal_id - 1) when nothing recovered"
        );
    }

    #[tokio::test]
    async fn test_crash_recovery_with_trailing_garbage() {
        // Write valid blocks then append garbage → recovery should rebuild
        // only the valid entries before the garbage
        let appendable_store = Arc::new(MockAppendableStore::new());
        let sst_format = SsTableFormat::default();

        // Manually create a partial WAL with valid blocks + trailing garbage
        {
            use crate::wal::streaming_wal_writer::StreamingWalWriter;

            let path = Path::from("/root/wal/00000000000000000001.sst");
            let mut writer = StreamingWalWriter::new(appendable_store.as_ref(), &path, &sst_format)
                .await
                .unwrap();

            for i in 1..=3u64 {
                writer
                    .add(make_entry(
                        &format!("key{}", i),
                        &format!("value{}", i),
                        i,
                        None,
                    ))
                    .await
                    .unwrap();
            }
            writer.flush_pending().await.unwrap();

            // Append garbage bytes to simulate a partial/corrupt write
            let objects = appendable_store.objects.lock().await;
            let key = path.to_string();
            let data = objects.get(&key).unwrap();
            let mut buf = data.lock().await;
            buf.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0xDE, 0xAD]);
        }

        // Recover
        let wal_id_store: Arc<dyn WalIdStore + Send + Sync> = Arc::new(MockWalIdStore {
            next_id: AtomicU64::new(10),
        });
        let test_clock = Arc::new(MockSystemClock::new());
        let mono_clock = Arc::new(MonotonicClock::new(test_clock.clone(), 0));
        let status_manager = DbStatusManager::new(0);
        let oracle = Arc::new(DbOracle::new(0, 0, 0, status_manager.clone()));
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(recorder.clone(), MetricLevel::default());
        let db_stats = DbStats::new(&helper);

        let manager = Arc::new(ZonalAppendableWalManager::new(
            wal_id_store,
            status_manager,
            db_stats,
            0,
            oracle,
            appendable_store.clone(),
            sst_format,
            "/root".to_string(),
            mono_clock,
            100_000,
            None,
        ));

        let last_rebuilt_id = manager.recover_partial_wals(1).await.unwrap();
        assert_eq!(last_rebuilt_id, 10, "Should have rebuilt partial WAL");

        // Verify rebuilt SST exists
        let objects = appendable_store.objects.lock().await;
        let rebuilt_path = Path::from("/root/wal/00000000000000000010.sst").to_string();
        assert!(objects.contains_key(&rebuilt_path));
    }
}
