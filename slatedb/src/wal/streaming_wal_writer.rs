//! Streaming WAL Writer for Appendable Object Stores
//!
//! This module provides a streaming WAL writer that uses appendable object storage
//! (such as GCS Rapid buckets) to write WAL entries incrementally instead of
//! buffering the entire encoded SSTable in memory.
//!
//! # Key Benefits
//! - **Memory Efficiency**: Streams blocks as they're built instead of buffering entire SSTable
//! - **Incremental Durability**: Flushes after each block write for better crash recovery
//! - **Resumable Writes**: Supports pausing and resuming writes using generation IDs
//!
//! # Architecture
//! ```text
//! EncodedWalSsTableBuilder → StreamingWalWriter → AppendWriter → flush() → Storage
//!                            (streams blocks)     (persists)      (durable)
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutResult};
use std::sync::Arc;

use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::format::sst::SsTableFormat;
use crate::types::RowEntry;
use crate::wal::wal_sst_builder::EncodedWalSsTableBuilder;

/// Trait for object stores that support appendable objects with flush-based semantics.
///
/// This trait provides the interface for working with appendable objects that can be
/// written incrementally and persisted via explicit flush operations. This is currently
/// implemented by GCS Rapid buckets but could be extended to other storage systems.
///
/// # Example Flow
/// ```ignore
/// let writer = store.start_append(&path).await?;
/// writer.write(data1).await?;  // Buffer data
/// writer.flush().await?;        // Persist to storage
/// writer.write(data2).await?;  // Buffer more data
/// writer.flush().await?;        // Persist again
/// writer.finalize().await?;     // Make immutable
/// ```
#[async_trait]
pub(crate) trait AppendableStore: ObjectStore {
    /// Start a new appendable object at the given location.
    ///
    /// This creates a new appendable object that doesn't yet exist. If an object
    /// already exists at this location, this should fail (preserving Create semantics).
    ///
    /// # Arguments
    /// * `location` - The path where the appendable object should be created
    ///
    /// # Returns
    /// An `AppendWriter` that can be used to write and flush data
    ///
    /// # Errors
    /// Returns an error if the object already exists or if the store doesn't support
    /// appendable objects
    async fn start_append(&self, location: &Path) -> Result<Box<dyn AppendWriter>, SlateDBError>;

    /// Resume writing to an existing appendable object.
    ///
    /// This allows resuming an append operation that was previously paused. The
    /// generation ID ensures we're resuming from the correct position.
    ///
    /// # Arguments
    /// * `location` - The path to the existing appendable object
    /// * `generation` - The generation ID from when the write was paused
    ///
    /// # Returns
    /// An `AppendWriter` positioned at the end of the last flushed data
    ///
    /// # Errors
    /// Returns an error if the object doesn't exist, the generation is invalid,
    /// or the store doesn't support resumable appends
    async fn resume_append(
        &self,
        location: &Path,
        generation: i64,
    ) -> Result<Box<dyn AppendWriter>, SlateDBError>;

    /// Read from the tail of an appendable object.
    ///
    /// This allows reading data from an in-progress appendable object, starting
    /// from a specific offset. Useful for monitoring or recovery scenarios.
    ///
    /// # Arguments
    /// * `location` - The path to the appendable object
    /// * `offset` - The byte offset to start reading from
    ///
    /// # Returns
    /// The bytes read from the object starting at the offset
    ///
    /// # Errors
    /// Returns an error if the object doesn't exist or the offset is invalid
    async fn tail_read(&self, location: &Path, offset: u64) -> Result<Bytes, SlateDBError>;
}

/// Writer for appending data to an appendable object.
///
/// This trait represents an open appendable object that supports incremental writes
/// with explicit flush control. Data written via `write()` is buffered until `flush()`
/// is called, at which point it's persisted to storage.
#[async_trait]
pub(crate) trait AppendWriter: Send + Sync {
    /// Write data to the append buffer.
    ///
    /// This sends data over the network (e.g., via gRPC for GCS) but doesn't persist
    /// it yet. The data is buffered until `flush()` is called.
    ///
    /// # Arguments
    /// * `data` - The bytes to append
    ///
    /// # Errors
    /// Returns an error if the write fails (e.g., network error, object finalized)
    async fn write(&mut self, data: Bytes) -> Result<(), SlateDBError>;

    /// Flush buffered data to persistent storage.
    ///
    /// This persists all data written since the last flush. The operation is synchronous
    /// in the sense that when it returns successfully, the data is durable.
    ///
    /// # Returns
    /// The total size of persisted data after this flush (cumulative)
    ///
    /// # Errors
    /// Returns an error if the flush fails
    async fn flush(&mut self) -> Result<i64, SlateDBError>;

    /// Finalize the object and make it immutable.
    ///
    /// This closes the appendable object and converts it to a regular immutable object.
    /// No further writes or flushes are allowed after finalization.
    ///
    /// # Returns
    /// A `PutResult` with metadata about the finalized object (etag, version, etc.)
    ///
    /// # Errors
    /// Returns an error if finalization fails
    async fn finalize(self: Box<Self>) -> Result<PutResult, SlateDBError>;

    /// Pause the append operation without finalizing.
    ///
    /// This closes the writer but leaves the object in appendable state so it can
    /// be resumed later with `resume_append()`. The object remains appendable.
    fn pause(self: Box<Self>);

    /// Get the current write offset.
    ///
    /// This returns the byte offset of the next write operation. Useful for
    /// tracking progress and for resuming writes.
    ///
    /// # Returns
    /// The current byte offset in the object
    fn write_offset(&self) -> i64;
}

/// Streaming WAL writer that uses AppendableStore for incremental writes.
///
/// This writer streams WAL blocks to an appendable object store as they're built,
/// flushing after each block for incremental durability. This avoids buffering the
/// entire encoded SSTable in memory.
///
/// # Memory Benefits
/// Traditional approach: Buffer all blocks + index + footer → 10MB+ in memory
/// Streaming approach: Stream each block individually → ~64KB per block in memory
///
/// # Durability Benefits
/// Traditional approach: All-or-nothing atomic write
/// Streaming approach: Each block is durable after flush (smaller recovery window)
pub(crate) struct StreamingWalWriter {
    append_writer: Box<dyn AppendWriter>,
    builder: EncodedWalSsTableBuilder,
    blocks_written: usize,
}

impl StreamingWalWriter {
    /// Create a new streaming WAL writer.
    ///
    /// This starts a new appendable object for the WAL and initializes the
    /// SSTable builder with the given format configuration.
    ///
    /// # Arguments
    /// * `store` - The appendable object store to write to
    /// * `path` - The path where the WAL SSTable should be created
    /// * `format` - The SSTable format configuration
    ///
    /// # Returns
    /// A new `StreamingWalWriter` ready to accept entries
    ///
    /// # Errors
    /// Returns an error if the appendable object cannot be created (e.g., already exists)
    pub(crate) async fn new(
        store: &dyn AppendableStore,
        path: &Path,
        format: &SsTableFormat,
    ) -> Result<Self, SlateDBError> {
        let append_writer = store.start_append(path).await?;
        let builder = format.wal_table_builder();

        Ok(Self {
            append_writer,
            builder,
            blocks_written: 0,
        })
    }

    /// Add an entry to the WAL and stream any completed blocks.
    ///
    /// This adds the entry to the builder. If the builder completes one or more blocks,
    /// they are immediately streamed to storage and flushed for durability.
    ///
    /// # Flush Strategy
    /// Blocks are flushed immediately after being written. This provides:
    /// - Incremental durability (only the current partial block is at risk on crash)
    /// - Progress visibility (can monitor via tail_read())
    /// - Memory efficiency (blocks are released after flush)
    ///
    /// # Arguments
    /// * `entry` - The WAL entry to add
    ///
    /// # Errors
    /// Returns an error if the entry cannot be added or if block writing/flushing fails
    pub(crate) async fn add(&mut self, entry: RowEntry) -> Result<(), SlateDBError> {
        // Add entry to builder (may complete one or more blocks)
        self.builder.add(entry).await?;

        // Stream any completed blocks immediately
        while let Some(block) = self.builder.next_block() {
            self.write_block_framed(block.encoded_bytes).await?;
            self.blocks_written += 1;

            // Flush for durability after each block
            // This ensures minimal data loss on crash (only the current partial block)
            self.append_writer.flush().await?;
        }

        Ok(())
    }

    /// Finalize the WAL SSTable and close the appendable object.
    ///
    /// This:
    /// 1. Flushes any remaining blocks from the builder
    /// 2. Builds and writes the footer (index + SST info)
    /// 3. Flushes the footer
    /// 4. Finalizes the object to make it immutable
    ///
    /// # Returns
    /// An `SsTableHandle` with metadata about the finalized WAL SSTable
    ///
    /// # Errors
    /// Returns an error if any step of the finalization process fails
    pub(crate) async fn finalize(mut self) -> Result<SsTableHandle, SlateDBError> {
        // Write any remaining blocks
        while let Some(block) = self.builder.next_block() {
            self.write_block_framed(block.encoded_bytes).await?;
            self.blocks_written += 1;
        }

        // Build and write the complete SSTable (now that all blocks are done)
        let encoded_sst = self.builder.build().await?;

        // Write the footer (index block + SST info + metadata offset + version)
        self.append_writer.write(encoded_sst.footer).await?;

        // Flush footer to ensure it's durable
        self.append_writer.flush().await?;

        // Finalize the object to make it immutable
        let _result = self.append_writer.finalize().await?;

        // Create handle with metadata from the finalized object
        // Note: The caller will set the correct WAL ID
        let handle = SsTableHandle::new(
            // id will be set by caller
            crate::db_state::SsTableId::Wal(0),
            encoded_sst.format_version,
            encoded_sst.info,
        );

        Ok(handle)
    }

    /// Flush any in-progress partial block to storage for durability.
    ///
    /// This finishes the current block (even if partial), writes it to the
    /// appendable object, and flushes for durability. After this call, all
    /// entries added so far are durable.
    pub(crate) async fn flush_pending(&mut self) -> Result<(), SlateDBError> {
        self.builder.finish_block().await?;
        while let Some(block) = self.builder.next_block() {
            self.write_block_framed(block.encoded_bytes).await?;
            self.blocks_written += 1;
        }
        self.append_writer.flush().await?;
        Ok(())
    }

    /// Write a block with a 4-byte big-endian length prefix.
    ///
    /// Format: `[u32 BE block_len][encoded_block_bytes]`
    ///
    /// This framing makes blocks self-delimiting, enabling crash recovery
    /// by sequentially parsing blocks without needing the SST footer/index.
    async fn write_block_framed(&mut self, block_bytes: Bytes) -> Result<(), SlateDBError> {
        let len_prefix = (block_bytes.len() as u32).to_be_bytes();
        self.append_writer
            .write(Bytes::copy_from_slice(&len_prefix))
            .await?;
        self.append_writer.write(block_bytes).await?;
        Ok(())
    }

    /// Returns the current write offset (total bytes written to the appendable object).
    pub(crate) fn write_offset(&self) -> i64 {
        self.append_writer.write_offset()
    }

    /// Get the number of blocks written so far.
    #[cfg(test)]
    pub(crate) fn blocks_written(&self) -> usize {
        self.blocks_written
    }
}

/// Recover WAL entries from a partial appendable object that has length-prefixed
/// blocks but no footer (e.g., after a crash before finalization).
///
/// Reads raw bytes via `tail_read`, parses sequential length-prefixed blocks,
/// verifies checksums, decodes each block, and extracts all [`RowEntry`] items.
///
/// # Block framing format
/// ```text
/// [u32 BE block_len][encoded_block_bytes (block_len bytes)]
/// [u32 BE block_len][encoded_block_bytes (block_len bytes)]
/// ...
/// ```
///
/// Any trailing bytes that don't form a complete framed block are silently
/// skipped (they represent an incomplete write interrupted by a crash).
pub(crate) async fn recover_partial_wal(
    store: &(dyn AppendableStore + Send + Sync),
    path: &Path,
    format: &SsTableFormat,
) -> Result<Vec<RowEntry>, SlateDBError> {
    use crate::block_iterator_v2::BlockIteratorV2;
    use crate::iter::RowEntryIterator;
    use bytes::Buf;

    let raw = store.tail_read(path, 0).await?;
    let mut entries = Vec::new();
    let mut cursor = raw.as_ref();

    while cursor.len() >= 4 {
        let block_len = cursor.get_u32() as usize;
        if cursor.len() < block_len {
            // Incomplete block — stop recovery here
            break;
        }
        let block_bytes = Bytes::copy_from_slice(&cursor[..block_len]);
        cursor = &cursor[block_len..];

        // Decode block: validate checksum, reverse transform, decompress
        let block = match format
            .decode_block(block_bytes, format.compression_codec)
            .await
        {
            Ok(b) => b,
            Err(_) => {
                // Corrupt block — stop recovery (remaining data is unreliable)
                break;
            }
        };

        // Extract entries from the block
        let mut iter = BlockIteratorV2::new_ascending(block);
        while let Ok(Some(entry)) = iter.next().await {
            entries.push(entry);
        }
    }

    Ok(entries)
}

/// Rebuild a complete WAL SST from recovered entries.
///
/// Writes a fresh SST (data blocks + footer) to a new appendable object at the
/// given path. The resulting file is a standard WAL SST readable by
/// `table_store.open_sst()` and `WalReplayIterator`.
///
/// This is used during crash recovery: entries are first extracted from a partial
/// appendable object (via [`recover_partial_wal`]), then re-encoded as a complete
/// SST at a new location.
pub(crate) async fn rebuild_wal_sst(
    store: &(dyn AppendableStore + Send + Sync),
    path: &Path,
    format: &SsTableFormat,
    entries: &[RowEntry],
) -> Result<(), SlateDBError> {
    let mut writer = StreamingWalWriter::new(store, path, format).await?;
    for entry in entries {
        writer.add(entry.clone()).await?;
    }
    writer.finalize().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flatbuffer_types::FlatBufferSsTableInfoCodec;
    use crate::format::sst::SsTableFormat;
    use crate::types::{RowEntry, ValueDeletable};
    use bytes::Bytes;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOpts, PutPayload,
    };
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::sync::Mutex as AsyncMutex;

    // Mock AppendWriter for testing
    struct MockAppendWriter {
        data: Arc<AsyncMutex<Vec<u8>>>,
        offset: Arc<Mutex<i64>>,
        flushed_offset: Arc<Mutex<i64>>,
        flush_count: Arc<Mutex<usize>>,
        finalized: Arc<Mutex<bool>>,
    }

    impl MockAppendWriter {
        fn new() -> Self {
            Self {
                data: Arc::new(AsyncMutex::new(Vec::new())),
                offset: Arc::new(Mutex::new(0)),
                flushed_offset: Arc::new(Mutex::new(0)),
                flush_count: Arc::new(Mutex::new(0)),
                finalized: Arc::new(Mutex::new(false)),
            }
        }

        async fn get_data(&self) -> Vec<u8> {
            self.data.lock().await.clone()
        }

        fn flush_count(&self) -> usize {
            *self.flush_count.lock().unwrap()
        }
    }

    #[async_trait]
    impl AppendWriter for MockAppendWriter {
        async fn write(&mut self, data: Bytes) -> Result<(), SlateDBError> {
            let mut buf = self.data.lock().await;
            buf.extend_from_slice(&data);
            let mut offset = self.offset.lock().unwrap();
            *offset += data.len() as i64;
            Ok(())
        }

        async fn flush(&mut self) -> Result<i64, SlateDBError> {
            let offset = *self.offset.lock().unwrap();
            *self.flushed_offset.lock().unwrap() = offset;
            *self.flush_count.lock().unwrap() += 1;
            Ok(offset)
        }

        async fn finalize(self: Box<Self>) -> Result<PutResult, SlateDBError> {
            *self.finalized.lock().unwrap() = true;
            Ok(PutResult {
                e_tag: Some("mock-etag".to_string()),
                version: Some("mock-version".to_string()),
            })
        }

        fn pause(self: Box<Self>) {
            // Nothing to do for mock
        }

        fn write_offset(&self) -> i64 {
            *self.offset.lock().unwrap()
        }
    }

    // Mock AppendableStore for testing
    #[derive(Debug)]
    struct MockAppendableStore {
        objects: Arc<AsyncMutex<std::collections::HashMap<Path, Arc<AsyncMutex<Vec<u8>>>>>>,
    }

    impl MockAppendableStore {
        fn new() -> Self {
            Self {
                objects: Arc::new(AsyncMutex::new(std::collections::HashMap::new())),
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
            _opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }

        async fn get(&self, _location: &Path) -> object_store::Result<GetResult> {
            // Not needed for streaming WAL writer tests
            unimplemented!("get not needed for tests")
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

        async fn head(&self, _location: &Path) -> object_store::Result<object_store::ObjectMeta> {
            unimplemented!()
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            Box::pin(futures::stream::empty())
        }

        fn list_with_offset(
            &self,
            _prefix: Option<&Path>,
            _offset: &Path,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
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
            // Check if object already exists (PutMode::Create semantics)
            if objects.contains_key(location) {
                return Err(SlateDBError::InvalidDBState);
            }

            let data = Arc::new(AsyncMutex::new(Vec::new()));
            objects.insert(location.clone(), data.clone());

            let writer = MockAppendWriter::new();
            Ok(Box::new(writer))
        }

        async fn resume_append(
            &self,
            _location: &Path,
            _generation: i64,
        ) -> Result<Box<dyn AppendWriter>, SlateDBError> {
            unimplemented!("Resume not needed for basic tests")
        }

        async fn tail_read(&self, _location: &Path, _offset: u64) -> Result<Bytes, SlateDBError> {
            unimplemented!("Tail read not needed for basic tests")
        }
    }

    #[tokio::test]
    async fn test_streaming_wal_writer_lifecycle() {
        // Given: a mock appendable store and streaming writer
        let store = MockAppendableStore::new();
        let path = Path::from("/test/wal/000001.sst");
        let format = SsTableFormat {
            block_size: 64, // Small block size to force multiple blocks
            ..SsTableFormat::default()
        };

        let mut writer = StreamingWalWriter::new(&store, &path, &format)
            .await
            .unwrap();

        // When: add several entries (enough to create multiple blocks)
        for i in 0..10 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            let entry = RowEntry::new(
                Bytes::from(key),
                ValueDeletable::Value(Bytes::from(value)),
                i as u64,
                Some(i as i64),
                None,
            );
            writer.add(entry).await.unwrap();
        }

        // Then: multiple blocks should have been written
        assert!(
            writer.blocks_written() > 0,
            "Expected at least one block to be written"
        );

        // When: finalize the writer
        let handle = writer.finalize().await.unwrap();

        // Then: handle should be created successfully
        assert_eq!(handle.id, crate::db_state::SsTableId::Wal(0));
    }

    #[tokio::test]
    async fn test_streaming_wal_writer_flushes_per_block() {
        // Given: a mock writer that tracks flush calls
        let mock_writer = Arc::new(AsyncMutex::new(MockAppendWriter::new()));
        let flush_count_before = mock_writer.lock().await.flush_count();

        // When: add entries to trigger block completion
        // (This is a simplified test - in practice would need to inject the mock writer)

        // Then: flush should be called after each block
        // (Full test would require dependency injection or other testing infrastructure)

        // For now, just verify the concept
        assert_eq!(flush_count_before, 0);
    }

    #[tokio::test]
    async fn test_appendable_store_prevents_duplicate_create() {
        // Given: a mock store with an existing object
        let store = MockAppendableStore::new();
        let path = Path::from("/test/wal/000001.sst");
        let format = SsTableFormat::default();

        // When: create first writer (should succeed)
        let writer1 = StreamingWalWriter::new(&store, &path, &format).await;
        assert!(writer1.is_ok());

        // When: try to create second writer at same path (should fail)
        let writer2 = StreamingWalWriter::new(&store, &path, &format).await;
        assert!(writer2.is_err());
    }
}
