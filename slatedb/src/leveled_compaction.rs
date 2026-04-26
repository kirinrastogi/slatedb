use std::collections::HashSet;

use bytes::Bytes;

use crate::compactor::{CompactionScheduler, CompactionSchedulerSupplier};
use crate::compactor_state::{CompactionSpec, SourceId};
use crate::compactor_state_protocols::CompactorStateView;
use crate::config::{CompactorOptions, LeveledCompactionSchedulerOptions};
use crate::db_state::SortedRun;
use crate::error::Error;
use crate::manifest::ManifestCore;
use log::warn;

/// Implements a leveled compaction scheduler modeled after RocksDB's leveled compaction.
///
/// Data is organized into levels L0, L1, ..., L(num_levels-1) with exponentially
/// increasing target sizes. Each level (except L0) corresponds to a single sorted run
/// identified by a pre-assigned SR ID:
///
///   `sr_id(level) = num_levels - 1 - level`
///
/// This ensures L1 has the highest SR ID (highest merge priority) and the last level
/// has the lowest, matching slatedb's "higher id = newer data" convention.
///
/// Compaction is triggered when a level exceeds its target size:
/// - L0: triggered when file count >= `level0_file_num_compaction_trigger`
/// - Ln (n >= 1): triggered when `actual_size > target_size`
///
/// When triggered, the entire level is merged into the next level. This differs from
/// RocksDB which picks individual files, but preserves the key leveled compaction
/// properties (exponential size targets, scoring-based priority, cascading compactions).
pub(crate) struct LeveledCompactionScheduler {
    options: LeveledCompactionSchedulerOptions,
}

impl CompactionScheduler for LeveledCompactionScheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let db_state = state.manifest().core();

        let active_specs: Vec<&CompactionSpec> = state
            .compactions()
            .into_iter()
            .flat_map(|c| c.core().recent_compactions())
            .filter(|c| c.active())
            .map(|c| c.spec())
            .collect();

        match self.pick_compaction(db_state, &active_specs) {
            Some(spec) => vec![spec],
            None => vec![],
        }
    }

    fn validate(&self, state: &CompactorStateView, spec: &CompactionSpec) -> Result<(), Error> {
        let db_state = state.manifest().core();
        let sources = spec.sources();

        let has_l0 = sources.iter().any(|s| matches!(s, SourceId::SstView(_)));

        // Deduplicate SR IDs (file-level compactions have multiple SortedRunSst
        // entries for the same SR)
        let unique_sr_ids: Vec<u32> = {
            let mut seen = HashSet::new();
            sources
                .iter()
                .filter_map(|s| s.maybe_unwrap_sorted_run())
                .filter(|id| seen.insert(*id))
                .collect()
        };

        if has_l0 {
            // L0 compaction: destination must be L1
            let l1_sr_id = self.sr_id_for_level(1);
            if spec.destination() != l1_sr_id {
                warn!(
                    "leveled compaction: L0 compaction destination {} != L1 SR ID {}",
                    spec.destination(),
                    l1_sr_id
                );
                return Err(Error::invalid(
                    "L0 compaction destination must be L1".to_string(),
                ));
            }

            // Only allowed SR source is L1 (if it exists)
            for &sr_id in &unique_sr_ids {
                if sr_id != l1_sr_id {
                    warn!(
                        "leveled compaction: L0 compaction includes non-L1 SR source {}",
                        sr_id
                    );
                    return Err(Error::invalid(
                        "L0 compaction can only include L1 as SR source".to_string(),
                    ));
                }
            }
        } else {
            // SR-only compaction: must involve adjacent levels Ln -> Ln+1
            if unique_sr_ids.is_empty() || unique_sr_ids.len() > 2 {
                warn!(
                    "leveled compaction: SR compaction must involve 1 or 2 levels, got {}",
                    unique_sr_ids.len()
                );
                return Err(Error::invalid(
                    "SR compaction must involve 1 or 2 levels".to_string(),
                ));
            }

            let source_level = match self.level_for_sr_id(unique_sr_ids[0]) {
                Some(l) => l,
                None => {
                    warn!(
                        "leveled compaction: SR source {} does not map to a valid level",
                        unique_sr_ids[0]
                    );
                    return Err(Error::invalid(
                        "SR source does not map to a valid level".to_string(),
                    ));
                }
            };

            let dest_level = source_level + 1;
            if dest_level >= self.options.num_levels {
                warn!(
                    "leveled compaction: source level {} has no next level",
                    source_level
                );
                return Err(Error::invalid("source level has no next level".to_string()));
            }

            let expected_dest = self.sr_id_for_level(dest_level);
            if spec.destination() != expected_dest {
                warn!(
                    "leveled compaction: destination {} != expected {} for L{} -> L{}",
                    spec.destination(),
                    expected_dest,
                    source_level,
                    dest_level
                );
                return Err(Error::invalid(
                    "destination does not match expected next level".to_string(),
                ));
            }

            if unique_sr_ids.len() == 2 {
                let second_level = self.level_for_sr_id(unique_sr_ids[1]);
                if second_level != Some(dest_level) {
                    warn!(
                        "leveled compaction: second SR {} is not the destination level L{}",
                        unique_sr_ids[1], dest_level
                    );
                    return Err(Error::invalid(
                        "second SR source must be the destination level".to_string(),
                    ));
                }
            }

            // Verify SRs exist
            let sr_ids: HashSet<u32> = db_state.compacted.iter().map(|sr| sr.id).collect();
            for &sr_id in &unique_sr_ids {
                if !sr_ids.contains(&sr_id) {
                    warn!("leveled compaction: SR {} not found in state", sr_id);
                    return Err(Error::invalid(format!("SR {} not found in state", sr_id)));
                }
            }
        }

        Ok(())
    }
}

impl LeveledCompactionScheduler {
    pub(crate) fn new(options: LeveledCompactionSchedulerOptions) -> Self {
        assert!(
            options.num_levels >= 2,
            "num_levels must be at least 2, got {}",
            options.num_levels
        );
        Self { options }
    }

    /// Maps a logical level (1-based, 1..num_levels-1) to a sorted run ID.
    /// L1 gets the highest SR ID, L_last gets SR ID 0.
    fn sr_id_for_level(&self, level: usize) -> u32 {
        assert!(
            level >= 1 && level < self.options.num_levels,
            "level {} out of range [1, {})",
            level,
            self.options.num_levels
        );
        (self.options.num_levels - 1 - level) as u32
    }

    /// Maps an SR ID back to a logical level, or None if the ID doesn't correspond
    /// to a valid level in this configuration.
    fn level_for_sr_id(&self, sr_id: u32) -> Option<usize> {
        let level = self.options.num_levels - 1 - sr_id as usize;
        if level >= 1 && level < self.options.num_levels {
            Some(level)
        } else {
            None
        }
    }

    /// Returns the target size in bytes for a given level.
    fn level_target_size(&self, level: usize) -> u64 {
        let base = self.options.max_bytes_for_level_base as f64;
        let multiplier = self.options.max_bytes_for_level_multiplier;
        (base * multiplier.powi((level - 1) as i32)) as u64
    }

    /// Returns the actual size of a level, or 0 if the level's SR doesn't exist.
    fn level_size(&self, db_state: &ManifestCore, level: usize) -> u64 {
        let sr_id = self.sr_id_for_level(level);
        db_state
            .compacted
            .iter()
            .find(|sr| sr.id == sr_id)
            .map_or(0, |sr| sr.estimate_size())
    }

    /// Returns true if a sorted run ID is involved in any active compaction
    /// (as source or destination).
    fn is_sr_id_in_active_compaction(active_specs: &[&CompactionSpec], sr_id: u32) -> bool {
        active_specs.iter().any(|spec| {
            spec.destination() == sr_id
                || spec.sources().iter().any(|s| match s {
                    SourceId::SortedRun(id) => *id == sr_id,
                    SourceId::SortedRunSst { sr_id: id, .. } => *id == sr_id,
                    SourceId::SstView(_) => false,
                })
        })
    }

    /// Finds the sorted run for a given level in the manifest.
    fn find_sr_for_level<'a>(
        &self,
        db_state: &'a ManifestCore,
        level: usize,
    ) -> Option<&'a SortedRun> {
        let sr_id = self.sr_id_for_level(level);
        db_state.compacted.iter().find(|sr| sr.id == sr_id)
    }

    /// Returns true if any active compaction involves L0 sources.
    fn is_l0_in_active_compaction(active_specs: &[&CompactionSpec]) -> bool {
        active_specs.iter().any(|spec| spec.has_l0_sources())
    }

    /// Core scheduling logic: scores all levels and picks the highest-scoring
    /// level that exceeds 1.0 and isn't already being compacted.
    fn pick_compaction(
        &self,
        db_state: &ManifestCore,
        active_specs: &[&CompactionSpec],
    ) -> Option<CompactionSpec> {
        let mut best_level: Option<usize> = None;
        let mut best_score: f64 = 0.0;

        // Score L0
        let l0_score =
            db_state.l0.len() as f64 / self.options.level0_file_num_compaction_trigger as f64;
        if l0_score >= 1.0
            && l0_score > best_score
            && !Self::is_l0_in_active_compaction(active_specs)
        {
            // Also check L1 (destination) is not busy
            let l1_sr_id = self.sr_id_for_level(1);
            if !Self::is_sr_id_in_active_compaction(active_specs, l1_sr_id) {
                best_score = l0_score;
                best_level = Some(0);
            }
        }

        // Score L1 through L(num_levels - 2). The last level (num_levels - 1) is excluded
        // because it has nowhere to push data.
        for level in 1..=(self.options.num_levels - 2) {
            let target = self.level_target_size(level);
            if target == 0 {
                continue;
            }
            let actual = self.level_size(db_state, level);
            let score = actual as f64 / target as f64;

            if score >= 1.0 && score > best_score {
                let src_sr_id = self.sr_id_for_level(level);
                let dst_sr_id = self.sr_id_for_level(level + 1);
                if !Self::is_sr_id_in_active_compaction(active_specs, src_sr_id)
                    && !Self::is_sr_id_in_active_compaction(active_specs, dst_sr_id)
                {
                    best_score = score;
                    best_level = Some(level);
                }
            }
        }

        match best_level {
            Some(0) => Some(self.build_l0_compaction(db_state)),
            Some(level) => Some(self.build_level_compaction(db_state, level)),
            None => None,
        }
    }

    /// Builds a CompactionSpec for L0 -> L1.
    fn build_l0_compaction(&self, db_state: &ManifestCore) -> CompactionSpec {
        let l1_sr_id = self.sr_id_for_level(1);

        // Sources: all L0 SST views (newest first) + L1 SR if it exists
        let mut sources: Vec<SourceId> = db_state
            .l0
            .iter()
            .map(|view| SourceId::SstView(view.id))
            .collect();

        if db_state.compacted.iter().any(|sr| sr.id == l1_sr_id) {
            sources.push(SourceId::SortedRun(l1_sr_id));
        }

        CompactionSpec::new(sources, l1_sr_id)
    }

    /// Builds a file-level CompactionSpec for Ln -> Ln+1.
    ///
    /// Picks one SST from Ln (the first one, which covers the smallest key range),
    /// finds overlapping SSTs in Ln+1, and creates a spec that merges just those files.
    /// If Ln+1 doesn't exist, the single SST is moved down as a new SR.
    fn build_level_compaction(&self, db_state: &ManifestCore, level: usize) -> CompactionSpec {
        let src_sr_id = self.sr_id_for_level(level);
        let dst_sr_id = self.sr_id_for_level(level + 1);

        let src_sr = self
            .find_sr_for_level(db_state, level)
            .expect("source level SR must exist");

        // Pick the first SST from the source level (smallest key range).
        // RocksDB uses a round-robin or "compact pointer" to pick files;
        // picking the first file is a simpler starting point.
        let picked_sst = &src_sr.sst_views[0];

        let mut sources = vec![SourceId::SortedRunSst {
            sr_id: src_sr_id,
            view_id: picked_sst.id,
        }];

        // Find overlapping SSTs in the destination level
        if let Some(dst_sr) = self.find_sr_for_level(db_state, level + 1) {
            let start_key = picked_sst.compacted_effective_start_key();
            // For the end key, use the SST info's last_entry if available,
            // otherwise use start_key (single-key range)
            let end_key = picked_sst.sst.info.last_entry.as_ref().unwrap_or(start_key);
            let overlapping = Self::find_overlapping_sst_ids(dst_sr, start_key, end_key);
            for view_id in overlapping {
                sources.push(SourceId::SortedRunSst {
                    sr_id: dst_sr_id,
                    view_id,
                });
            }
        }

        CompactionSpec::new(sources, dst_sr_id)
    }

    /// Finds SST view IDs in a sorted run that overlap with the key range
    /// of the picked SST. Uses the SortedRun::tables_covering_range method
    /// which handles the boundary logic correctly.
    fn find_overlapping_sst_ids(
        sr: &SortedRun,
        start_key: &Bytes,
        end_key: &Bytes,
    ) -> Vec<ulid::Ulid> {
        let range = start_key.clone()..=end_key.clone();
        sr.tables_covering_range(range)
            .iter()
            .map(|view| view.id)
            .collect()
    }
}

/// Supplier that creates a [`LeveledCompactionScheduler`].
///
/// Use this with [`CompactorBuilder::with_scheduler_supplier`] or
/// [`DbBuilder::with_compactor_scheduler_supplier`] to enable leveled compaction.
///
/// # Example
///
/// ```ignore
/// use slatedb::leveled_compaction::LeveledCompactionSchedulerSupplier;
/// use std::sync::Arc;
///
/// let db = Db::builder("path", object_store)
///     .with_compactor_scheduler_supplier(Arc::new(LeveledCompactionSchedulerSupplier::new()))
///     .build()
///     .await?;
/// ```
#[derive(Default)]
pub struct LeveledCompactionSchedulerSupplier;

impl LeveledCompactionSchedulerSupplier {
    pub const fn new() -> Self {
        Self
    }
}

impl CompactionSchedulerSupplier for LeveledCompactionSchedulerSupplier {
    fn compaction_scheduler(
        &self,
        compactor_options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        let scheduler_options =
            LeveledCompactionSchedulerOptions::from(&compactor_options.scheduler_options);
        Box::new(LeveledCompactionScheduler::new(scheduler_options))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::compactor::{CompactionScheduler, CompactionSchedulerSupplier};
    use crate::compactor_state::{
        Compaction, CompactionSpec, Compactions, CompactorState, SourceId,
    };
    use crate::config::{CompactorOptions, LeveledCompactionSchedulerOptions};
    use crate::db_state::{SortedRun, SsTableHandle, SsTableId, SsTableInfo, SsTableView};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::leveled_compaction::{
        LeveledCompactionScheduler, LeveledCompactionSchedulerSupplier,
    };
    use crate::manifest::store::test_utils::new_dirty_manifest;
    use crate::manifest::ManifestCore;
    use crate::seq_tracker::SequenceTracker;
    use crate::utils::IdGenerator;
    use crate::DbRand;
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_txn_obj::test_utils::new_dirty_object;
    use std::sync::Arc;

    /// Default test config: num_levels=7, trigger=4, base=256, multiplier=10
    fn default_scheduler() -> LeveledCompactionScheduler {
        LeveledCompactionScheduler::new(LeveledCompactionSchedulerOptions {
            level0_file_num_compaction_trigger: 4,
            max_bytes_for_level_base: 256,
            max_bytes_for_level_multiplier: 10.0,
            num_levels: 7,
        })
    }

    /// Creates an SST view without key ranges (for L0 tests where ranges don't matter).
    fn create_sst_view(size: u64) -> SsTableView {
        let info = SsTableInfo {
            first_entry: None,
            last_entry: None,
            index_offset: size,
            index_len: 0,
            filter_offset: 0,
            filter_len: 0,
            compression_codec: None,
            ..Default::default()
        };
        SsTableView::identity(SsTableHandle::new(
            SsTableId::Compacted(ulid::Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            info,
        ))
    }

    /// Creates an SST view with key range [first_key, last_key].
    fn create_sst_view_with_range(size: u64, first_key: &[u8], last_key: &[u8]) -> SsTableView {
        let info = SsTableInfo {
            first_entry: Some(bytes::Bytes::copy_from_slice(first_key)),
            last_entry: Some(bytes::Bytes::copy_from_slice(last_key)),
            index_offset: size,
            index_len: 0,
            filter_offset: 0,
            filter_len: 0,
            compression_codec: None,
            ..Default::default()
        };
        SsTableView::identity(SsTableHandle::new(
            SsTableId::Compacted(ulid::Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            info,
        ))
    }

    /// Creates a sorted run without key ranges (for simple size-based tests).
    fn create_sr(id: u32, total_size: u64, num_ssts: usize) -> SortedRun {
        let sst_size = total_size / num_ssts as u64;
        let ssts: Vec<SsTableView> = (0..num_ssts).map(|_| create_sst_view(sst_size)).collect();
        SortedRun {
            id,
            sst_views: ssts,
        }
    }

    /// Creates a sorted run with SSTs that have sequential, non-overlapping key ranges.
    /// SST i covers keys [prefix_i_00, prefix_i_ff].
    fn create_sr_with_ranges(id: u32, sst_size: u64, num_ssts: usize) -> SortedRun {
        let ssts: Vec<SsTableView> = (0..num_ssts)
            .map(|i| {
                let first_key = format!("{:02}_00", i);
                let last_key = format!("{:02}_ff", i);
                create_sst_view_with_range(sst_size, first_key.as_bytes(), last_key.as_bytes())
            })
            .collect();
        SortedRun {
            id,
            sst_views: ssts,
        }
    }

    fn create_db_state(l0: VecDeque<SsTableView>, srs: Vec<SortedRun>) -> ManifestCore {
        ManifestCore {
            initialized: true,
            last_compacted_l0_sst_view_id: None,
            last_compacted_l0_sst_id: None,
            l0,
            compacted: srs,
            next_wal_sst_id: 0,
            replay_after_wal_id: 0,
            last_l0_seq: 0,
            last_l0_clock_tick: 0,
            checkpoints: vec![],
            wal_object_store_uri: None,
            recent_snapshot_min_seq: 0,
            sequence_tracker: SequenceTracker::new(),
        }
    }

    fn create_compactor_state(db_state: ManifestCore) -> CompactorState {
        let mut dirty = new_dirty_manifest();
        dirty.value.core = db_state;
        let compactions = new_dirty_object(1u64, Compactions::new(dirty.value.compactor_epoch));
        CompactorState::new(dirty, compactions)
    }

    // ---------------------------------------------------------------
    // L0 compaction tests
    // ---------------------------------------------------------------

    #[test]
    fn test_should_compact_l0_to_l1_when_trigger_reached() {
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..4).map(|_| create_sst_view(1)).collect();
        let state = create_compactor_state(create_db_state(l0.clone(), vec![]));

        let specs = scheduler.propose(&(&state).into());

        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        // All L0 views as sources, no SR source (L1 doesn't exist)
        let expected_sources: Vec<SourceId> = l0.iter().map(|v| SourceId::SstView(v.id)).collect();
        assert_eq!(spec.sources(), &expected_sources);
        // Destination is L1's SR ID = num_levels - 2 = 5
        assert_eq!(spec.destination(), 5);
    }

    #[test]
    fn test_should_not_compact_l0_below_trigger() {
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..3).map(|_| create_sst_view(1)).collect();
        let state = create_compactor_state(create_db_state(l0, vec![]));

        let specs = scheduler.propose(&(&state).into());
        assert!(specs.is_empty());
    }

    #[test]
    fn test_should_compact_l0_into_existing_l1() {
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..4).map(|_| create_sst_view(1)).collect();
        // L1 = SR id 5
        let l1 = create_sr(5, 100, 2);
        let state = create_compactor_state(create_db_state(l0.clone(), vec![l1]));

        let specs = scheduler.propose(&(&state).into());

        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        // Sources: all L0 views + L1 SR
        let mut expected_sources: Vec<SourceId> =
            l0.iter().map(|v| SourceId::SstView(v.id)).collect();
        expected_sources.push(SourceId::SortedRun(5));
        assert_eq!(spec.sources(), &expected_sources);
        assert_eq!(spec.destination(), 5);
    }

    #[test]
    fn test_should_compact_l0_when_l1_absent_but_deeper_levels_exist() {
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..4).map(|_| create_sst_view(1)).collect();
        // L2 = SR id 4, L3 = SR id 3 (no L1)
        let l2 = create_sr(4, 100, 2);
        let l3 = create_sr(3, 1000, 4);
        let state = create_compactor_state(create_db_state(l0.clone(), vec![l2, l3]));

        let specs = scheduler.propose(&(&state).into());

        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        // L0-only (L1 doesn't exist), destination = L1 SR id = 5
        let expected_sources: Vec<SourceId> = l0.iter().map(|v| SourceId::SstView(v.id)).collect();
        assert_eq!(spec.sources(), &expected_sources);
        assert_eq!(spec.destination(), 5);
    }

    // ---------------------------------------------------------------
    // Level compaction tests
    // ---------------------------------------------------------------

    #[test]
    fn test_should_compact_l1_to_l2_when_over_target() {
        let scheduler = default_scheduler();
        // L1 (SR 5) with 2 SSTs, total size = 300 > target of 256
        let l1 = create_sr_with_ranges(5, 150, 2);
        let l1_first_view = l1.sst_views[0].id;
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1]));

        let specs = scheduler.propose(&(&state).into());

        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        // File-level: picks first SST from L1
        assert_eq!(
            spec.sources(),
            &vec![SourceId::SortedRunSst {
                sr_id: 5,
                view_id: l1_first_view,
            }]
        );
        // Destination is L2's SR ID = 4
        assert_eq!(spec.destination(), 4);
    }

    #[test]
    fn test_should_compact_l1_into_existing_l2_with_overlapping_ssts() {
        let scheduler = default_scheduler();
        // L1 (SR 5): SSTs covering [00_00, 00_ff] and [01_00, 01_ff], total 300
        let l1 = create_sr_with_ranges(5, 150, 2);
        let l1_first_view = l1.sst_views[0].id;
        // L2 (SR 4): SSTs covering [00_00, 00_ff], [01_00, 01_ff], [02_00, 02_ff], [03_00, 03_ff]
        let l2 = create_sr_with_ranges(4, 250, 4);
        let l2_first_view = l2.sst_views[0].id;
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l2]));

        let specs = scheduler.propose(&(&state).into());

        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        // Picks first SST from L1 (range 00_00..00_ff)
        // Finds overlapping SST in L2 (range 00_00..00_ff)
        assert_eq!(spec.sources().len(), 2);
        assert_eq!(
            spec.sources()[0],
            SourceId::SortedRunSst {
                sr_id: 5,
                view_id: l1_first_view,
            }
        );
        assert_eq!(
            spec.sources()[1],
            SourceId::SortedRunSst {
                sr_id: 4,
                view_id: l2_first_view,
            }
        );
        assert_eq!(spec.destination(), 4);
    }

    #[test]
    fn test_should_compact_deeper_level_when_over_target() {
        let scheduler = default_scheduler();
        // L1 within target (256), L2 over target (2560)
        let l1 = create_sr_with_ranges(5, 100, 2);
        // L2 (SR 4) target = 256 * 10 = 2560, actual = 3000 > 2560
        let l2 = create_sr_with_ranges(4, 750, 4);
        let l2_first_view = l2.sst_views[0].id;
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l2]));

        let specs = scheduler.propose(&(&state).into());

        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        // L2 -> L3: picks first SST from L2
        assert_eq!(
            spec.sources()[0],
            SourceId::SortedRunSst {
                sr_id: 4,
                view_id: l2_first_view,
            }
        );
        assert_eq!(spec.destination(), 3); // L3 SR id
    }

    #[test]
    fn test_should_not_compact_last_level() {
        let scheduler = default_scheduler();
        // L6 (SR 0, last level) is huge but has nowhere to go
        let l6 = create_sr(0, 1_000_000, 10);
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l6]));

        let specs = scheduler.propose(&(&state).into());
        assert!(specs.is_empty());
    }

    #[test]
    fn test_should_not_compact_when_all_under_target() {
        let scheduler = default_scheduler();
        let l1 = create_sr(5, 100, 2); // < 256
        let l2 = create_sr(4, 1000, 4); // < 2560
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l2]));

        let specs = scheduler.propose(&(&state).into());
        assert!(specs.is_empty());
    }

    // ---------------------------------------------------------------
    // Scoring priority tests
    // ---------------------------------------------------------------

    #[test]
    fn test_should_prefer_highest_score() {
        let scheduler = default_scheduler();
        // L0: 8 files, score = 8/4 = 2.0
        let l0: VecDeque<_> = (0..8).map(|_| create_sst_view(1)).collect();
        // L1 (SR 5): 400 bytes, score = 400/256 = 1.5625
        let l1 = create_sr(5, 400, 2);
        let state = create_compactor_state(create_db_state(l0.clone(), vec![l1]));

        let specs = scheduler.propose(&(&state).into());

        assert_eq!(specs.len(), 1);
        // L0 score (2.0) > L1 score (1.56), so L0 compaction wins
        assert!(specs[0].has_l0_sources());
    }

    #[test]
    fn test_should_prefer_l1_when_higher_score() {
        let scheduler = default_scheduler();
        // L0: 5 files, score = 5/4 = 1.25
        let l0: VecDeque<_> = (0..5).map(|_| create_sst_view(1)).collect();
        // L1 (SR 5): 1000 bytes, score = 1000/256 = 3.9
        let l1 = create_sr_with_ranges(5, 250, 4);
        let l1_first_view = l1.sst_views[0].id;
        let state = create_compactor_state(create_db_state(l0, vec![l1]));

        let specs = scheduler.propose(&(&state).into());

        assert_eq!(specs.len(), 1);
        // L1 score (3.9) > L0 score (1.25), so L1 compaction wins
        // File-level: picks first SST from L1
        assert_eq!(
            specs[0].sources()[0],
            SourceId::SortedRunSst {
                sr_id: 5,
                view_id: l1_first_view,
            }
        );
        assert_eq!(specs[0].destination(), 4);
    }

    // ---------------------------------------------------------------
    // Conflict tests
    // ---------------------------------------------------------------

    #[test]
    fn test_should_skip_l0_if_l0_compaction_running() {
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..8).map(|_| create_sst_view(1)).collect();
        let mut state = create_compactor_state(create_db_state(l0.clone(), vec![]));

        // Simulate a running L0 compaction
        let rand = Arc::new(DbRand::default());
        let clock = Arc::new(DefaultSystemClock::new());
        let compaction_id = rand.rng().gen_ulid(clock.as_ref());
        let running_spec = CompactionSpec::new(
            l0.iter().take(4).map(|v| SourceId::SstView(v.id)).collect(),
            5,
        );
        state
            .add_compaction(Compaction::new(compaction_id, running_spec))
            .expect("failed to add");

        let specs = scheduler.propose(&(&state).into());
        assert!(specs.is_empty());
    }

    #[test]
    fn test_should_skip_level_if_destination_busy() {
        let scheduler = default_scheduler();
        // L1 over target
        let l1 = create_sr(5, 300, 2);
        // L2 exists and is involved in a running compaction (L2->L3)
        let l2 = create_sr(4, 3000, 4);
        let l3 = create_sr(3, 30000, 8);
        let mut state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l2, l3]));

        let rand = Arc::new(DbRand::default());
        let clock = Arc::new(DefaultSystemClock::new());
        let compaction_id = rand.rng().gen_ulid(clock.as_ref());
        // L2->L3 running: L2 (SR 4) is a source
        let running_spec =
            CompactionSpec::new(vec![SourceId::SortedRun(4), SourceId::SortedRun(3)], 3);
        state
            .add_compaction(Compaction::new(compaction_id, running_spec))
            .expect("failed to add");

        let specs = scheduler.propose(&(&state).into());

        // L1->L2 is skipped because L2 (SR 4) is busy as a source in L2->L3
        assert!(specs.is_empty());
    }

    #[test]
    fn test_should_allow_non_conflicting_compaction_when_other_running() {
        let scheduler = default_scheduler();
        // L1 over target, L3->L4 is running (no conflict with L1->L2)
        let l1 = create_sr_with_ranges(5, 150, 2);
        let l1_first_view = l1.sst_views[0].id;
        let l3 = create_sr_with_ranges(3, 7500, 4);
        let l4 = create_sr_with_ranges(2, 37500, 8);
        let mut state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l3, l4]));

        let rand = Arc::new(DbRand::default());
        let clock = Arc::new(DefaultSystemClock::new());
        let compaction_id = rand.rng().gen_ulid(clock.as_ref());
        // L3->L4 running (whole-level compaction)
        let running_spec =
            CompactionSpec::new(vec![SourceId::SortedRun(3), SourceId::SortedRun(2)], 2);
        state
            .add_compaction(Compaction::new(compaction_id, running_spec))
            .expect("failed to add");

        let specs = scheduler.propose(&(&state).into());

        // L1->L2 should still be proposed (no conflict)
        assert_eq!(specs.len(), 1);
        // File-level: picks first SST from L1
        assert_eq!(
            specs[0].sources()[0],
            SourceId::SortedRunSst {
                sr_id: 5,
                view_id: l1_first_view,
            }
        );
        assert_eq!(specs[0].destination(), 4);
    }

    // ---------------------------------------------------------------
    // Validation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_validate_accepts_valid_l0_compaction() {
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..4).map(|_| create_sst_view(1)).collect();
        let state = create_compactor_state(create_db_state(l0.clone(), vec![]));

        let spec = CompactionSpec::new(l0.iter().map(|v| SourceId::SstView(v.id)).collect(), 5);

        assert!(scheduler.validate(&(&state).into(), &spec).is_ok());
    }

    #[test]
    fn test_validate_accepts_valid_l0_with_l1() {
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..4).map(|_| create_sst_view(1)).collect();
        let l1 = create_sr(5, 100, 2);
        let state = create_compactor_state(create_db_state(l0.clone(), vec![l1]));

        let mut sources: Vec<SourceId> = l0.iter().map(|v| SourceId::SstView(v.id)).collect();
        sources.push(SourceId::SortedRun(5));
        let spec = CompactionSpec::new(sources, 5);

        assert!(scheduler.validate(&(&state).into(), &spec).is_ok());
    }

    #[test]
    fn test_validate_rejects_l0_with_wrong_destination() {
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..4).map(|_| create_sst_view(1)).collect();
        let state = create_compactor_state(create_db_state(l0.clone(), vec![]));

        let spec = CompactionSpec::new(
            l0.iter().map(|v| SourceId::SstView(v.id)).collect(),
            4, // Should be 5 (L1)
        );

        assert!(scheduler.validate(&(&state).into(), &spec).is_err());
    }

    #[test]
    fn test_validate_accepts_valid_level_compaction_whole_sr() {
        let scheduler = default_scheduler();
        let l1 = create_sr(5, 300, 2);
        let l2 = create_sr(4, 1000, 4);
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l2]));

        let spec = CompactionSpec::new(vec![SourceId::SortedRun(5), SourceId::SortedRun(4)], 4);

        assert!(scheduler.validate(&(&state).into(), &spec).is_ok());
    }

    #[test]
    fn test_validate_accepts_valid_file_level_compaction() {
        let scheduler = default_scheduler();
        let l1 = create_sr_with_ranges(5, 150, 2);
        let l2 = create_sr_with_ranges(4, 250, 4);
        let state = create_compactor_state(create_db_state(
            VecDeque::new(),
            vec![l1.clone(), l2.clone()],
        ));

        // File-level: pick one SST from L1, one from L2
        let spec = CompactionSpec::new(
            vec![
                SourceId::SortedRunSst {
                    sr_id: 5,
                    view_id: l1.sst_views[0].id,
                },
                SourceId::SortedRunSst {
                    sr_id: 4,
                    view_id: l2.sst_views[0].id,
                },
            ],
            4,
        );

        assert!(scheduler.validate(&(&state).into(), &spec).is_ok());
    }

    #[test]
    fn test_validate_rejects_non_adjacent_levels() {
        let scheduler = default_scheduler();
        let l1 = create_sr(5, 300, 2);
        let l3 = create_sr(3, 1000, 4);
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l3]));

        // L1 -> L3 (skipping L2) is invalid
        let spec = CompactionSpec::new(vec![SourceId::SortedRun(5), SourceId::SortedRun(3)], 3);

        assert!(scheduler.validate(&(&state).into(), &spec).is_err());
    }

    #[test]
    fn test_validate_rejects_wrong_destination_for_level() {
        let scheduler = default_scheduler();
        let l1 = create_sr(5, 300, 2);
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1]));

        // L1 -> destination 3 (should be 4 for L2)
        let spec = CompactionSpec::new(vec![SourceId::SortedRun(5)], 3);

        assert!(scheduler.validate(&(&state).into(), &spec).is_err());
    }

    // ---------------------------------------------------------------
    // Supplier and config tests
    // ---------------------------------------------------------------

    #[test]
    fn test_supplier_creates_scheduler_with_options() {
        let supplier = LeveledCompactionSchedulerSupplier::new();
        let scheduler_options = LeveledCompactionSchedulerOptions {
            level0_file_num_compaction_trigger: 2,
            max_bytes_for_level_base: 128,
            max_bytes_for_level_multiplier: 5.0,
            num_levels: 4,
        };
        let compactor_options = CompactorOptions {
            scheduler_options: scheduler_options.into(),
            ..CompactorOptions::default()
        };
        let scheduler = supplier.compaction_scheduler(&compactor_options);

        // With trigger=2 and 2 L0 files, compaction should be proposed
        let l0: VecDeque<_> = (0..2).map(|_| create_sst_view(1)).collect();
        let state = create_compactor_state(create_db_state(l0, vec![]));

        let specs = scheduler.propose(&(&state).into());
        assert_eq!(specs.len(), 1);
        // With num_levels=4, L1 SR id = 4-1-1 = 2
        assert_eq!(specs[0].destination(), 2);
    }

    #[test]
    fn test_config_options_roundtrip() {
        let options = LeveledCompactionSchedulerOptions {
            level0_file_num_compaction_trigger: 8,
            max_bytes_for_level_base: 512 * 1024 * 1024,
            max_bytes_for_level_multiplier: 8.0,
            num_levels: 5,
        };

        let map: std::collections::HashMap<String, String> = options.into();
        let roundtripped = LeveledCompactionSchedulerOptions::from(map);

        assert_eq!(roundtripped.level0_file_num_compaction_trigger, 8);
        assert_eq!(roundtripped.max_bytes_for_level_base, 512 * 1024 * 1024);
        assert_eq!(roundtripped.max_bytes_for_level_multiplier, 8.0);
        assert_eq!(roundtripped.num_levels, 5);
    }

    // ---------------------------------------------------------------
    // Edge case tests
    // ---------------------------------------------------------------

    #[test]
    fn test_empty_db_no_compaction() {
        let scheduler = default_scheduler();
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![]));

        let specs = scheduler.propose(&(&state).into());
        assert!(specs.is_empty());
    }

    #[test]
    fn test_single_sr_at_last_level_no_compaction() {
        let scheduler = default_scheduler();
        // Only L6 (SR 0) exists, it's the last level
        let l6 = create_sr(0, 100, 2);
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l6]));

        let specs = scheduler.propose(&(&state).into());
        assert!(specs.is_empty());
    }

    #[test]
    fn test_validate_rejects_l0_with_non_l1_sr() {
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..4).map(|_| create_sst_view(1)).collect();
        let l2 = create_sr_with_ranges(4, 50, 2);
        let state = create_compactor_state(create_db_state(l0.clone(), vec![l2]));

        // L0 + L2 SR (should only be L0 + L1)
        let mut sources: Vec<SourceId> = l0.iter().map(|v| SourceId::SstView(v.id)).collect();
        sources.push(SourceId::SortedRun(4)); // L2, not L1
        let spec = CompactionSpec::new(sources, 5);

        assert!(scheduler.validate(&(&state).into(), &spec).is_err());
    }
}
