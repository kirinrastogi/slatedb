use std::collections::HashSet;

use bytes::Bytes;
use log::warn;

use crate::compactor::{CompactionScheduler, CompactionSchedulerSupplier};
use crate::compactor_state::{CompactionSpec, SortedRunSstSelection, SourceId};
use crate::compactor_state_protocols::CompactorStateView;
use crate::config::{CompactorOptions, LeveledCompactionSchedulerOptions};
use crate::db_state::SortedRun;
use crate::error::Error;
use crate::manifest::ManifestCore;
use crate::size_tiered_compaction::SizeTieredCompactionScheduler;

/// Leveled compaction scheduler.
///
/// Levels L0..L(num_levels-1). Each Ln (n>=1) is a single sorted run with id
/// `num_levels - 1 - n`, so L1 has the highest SR id and the deepest level
/// holds SR 0.
///
/// L0 -> L1 is delegated to a size-tiered scheduler so that L0 flushing is
/// driven by file count thresholds, matching the existing behavior. Ln -> Ln+1
/// (n >= 1) compactions are emitted as `LeveledCompactionSpec`s with
/// `sr_sst_selections` listing the SST views to consume from each side
/// (file-level / partial SR compaction).
pub(crate) struct LeveledCompactionScheduler {
    options: LeveledCompactionSchedulerOptions,
    l0_scheduler: SizeTieredCompactionScheduler,
}

impl LeveledCompactionScheduler {
    pub(crate) fn new(
        options: LeveledCompactionSchedulerOptions,
        l0_scheduler: SizeTieredCompactionScheduler,
    ) -> Self {
        assert!(
            options.num_levels >= 2,
            "num_levels must be at least 2, got {}",
            options.num_levels
        );
        Self {
            options,
            l0_scheduler,
        }
    }

    /// Maps a logical level (1..num_levels) to a sorted run id.
    fn sr_id_for_level(&self, level: usize) -> u32 {
        assert!(
            level >= 1 && level < self.options.num_levels,
            "level {} out of range [1, {})",
            level,
            self.options.num_levels
        );
        (self.options.num_levels - 1 - level) as u32
    }

    /// Maps an SR id back to a logical level, or None if it doesn't fit.
    fn level_for_sr_id(&self, sr_id: u32) -> Option<usize> {
        let level = self
            .options
            .num_levels
            .checked_sub(1)?
            .checked_sub(sr_id as usize)?;
        if (1..self.options.num_levels).contains(&level) {
            Some(level)
        } else {
            None
        }
    }

    fn level_target_size(&self, level: usize) -> u64 {
        let base = self.options.max_bytes_for_level_base as f64;
        let multiplier = self.options.max_bytes_for_level_multiplier;
        (base * multiplier.powi((level - 1) as i32)) as u64
    }

    fn find_sr_for_level<'a>(
        &self,
        db_state: &'a ManifestCore,
        level: usize,
    ) -> Option<&'a SortedRun> {
        let sr_id = self.sr_id_for_level(level);
        db_state.tree.compacted.iter().find(|sr| sr.id == sr_id)
    }

    fn level_size(&self, db_state: &ManifestCore, level: usize) -> u64 {
        self.find_sr_for_level(db_state, level)
            .map_or(0, |sr| sr.estimate_size())
    }

    fn is_sr_id_in_active_compaction(active: &[&CompactionSpec], sr_id: u32) -> bool {
        active.iter().any(|spec| {
            spec.destination() == Some(sr_id)
                || spec.sources().iter().any(|s| match s {
                    SourceId::SortedRun(id) => *id == sr_id,
                    SourceId::SstView(_) => false,
                })
                || spec.sr_sst_selections().iter().any(|s| s.sr_id == sr_id)
        })
    }

    fn pick_level_compaction(
        &self,
        db_state: &ManifestCore,
        active: &[&CompactionSpec],
    ) -> Option<CompactionSpec> {
        let mut best_level: Option<usize> = None;
        let mut best_score: f64 = 1.0;

        for level in 1..=(self.options.num_levels - 2) {
            let target = self.level_target_size(level);
            if target == 0 {
                continue;
            }
            let actual = self.level_size(db_state, level);
            let score = actual as f64 / target as f64;
            if score >= 1.0 && score > best_score {
                let src = self.sr_id_for_level(level);
                let dst = self.sr_id_for_level(level + 1);
                if !Self::is_sr_id_in_active_compaction(active, src)
                    && !Self::is_sr_id_in_active_compaction(active, dst)
                {
                    best_score = score;
                    best_level = Some(level);
                }
            } else if (score - 1.0).abs() < f64::EPSILON && best_level.is_none() {
                // edge case: score == 1.0 exactly and no better candidate yet
                let src = self.sr_id_for_level(level);
                let dst = self.sr_id_for_level(level + 1);
                if !Self::is_sr_id_in_active_compaction(active, src)
                    && !Self::is_sr_id_in_active_compaction(active, dst)
                {
                    best_score = score;
                    best_level = Some(level);
                }
            }
        }

        best_level.map(|level| self.build_level_compaction(db_state, level))
    }

    /// Builds a file-level CompactionSpec for Ln -> Ln+1.
    ///
    /// Picks the first SST from the source SR (smallest key range), finds
    /// overlapping SSTs in the destination SR, and emits a partial-SR spec.
    fn build_level_compaction(&self, db_state: &ManifestCore, level: usize) -> CompactionSpec {
        let src_sr_id = self.sr_id_for_level(level);
        let dst_sr_id = self.sr_id_for_level(level + 1);

        let src_sr = self
            .find_sr_for_level(db_state, level)
            .expect("source level SR must exist");
        let picked = &src_sr.sst_views[0];

        let mut sources = vec![SourceId::SortedRun(src_sr_id)];
        let mut selections = vec![SortedRunSstSelection {
            sr_id: src_sr_id,
            view_ids: vec![picked.id],
        }];

        if let Some(dst_sr) = self.find_sr_for_level(db_state, level + 1) {
            let start_key = picked.compacted_effective_start_key();
            let end_key = picked.sst.info.last_entry.as_ref().unwrap_or(start_key);
            let overlapping = Self::find_overlapping_sst_ids(dst_sr, start_key, end_key);
            if !overlapping.is_empty() {
                sources.push(SourceId::SortedRun(dst_sr_id));
                selections.push(SortedRunSstSelection {
                    sr_id: dst_sr_id,
                    view_ids: overlapping,
                });
            }
        }

        CompactionSpec::leveled(sources, dst_sr_id, selections)
    }

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

impl CompactionScheduler for LeveledCompactionScheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        // L0 -> L1: delegate to size-tiered. Filter to specs that include L0
        // SSTs only; the size-tiered scheduler may also propose SR-only specs,
        // but Ln -> Ln+1 selection is owned by us.
        let mut compactions: Vec<CompactionSpec> = self
            .l0_scheduler
            .propose(state)
            .into_iter()
            .filter(|spec| spec.has_l0_sources())
            .collect();

        let active: Vec<&CompactionSpec> = state
            .compactions()
            .into_iter()
            .flat_map(|c| c.core().recent_compactions())
            .filter(|c| c.active())
            .map(|c| c.spec())
            .chain(compactions.iter())
            .collect();

        if let Some(spec) = self.pick_level_compaction(state.manifest().core(), &active) {
            compactions.push(spec);
        }

        compactions
    }

    fn validate(&self, state: &CompactorStateView, spec: &CompactionSpec) -> Result<(), Error> {
        if spec.has_l0_sources() {
            return self.l0_scheduler.validate(state, spec);
        }

        let db_state = state.manifest().core();
        let unique_sr_ids: Vec<u32> = {
            let mut seen = HashSet::new();
            spec.sources()
                .iter()
                .filter_map(|s| s.maybe_unwrap_sorted_run())
                .filter(|id| seen.insert(*id))
                .collect()
        };

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
                return Err(Error::invalid(
                    "SR source does not map to a valid level".to_string(),
                ));
            }
        };

        let dest_level = source_level + 1;
        if dest_level >= self.options.num_levels {
            return Err(Error::invalid("source level has no next level".to_string()));
        }

        let expected_dest = self.sr_id_for_level(dest_level);
        if spec.destination() != Some(expected_dest) {
            return Err(Error::invalid(
                "destination does not match expected next level".to_string(),
            ));
        }

        if unique_sr_ids.len() == 2 && self.level_for_sr_id(unique_sr_ids[1]) != Some(dest_level) {
            return Err(Error::invalid(
                "second SR source must be the destination level".to_string(),
            ));
        }

        let sr_ids: HashSet<u32> = db_state.tree.compacted.iter().map(|sr| sr.id).collect();
        for &sr_id in &unique_sr_ids {
            if !sr_ids.contains(&sr_id) {
                return Err(Error::invalid(format!("SR {} not found in state", sr_id)));
            }
        }

        Ok(())
    }
}

/// Supplier that creates a [`LeveledCompactionScheduler`].
///
/// The L0 -> L1 component uses size-tiered logic and reads its options from
/// the same `scheduler_options` map, so users can tune both the leveled and
/// the underlying size-tiered behavior through one config block.
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
        let leveled_options =
            LeveledCompactionSchedulerOptions::from(&compactor_options.scheduler_options);
        let size_tiered_options = crate::config::SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: leveled_options.level0_file_num_compaction_trigger,
            ..Default::default()
        };
        let l0_scheduler = SizeTieredCompactionScheduler::new(
            size_tiered_options,
            compactor_options.max_concurrent_compactions,
        );
        Box::new(LeveledCompactionScheduler::new(
            leveled_options,
            l0_scheduler,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactor_state::{Compaction, Compactions, CompactorState};
    use crate::config::SizeTieredCompactionSchedulerOptions;
    use crate::db_state::{SsTableHandle, SsTableId, SsTableInfo, SsTableView};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::store::test_utils::new_dirty_manifest;
    use crate::manifest::{LsmTreeState, ManifestCore};
    use crate::seq_tracker::SequenceTracker;
    use crate::utils::IdGenerator;
    use crate::DbRand;
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_txn_obj::test_utils::new_dirty_object;
    use std::collections::VecDeque;
    use std::sync::Arc;

    fn default_scheduler() -> LeveledCompactionScheduler {
        let options = LeveledCompactionSchedulerOptions {
            level0_file_num_compaction_trigger: 4,
            max_bytes_for_level_base: 256,
            max_bytes_for_level_multiplier: 10.0,
            num_levels: 7,
        };
        let st_options = SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 4,
            max_compaction_sources: 8,
            include_size_threshold: 4.0,
        };
        LeveledCompactionScheduler::new(options, SizeTieredCompactionScheduler::new(st_options, 4))
    }

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

    fn create_sst_view_with_range(size: u64, first: &[u8], last: &[u8]) -> SsTableView {
        let info = SsTableInfo {
            first_entry: Some(Bytes::copy_from_slice(first)),
            last_entry: Some(Bytes::copy_from_slice(last)),
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

    fn create_sr(id: u32, total: u64, n: usize) -> SortedRun {
        let sst_size = total / n as u64;
        SortedRun {
            id,
            sst_views: (0..n).map(|_| create_sst_view(sst_size)).collect(),
        }
    }

    fn create_sr_with_ranges(id: u32, sst_size: u64, n: usize) -> SortedRun {
        SortedRun {
            id,
            sst_views: (0..n)
                .map(|i| {
                    let first = format!("{:02}_00", i);
                    let last = format!("{:02}_ff", i);
                    create_sst_view_with_range(sst_size, first.as_bytes(), last.as_bytes())
                })
                .collect(),
        }
    }

    fn create_db_state(l0: VecDeque<SsTableView>, srs: Vec<SortedRun>) -> ManifestCore {
        ManifestCore {
            initialized: true,
            tree: LsmTreeState {
                last_compacted_l0_sst_view_id: None,
                last_compacted_l0_sst_id: None,
                l0,
                compacted: srs,
            },
            segments: vec![],
            segment_extractor_name: None,
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

    #[test]
    fn test_should_compact_l0_to_l1_via_size_tiered() {
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..4).map(|_| create_sst_view(1)).collect();
        let state = create_compactor_state(create_db_state(l0.clone(), vec![]));

        let specs = scheduler.propose(&(&state).into());
        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        assert!(spec.has_l0_sources());
        assert!(!spec.is_partial());
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
    fn test_should_compact_l1_to_l2_when_over_target() {
        let scheduler = default_scheduler();
        let l1 = create_sr_with_ranges(5, 150, 2);
        let l1_first_id = l1.sst_views[0].id;
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1]));

        let specs = scheduler.propose(&(&state).into());
        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        assert!(spec.is_partial());
        assert_eq!(spec.destination(), Some(4));
        let sel = spec.sr_sst_selections();
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].sr_id, 5);
        assert_eq!(sel[0].view_ids, vec![l1_first_id]);
    }

    #[test]
    fn test_should_compact_l1_into_existing_l2_with_overlap() {
        let scheduler = default_scheduler();
        let l1 = create_sr_with_ranges(5, 150, 2);
        let l1_first_id = l1.sst_views[0].id;
        let l2 = create_sr_with_ranges(4, 250, 4);
        let l2_first_id = l2.sst_views[0].id;
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l2]));

        let specs = scheduler.propose(&(&state).into());
        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        assert_eq!(spec.destination(), Some(4));
        let sel = spec.sr_sst_selections();
        assert_eq!(sel.len(), 2);
        assert_eq!(sel[0].view_ids, vec![l1_first_id]);
        assert_eq!(sel[1].view_ids, vec![l2_first_id]);
    }

    #[test]
    fn test_should_not_compact_last_level() {
        let scheduler = default_scheduler();
        let l6 = create_sr(0, 1_000_000, 10);
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l6]));
        let specs = scheduler.propose(&(&state).into());
        assert!(specs.is_empty());
    }

    #[test]
    fn test_should_skip_level_if_destination_busy() {
        let scheduler = default_scheduler();
        let l1 = create_sr_with_ranges(5, 150, 2);
        let l2 = create_sr_with_ranges(4, 3000, 4);
        let l3 = create_sr_with_ranges(3, 30000, 8);
        let mut state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l2, l3]));

        let rand = Arc::new(DbRand::default());
        let clock = Arc::new(DefaultSystemClock::new());
        let id = rand.rng().gen_ulid(clock.as_ref());
        let running = CompactionSpec::new(vec![SourceId::SortedRun(4), SourceId::SortedRun(3)], 3);
        state.add_compaction(Compaction::new(id, running)).unwrap();

        let specs = scheduler.propose(&(&state).into());
        assert!(specs.is_empty());
    }

    #[test]
    fn test_validate_accepts_partial_l1_to_l2() {
        let scheduler = default_scheduler();
        let l1 = create_sr_with_ranges(5, 150, 2);
        let l2 = create_sr_with_ranges(4, 250, 4);
        let l1_id = l1.sst_views[0].id;
        let l2_id = l2.sst_views[0].id;
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l2]));

        let spec = CompactionSpec::leveled(
            vec![SourceId::SortedRun(5), SourceId::SortedRun(4)],
            4,
            vec![
                SortedRunSstSelection {
                    sr_id: 5,
                    view_ids: vec![l1_id],
                },
                SortedRunSstSelection {
                    sr_id: 4,
                    view_ids: vec![l2_id],
                },
            ],
        );
        assert!(scheduler.validate(&(&state).into(), &spec).is_ok());
    }

    #[test]
    fn test_validate_rejects_non_adjacent_levels() {
        let scheduler = default_scheduler();
        let l1 = create_sr(5, 300, 2);
        let l3 = create_sr(3, 1000, 4);
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1, l3]));
        let spec = CompactionSpec::leveled(
            vec![SourceId::SortedRun(5), SourceId::SortedRun(3)],
            3,
            vec![],
        );
        assert!(scheduler.validate(&(&state).into(), &spec).is_err());
    }

    #[test]
    fn test_supplier_creates_scheduler() {
        let supplier = LeveledCompactionSchedulerSupplier::new();
        let opts = LeveledCompactionSchedulerOptions {
            level0_file_num_compaction_trigger: 2,
            max_bytes_for_level_base: 128,
            max_bytes_for_level_multiplier: 5.0,
            num_levels: 4,
        };
        let compactor_options = CompactorOptions {
            scheduler_options: opts.into(),
            ..CompactorOptions::default()
        };
        let scheduler = supplier.compaction_scheduler(&compactor_options);
        let l0: VecDeque<_> = (0..2).map(|_| create_sst_view(1)).collect();
        let state = create_compactor_state(create_db_state(l0, vec![]));
        let specs = scheduler.propose(&(&state).into());
        assert_eq!(specs.len(), 1);
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
        let rt = LeveledCompactionSchedulerOptions::from(map);
        assert_eq!(rt.level0_file_num_compaction_trigger, 8);
        assert_eq!(rt.max_bytes_for_level_base, 512 * 1024 * 1024);
        assert_eq!(rt.max_bytes_for_level_multiplier, 8.0);
        assert_eq!(rt.num_levels, 5);
    }
}
