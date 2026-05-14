use std::collections::HashSet;

use bytes::Bytes;
use log::warn;

use crate::compactor::{CompactionScheduler, CompactionSchedulerSupplier};
use crate::compactor_state::{CompactionSpec, SortedRunSstSelection, SourceId};
use crate::compactor_state_protocols::CompactorStateView;
use crate::config::{CompactorOptions, LeveledCompactionSchedulerOptions};
use crate::db_state::{SortedRun, SsTableView};
use crate::error::Error;
use crate::manifest::ManifestCore;
use crate::size_tiered_compaction::SizeTieredCompactionScheduler;

/// Leveled compaction scheduler.
///
/// Each level `k` in `1..num_levels` is identified by its sorted-run id:
/// L1 = SR(1), L2 = SR(2), … L_(num_levels-1) = SR(num_levels-1). L0 lives
/// outside `compacted` in `tree.l0` (flush layer). Because the manifest
/// invariant keeps `compacted` in strictly descending id order, the
/// deepest level sits at `compacted[0]` and L1 sits at `compacted.last()`.
///
/// L0 -> L1 is delegated to a size-tiered scheduler so that L0 flushing is
/// driven by file count thresholds. We then rewrite each emitted L0-only
/// spec to fold the existing L1 SR (if any) in as an additional source with
/// destination = SR(1), so L1 grows as a single SR rather than fragmenting
/// into a new SR every flush.
///
/// Ln -> Ln+1 (n >= 1) compactions are emitted as `LeveledCompactionSpec`s
/// with `sr_sst_selections` listing the SST views to consume from each side
/// (file-level / partial SR compaction). The destination is always SR(n+1).
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

    /// Returns the SR for level `level`, where `level == sr_id`. Returns
    /// `None` if the level is out of range or no SR with that id exists.
    fn sr_at_level<'a>(&self, db_state: &'a ManifestCore, level: usize) -> Option<&'a SortedRun> {
        if level < 1 || level >= self.options.num_levels {
            return None;
        }
        db_state
            .tree
            .compacted
            .iter()
            .find(|sr| sr.id as usize == level)
    }

    /// Returns the level of `sr_id` (which equals the id), or `None` if the
    /// id falls outside the valid level range.
    fn level_of_sr_id(&self, _db_state: &ManifestCore, sr_id: u32) -> Option<usize> {
        let level = sr_id as usize;
        (1..self.options.num_levels)
            .contains(&level)
            .then_some(level)
    }

    fn level_target_size(&self, level: usize) -> u64 {
        let base = self.options.max_bytes_for_level_base as f64;
        let multiplier = self.options.max_bytes_for_level_multiplier;
        (base * multiplier.powi((level - 1) as i32)) as u64
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

        // Iterate every level that could be a source (1..=num_levels-2 so
        // Ln+1 stays in range). Skip levels with no SR — the level
        // doesn't exist yet, nothing to cascade.
        for level in 1..=self.options.num_levels.saturating_sub(2) {
            let Some(src_sr) = self.sr_at_level(db_state, level) else {
                continue;
            };
            let target = self.level_target_size(level);
            if target == 0 {
                continue;
            }
            let actual = src_sr.estimate_size();
            let score = actual as f64 / target as f64;
            let qualifies =
                score > best_score || ((score - 1.0).abs() < f64::EPSILON && best_level.is_none());
            if !qualifies {
                continue;
            }
            let src = src_sr.id;
            let dst_id = (level + 1) as u32;
            if Self::is_sr_id_in_active_compaction(active, src)
                || Self::is_sr_id_in_active_compaction(active, dst_id)
            {
                continue;
            }
            best_score = score;
            best_level = Some(level);
        }

        best_level.and_then(|level| self.build_level_compaction(db_state, level))
    }

    /// Builds a file-level CompactionSpec for Ln -> Ln+1.
    ///
    /// Picks enough consecutive SSTs from the source SR (starting at the
    /// smallest key) to bring Ln back at or below its target after the
    /// cascade, finds overlapping SSTs in the destination SR, and emits a
    /// partial-SR spec. Picking only one SST per cascade would limit each
    /// propose() call to moving at most `max_sst_size` bytes downward,
    /// which makes the cascade saturate slowly and prevents deeper levels
    /// from accumulating to their (much larger) targets in any reasonable
    /// time.
    fn build_level_compaction(
        &self,
        db_state: &ManifestCore,
        level: usize,
    ) -> Option<CompactionSpec> {
        let src_sr = self.sr_at_level(db_state, level)?;
        let src_sr_id = src_sr.id;
        let dst_existing = self.sr_at_level(db_state, level + 1);

        // Pick enough SSTs to remove (src_total - target) bytes, capped to
        // the source's SSTs. Take at least one.
        let target = self.level_target_size(level);
        let src_total = src_sr.estimate_size();
        let to_remove = src_total.saturating_sub(target);
        let mut picked_size: u64 = 0;
        let mut picked_view_ids: Vec<ulid::Ulid> = Vec::new();
        for view in &src_sr.sst_views {
            if picked_size >= to_remove && !picked_view_ids.is_empty() {
                break;
            }
            picked_view_ids.push(view.id);
            picked_size += view.estimate_size();
        }

        // Skip no-op cascades: if the source has only one SST view AND there
        // is no destination SR yet, the "cascade" would just rename the SR
        // (move the lone SST to a new id with smaller value, drop the now-
        // empty source). That doesn't reduce L1's over-target condition —
        // the renamed SR becomes the new L1 — and would cycle indefinitely
        // until sr_id hits 0. Wait for more data to accumulate so the
        // source has at least 2 SSTs and the cascade actually splits it.
        if dst_existing.is_none() && picked_view_ids.len() == src_sr.sst_views.len() {
            return None;
        }

        let mut sources = vec![SourceId::SortedRun(src_sr_id)];
        let mut selections = vec![SortedRunSstSelection {
            sr_id: src_sr_id,
            view_ids: picked_view_ids.clone(),
        }];

        let dst_sr_id = (level + 1) as u32;
        if let Some(dst_sr) = dst_existing {
            // Find the key range spanned by all picked SSTs.
            let picked_set: HashSet<ulid::Ulid> = picked_view_ids.iter().copied().collect();
            let picked_views: Vec<&SsTableView> = src_sr
                .sst_views
                .iter()
                .filter(|v| picked_set.contains(&v.id))
                .collect();
            let start_key = picked_views.first()?.compacted_effective_start_key();
            let last_picked = picked_views.last()?;
            let end_key = last_picked
                .sst
                .info
                .last_entry
                .as_ref()
                .unwrap_or(start_key);
            let overlapping = Self::find_overlapping_sst_ids(dst_sr, start_key, end_key);
            if !overlapping.is_empty() {
                sources.push(SourceId::SortedRun(dst_sr.id));
                selections.push(SortedRunSstSelection {
                    sr_id: dst_sr.id,
                    view_ids: overlapping,
                });
            }
        }

        Some(CompactionSpec::leveled(sources, dst_sr_id, selections))
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

    /// True if every source in `spec` is an L0 SST view (no SR sources).
    fn is_pure_l0_spec(spec: &CompactionSpec) -> bool {
        !spec.sources().is_empty()
            && spec
                .sources()
                .iter()
                .all(|s| matches!(s, SourceId::SstView(_)))
    }

    /// Rewrites a pure-L0 spec produced by the size-tiered delegate so that
    /// the existing L1 SR (if any, in the root tree, and not in active
    /// compaction) is folded in as an additional source and the destination
    /// is SR(1). This keeps L1 as a single growing SR instead of
    /// fragmenting into a new SR each flush.
    ///
    /// If no L1 exists yet (bootstrap case), rewrites the destination to
    /// `1` so that each level k uses sr_id = k.
    ///
    /// Returns `None` when L1 exists but is busy in an active compaction
    /// (typically an L1 -> L2 cascade). Emitting the size-tiered spec
    /// unchanged in that case would route the new data to a fresh max+1
    /// id (e.g., 7 with num_levels=7), creating a "shadow L1" SR that
    /// every cascade thereafter would treat as a separate level. Waiting
    /// for the cascade to finish keeps a single L1.
    fn fold_l1_into_l0_spec(
        &self,
        db_state: &ManifestCore,
        active: &[&CompactionSpec],
        spec: CompactionSpec,
    ) -> Option<CompactionSpec> {
        if !spec.segment().is_empty() {
            return Some(spec);
        }
        if let Some(l1) = self.sr_at_level(db_state, 1) {
            if Self::is_sr_id_in_active_compaction(active, l1.id) {
                return None;
            }
            let mut sources = spec.sources().to_vec();
            sources.push(SourceId::SortedRun(l1.id));
            return Some(CompactionSpec::for_segment(
                spec.segment().clone(),
                sources,
                l1.id,
            ));
        }

        // Bootstrap: no L1 exists. Always target sr_id 1 so level == id.
        Some(CompactionSpec::for_segment(
            spec.segment().clone(),
            spec.sources().to_vec(),
            1,
        ))
    }
}

impl CompactionScheduler for LeveledCompactionScheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let db_state = state.manifest().core();

        // L0 -> L1: delegate to size-tiered. Accept only specs whose sources
        // are entirely L0 SST views (size-tiered may also propose SR-only
        // specs, but Ln -> Ln+1 selection is owned by us).
        let l0_specs: Vec<CompactionSpec> = self
            .l0_scheduler
            .propose(state)
            .into_iter()
            .filter(Self::is_pure_l0_spec)
            .collect();

        let pre_active: Vec<&CompactionSpec> = state
            .compactions()
            .into_iter()
            .flat_map(|c| c.core().recent_compactions())
            .filter(|c| c.active())
            .map(|c| c.spec())
            .collect();

        // Pick the cascade (Ln -> Ln+1) BEFORE folding L1 into L0 specs.
        // Order matters: the fold-in claims L1 as destination, which would
        // otherwise mask L1 from `is_sr_id_in_active_compaction` and block
        // the cascade indefinitely while L0 keeps firing. Picking the
        // cascade first means a busy L1 (drain into L2 in progress) will
        // suppress the fold-in for one round, which is the right behavior.
        let cascade = self.pick_level_compaction(db_state, &pre_active);
        let active_after_cascade: Vec<&CompactionSpec> =
            pre_active.iter().copied().chain(cascade.iter()).collect();

        // Fold the existing L1 SR into each L0 spec so L1 grows as a single
        // SR. Dropped entirely when L1 is busy (e.g. just-picked L1->L2
        // cascade): emitting the size-tiered fall-through spec would land
        // the new data in a fresh max+1 SR id, creating a "shadow L1" that
        // demotes the real L1 to L2 under position-based mapping and
        // prevents deeper levels from growing 10x.
        let l0_compactions: Vec<CompactionSpec> = l0_specs
            .into_iter()
            .filter_map(|spec| self.fold_l1_into_l0_spec(db_state, &active_after_cascade, spec))
            .collect();

        let mut compactions = Vec::new();
        compactions.extend(cascade);
        compactions.extend(l0_compactions);
        compactions
    }

    fn validate(&self, state: &CompactorStateView, spec: &CompactionSpec) -> Result<(), Error> {
        let db_state = state.manifest().core();

        if spec.has_l0_sources() {
            // Leveled L0 spec: all L0 SstView sources + optionally exactly
            // one SortedRun source = L1 (sr_id 1). Delegating to size-tiered
            // would reject this when deeper levels exist, because its
            // "strictly consecutive in [L0..., SR_highest..SR_0]" rule
            // requires every existing SR to be a source. The leveled
            // contract only requires L1 to participate in the fold-in.
            let sr_sources: Vec<u32> = spec
                .sources()
                .iter()
                .filter_map(|s| s.maybe_unwrap_sorted_run())
                .collect();
            match sr_sources.as_slice() {
                [] => {
                    // Pure L0 -> L1 bootstrap. Dest must be 1.
                    if spec.destination() != Some(1) {
                        return Err(Error::invalid(
                            "L0 bootstrap spec must target SR(1)".to_string(),
                        ));
                    }
                }
                [l1_id] => {
                    // Fold-in. The lone SR source must be L1 (sr_id 1) and
                    // dest must equal it.
                    if *l1_id != 1 {
                        return Err(Error::invalid(format!(
                            "L0 fold-in spec must reference L1=SR(1), got SR({})",
                            l1_id
                        )));
                    }
                    if spec.destination() != Some(1) {
                        return Err(Error::invalid(
                            "L0 fold-in destination must be SR(1)".to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(Error::invalid(
                        "L0 spec must have at most one SR source (L1 fold-in)".to_string(),
                    ));
                }
            }
            // Defer to size-tiered for the L0 ordering check by feeding it a
            // synthetic L0-only spec (no SR sources). Skip this if there's
            // no L1 to remove from sources.
            let l0_only_sources: Vec<SourceId> = spec
                .sources()
                .iter()
                .copied()
                .filter(|s| s.maybe_unwrap_sst_view().is_some())
                .collect();
            let l0_only_dest = spec.destination().unwrap_or(0);
            let l0_only_spec =
                CompactionSpec::for_segment(spec.segment().clone(), l0_only_sources, l0_only_dest);
            return self.l0_scheduler.validate(state, &l0_only_spec);
        }
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

        let source_level = match self.level_of_sr_id(db_state, unique_sr_ids[0]) {
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

        // Destination must equal dest_level (level == sr_id).
        if spec.destination() != Some(dest_level as u32) {
            return Err(Error::invalid(
                "destination does not match expected next level".to_string(),
            ));
        }

        if unique_sr_ids.len() == 2
            && self.level_of_sr_id(db_state, unique_sr_ids[1]) != Some(dest_level)
        {
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
        // L1 has sr_id == 1 in the level == id mapping.
        let l1 = create_sr_with_ranges(1, 150, 2);
        let l1_first_id = l1.sst_views[0].id;
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l1]));

        let specs = scheduler.propose(&(&state).into());
        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        assert!(spec.is_partial());
        assert_eq!(spec.destination(), Some(2));
        let sel = spec.sr_sst_selections();
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].sr_id, 1);
        assert_eq!(sel[0].view_ids, vec![l1_first_id]);
    }

    #[test]
    fn test_should_compact_l1_into_existing_l2_with_overlap() {
        let scheduler = default_scheduler();
        let l1 = create_sr_with_ranges(1, 150, 2);
        let l1_first_id = l1.sst_views[0].id;
        let l2 = create_sr_with_ranges(2, 250, 4);
        let l2_first_id = l2.sst_views[0].id;
        // compacted is kept in strictly descending id order.
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l2, l1]));

        let specs = scheduler.propose(&(&state).into());
        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        assert_eq!(spec.destination(), Some(2));
        let sel = spec.sr_sst_selections();
        assert_eq!(sel.len(), 2);
        assert_eq!(sel[0].sr_id, 1);
        assert_eq!(sel[0].view_ids, vec![l1_first_id]);
        assert_eq!(sel[1].sr_id, 2);
        assert_eq!(sel[1].view_ids, vec![l2_first_id]);
    }

    #[test]
    fn test_should_not_compact_last_level() {
        // L_(num_levels-1) is the deepest level and never picked as a source.
        let scheduler = default_scheduler();
        let deepest = create_sr(6, 1_000_000, 10); // num_levels=7 -> deepest = 6
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![deepest]));
        let specs = scheduler.propose(&(&state).into());
        assert!(specs.is_empty());
    }

    #[test]
    fn test_should_skip_level_if_destination_busy() {
        let scheduler = default_scheduler();
        let l1 = create_sr_with_ranges(1, 150, 2);
        let l2 = create_sr_with_ranges(2, 3000, 4);
        let l3 = create_sr_with_ranges(3, 30000, 8);
        // descending id order
        let mut state = create_compactor_state(create_db_state(VecDeque::new(), vec![l3, l2, l1]));

        let rand = Arc::new(DbRand::default());
        let clock = Arc::new(DefaultSystemClock::new());
        let id = rand.rng().gen_ulid(clock.as_ref());
        // A merge involving SR(2), SR(3), and SR(4) is in flight: it claims
        // SR(2) and SR(3) as sources and SR(4) as its destination. This
        // simultaneously blocks: L1->L2 (dst=2 busy as a source),
        // L2->L3 (src=2 busy), and L3->L4 (src=3 busy, dst=4 busy).
        let running = CompactionSpec::new(vec![SourceId::SortedRun(2), SourceId::SortedRun(3)], 4);
        state.add_compaction(Compaction::new(id, running)).unwrap();

        let specs = scheduler.propose(&(&state).into());
        assert!(specs.is_empty());
    }

    #[test]
    fn test_validate_accepts_partial_l1_to_l2() {
        let scheduler = default_scheduler();
        let l1 = create_sr_with_ranges(1, 150, 2);
        let l2 = create_sr_with_ranges(2, 250, 4);
        let l1_id = l1.sst_views[0].id;
        let l2_id = l2.sst_views[0].id;
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l2, l1]));

        let spec = CompactionSpec::leveled(
            vec![SourceId::SortedRun(1), SourceId::SortedRun(2)],
            2,
            vec![
                SortedRunSstSelection {
                    sr_id: 1,
                    view_ids: vec![l1_id],
                },
                SortedRunSstSelection {
                    sr_id: 2,
                    view_ids: vec![l2_id],
                },
            ],
        );
        assert!(scheduler.validate(&(&state).into(), &spec).is_ok());
    }

    #[test]
    fn test_validate_rejects_non_adjacent_levels() {
        // compacted = [SR(3), SR(2), SR(1)] under level == sr_id mapping.
        // A spec from L1 (SR(1)) that targets L3 (SR(3)) skips L2 and is
        // invalid — destination must equal source_level + 1.
        let scheduler = default_scheduler();
        let l1 = create_sr(1, 300, 2);
        let l2 = create_sr(2, 1000, 4);
        let l3 = create_sr(3, 5000, 4);
        let state = create_compactor_state(create_db_state(VecDeque::new(), vec![l3, l2, l1]));
        let spec = CompactionSpec::leveled(vec![SourceId::SortedRun(1)], 3, vec![]);
        assert!(scheduler.validate(&(&state).into(), &spec).is_err());
    }

    #[test]
    fn test_l0_to_l1_folds_existing_l1_into_spec() {
        // When L1 exists and is not in active compaction, an L0->L1 spec
        // should fold the L1 SR in as a source and target L1's id (= 1).
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..4).map(|_| create_sst_view(1)).collect();
        let l1 = create_sr(1, 100, 2);
        let state = create_compactor_state(create_db_state(l0, vec![l1]));

        let specs = scheduler.propose(&(&state).into());
        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        assert!(spec.has_l0_sources());
        assert!(spec.has_sr_sources());
        assert_eq!(spec.destination(), Some(1));
        let sr_sources: Vec<_> = spec
            .sources()
            .iter()
            .filter_map(|s| s.maybe_unwrap_sorted_run())
            .collect();
        assert_eq!(sr_sources, vec![1]);
    }

    #[test]
    fn test_l0_to_l1_drops_spec_when_l1_busy() {
        // If L1 is currently a source/destination of an active compaction,
        // the L0 fold-in must be dropped entirely. Emitting a fall-through
        // spec with size-tiered's max+1 dest would create a "shadow L1"
        // SR that the leveled mapping has no proper home for.
        let scheduler = default_scheduler();
        let l0: VecDeque<_> = (0..4).map(|_| create_sst_view(1)).collect();
        let l1 = create_sr_with_ranges(1, 200, 2);
        let l2 = create_sr_with_ranges(2, 100, 2);
        let mut state = create_compactor_state(create_db_state(l0, vec![l2, l1]));

        let rand = Arc::new(DbRand::default());
        let clock = Arc::new(DefaultSystemClock::new());
        let id = rand.rng().gen_ulid(clock.as_ref());
        // Pretend an L1->L2 compaction is already running.
        let running = CompactionSpec::leveled(
            vec![SourceId::SortedRun(1), SourceId::SortedRun(2)],
            2,
            vec![SortedRunSstSelection {
                sr_id: 1,
                view_ids: vec![],
            }],
        );
        state.add_compaction(Compaction::new(id, running)).unwrap();

        let specs = scheduler.propose(&(&state).into());
        let l0_specs: Vec<_> = specs.iter().filter(|s| s.has_l0_sources()).collect();
        assert!(
            l0_specs.is_empty(),
            "expected no L0 spec while L1 is busy, got {:?}",
            l0_specs.iter().map(|s| s.destination()).collect::<Vec<_>>(),
        );
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
