# SlateDB RFC: Leveled Compaction (Hybrid Size-Tiered L0→L1)

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Background](#background)
- [Design](#design)
   - [Level Mapping](#level-mapping)
   - [L0 → L1: Size-Tiered](#l0--l1-size-tiered)
   - [Ln → Ln+1: Leveled with Partial-SR Compactions](#ln--ln1-leveled-with-partial-sr-compactions)
   - [Level Selection (Score)](#level-selection-score)
   - [Worked Example](#worked-example)
   - [Validation](#validation)
   - [Backpressure](#backpressure)
- [Composition with Segmented Compaction](#composition-with-segmented-compaction)
   - [Per-Segment Leveled Hierarchy](#per-segment-leveled-hierarchy)
   - [Cross-Segment Prioritization](#cross-segment-prioritization)
   - [Drain Interaction](#drain-interaction)
   - [Level Identity Under a Global SR Counter](#level-identity-under-a-global-sr-counter)
- [Read Path](#read-path)
- [Configuration](#configuration)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
   - [Performance & Cost](#performance--cost)
   - [Observability](#observability)
   - [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)

<!-- TOC end -->

Status: Draft

Authors:

* [Kirin Rastogi](https://github.com/kirinrastogi)

## Summary

This RFC specifies a leveled compaction strategy for SlateDB with a hybrid
optimization: **size-tiered L0→L1, leveled L1→Lmax**. The goal is to give
SlateDB a read-amplification profile closer to traditional leveled stores
(O(log N) sorted runs to consult on a point read) without paying leveled's
worst-case L0→L1 rewrite cost on every memtable flush.

The hybrid scheduler delegates L0→L1 work to the existing
`SizeTieredCompactionScheduler` and emits leveled, partial-SR
(file-level) compactions for Ln→Ln+1 (n ≥ 1) using the existing
`SortedRunSstSelection` primitive — no manifest, WAL, or `CompactionSpec`
schema changes are required. The design also describes how leveled
compaction composes with the per-segment LSM trees introduced in
[RFC 0024](./0024-segment-oriented-compaction.md): each segment runs
its own independent leveled hierarchy.

A working implementation already exists at
`slatedb/src/leveled_compaction.rs` (commit `1863810`); this RFC formalizes
the design, documents the hybrid choice, and stakes out segment composition
explicitly.

## Motivation

[RFC 0002](./0002-compaction.md) ships a tiered compaction scheduler. Tiered
keeps the total number of sorted runs bounded by O(T·log N) where T is the
per-level fanout. That's good for write-heavy workloads — write amplification
stays low — but read amplification is proportional to the total number of
runs, which translates directly into bloom-filter false positives, index
working-set pressure, and (worst case) extra GETs on cache misses.

For read-heavy workloads where a small working set sits in the upper levels,
leveled compaction is a better fit: each level (L1 onward) is a single sorted
run, so the total number of runs to consult is O(log N) and a point read
typically resolves with a single SST read once the deepest level holding the
key is found. The cost is write amplification, which goes up by roughly a
factor of T per level.

Pure leveled has one well-known pathology: every L0 flush, no matter how
small, rewrites all of L1 that overlaps with it. SlateDB flushes small L0
SSTs frequently (driven by `l0_sst_size_bytes` and the WAL flush cadence),
so naïvely promoting to leveled at L0→L1 would multiply write traffic by a
large factor and saturate the compactor's network budget. The fix used by
RocksDB (and others) is to keep L0→L1 size-tiered — let small L0s coalesce
before paying the leveled merge cost — and apply leveled rules from L1
downward. That's the hybrid this RFC specifies.

[Issue #1598](https://github.com/slatedb/slatedb/issues/1598) originally
scoped hybrid compaction *out* of the initial leveled work. After
implementing the leveled scheduler, the hybrid optimization turns out to be
a small additional step (delegate to the existing size-tiered scheduler for
L0 sources) and is in scope for this RFC.

## Goals

- A pluggable `LeveledCompactionScheduler` selectable via the existing
  `CompactionSchedulerSupplier` mechanism.
- Hybrid behavior: size-tiered L0→L1, leveled L1→Lmax.
- File-level (partial-SR) Ln→Ln+1 compactions to bound the per-compaction
  rewrite cost.
- Reuse the existing `CompactionSpec` and `SortedRunSstSelection` types — no
  manifest, WAL, or compaction-spec format changes.
- Compose with [RFC 0024](./0024-segment-oriented-compaction.md): each
  segment runs its own leveled hierarchy.

## Non-Goals

- **Dynamic level sizing.** Levels use static `target_size = base · multiplier^(n-1)`
  thresholds. Adapting target sizes to actual db size is deferred.
- **Subcompactions / parallel work within one level pair.** The single-
  destination-SR contract of `CompactionSpec` is preserved; parallelism comes
  from independent segments and from disjoint level pairs, not from splitting
  a single Ln→Ln+1 job.
- **Cross-segment leveled coordination.** Each segment's hierarchy is
  independent. There is no rebalancing that moves data across segments.
- **Multi-output `CompactionSpec`.** This RFC stays within the existing
  one-destination-per-spec contract by using `SortedRunSstSelection` to
  splice partial outputs into existing SRs.
- **Manifest-format changes** to record level identity. Level is derived
  from per-segment sorted-run list position (see
  [Level Identity Under a Global SR Counter](#level-identity-under-a-global-sr-counter)).

## Background

[RFC 0002](./0002-compaction.md) defines the `CompactionScheduler` /
`CompactionExecutor` split: the scheduler observes manifest state and
proposes `CompactionSpec` instances; the executor validates and runs them,
then commits the resulting sorted runs. A `CompactionSpec` today contains:

- `sources: Vec<SourceId>` — each `SourceId` is either an L0 `SsTableView`
  or a `SortedRun` id.
- `destination: u32` — the resulting SR's id.
- `sr_sst_selections: Vec<SortedRunSstSelection>` — when present, restricts
  consumption to specific SST views within the named source SRs (file-level
  compaction). The output replaces exactly those views in the destination
  SR's view list, leaving the rest of the SR untouched.
- `segment: Bytes` — the target segment's prefix
  ([RFC 0024](./0024-segment-oriented-compaction.md)). Empty means the
  compatibility-encoded `prefix=""` segment.

A `SortedRun` (`slatedb/src/db_state.rs`) holds an ordered list of
`SsTableView`s, each carrying `first_entry`/`last_entry` in `SsTableInfo`.
The helpers `SortedRun::overlaps_range` and `SortedRun::tables_covering_range`
already provide the key-range queries leveled compaction needs.

## Design

### Level Mapping

Levels are numbered `L0, L1, ..., L(num_levels - 1)`. L0 is the unsorted L0
list (carrying read precedence by list position). Each Ln (n ≥ 1) is a
single sorted run. Within a segment's `compacted` list, level identity maps
to sorted-run *list position*: the run at list index `i` in a `compacted`
list of length `k` is `L(k − i)`. Equivalently, the most recent compacted SR
is L1 and the oldest compacted SR is L(k).

This is a deliberate change from the global "SR id 0 is the deepest level"
convention used in [RFC 0002](./0002-compaction.md). That convention works
when the manifest holds a single tree with at most one SR per level and no
other source of SR ids, but [RFC 0024](./0024-segment-oriented-compaction.md)
introduces a global monotonic SR id counter shared across segments. Once
that counter is shared, no segment can rely on its deepest SR having id 0.
Deriving level from list position keeps the read-precedence rule unchanged
(list-position wins, exactly as today) and lets level identity work
consistently across segments without a manifest field.

The current implementation in `slatedb/src/leveled_compaction.rs` uses a
per-database id mapping (`sr_id = num_levels − 1 − level`); see
[Level Identity Under a Global SR Counter](#level-identity-under-a-global-sr-counter)
for the migration path to the per-segment list-position derivation.

### L0 → L1: Size-Tiered

L0→L1 compactions are produced by delegating to the existing
`SizeTieredCompactionScheduler` (see `slatedb/src/size_tiered_compaction.rs`)
and filtering its output to specs whose sources include at least one L0 SST.
This is what `LeveledCompactionScheduler::propose` does today.

The delegated size-tiered scheduler is constructed with
`min_compaction_sources = level0_file_num_compaction_trigger`, so the
hybrid's L0 trigger threshold is the leveled config knob the user expects.
The other size-tiered options (`max_compaction_sources`,
`include_size_threshold`) keep their defaults, which gives the same
"coalesce a small batch of L0 SSTs before promoting" behavior the tiered
scheduler uses today.

Why size-tiered for L0:

- **Write-amp.** A pure-leveled L0→L1 path rewrites every L1 SST that
  overlaps with the (small) L0 batch every time L0 fires. Letting L0
  coalesce first amortizes that rewrite over many memtable flushes.
- **Backpressure compatibility.** The size-tiered `BackpressureChecker`
  semantics already guard against producing too-small outputs; reusing the
  scheduler reuses that guard.
- **Operational continuity.** Existing deployments running the size-tiered
  scheduler can switch to leveled without re-tuning L0 thresholds.

### Ln → Ln+1: Leveled with Partial-SR Compactions

For n ≥ 1, the scheduler picks one source SST from Ln (the first SST in the
SR — typically the smallest key range and the oldest), finds the overlapping
SSTs in Ln+1 via `SortedRun::tables_covering_range`, and emits a
`CompactionSpec` whose:

- `sources` names the Ln SR (always) and the Ln+1 SR (if it exists and
  has overlapping SSTs).
- `destination` is the Ln+1 SR id.
- `sr_sst_selections` enumerates the chosen views from each SR.

The executor's existing partial-destination splice
(`compactor_state::finish_compaction`) replaces exactly the named views in
the destination SR's view list with the new compaction output, preserving
all unaffected views in their original positions. No full-SR rewrite
occurs, and the SR's id stays the same across the compaction.

Because each spec touches a single source SST from Ln and only its
overlapping subset of Ln+1, work-per-compaction is bounded by SST size —
not by SR size — which is what makes leveled tractable for large lower
levels.

### Level Selection (Score)

When the L0→L1 path has nothing to do (or in addition to it), the
scheduler evaluates each level n ∈ [1, num_levels − 2] in turn:

```
score(n) = size(Ln) / target(n)
target(n) = max_bytes_for_level_base · max_bytes_for_level_multiplier^(n−1)
```

It picks the level with the highest score above 1.0 and emits a single
Ln→Ln+1 spec for that level, *unless* either the source or destination SR
is already participating in an active or queued compaction (matching the
existing `is_sr_id_in_active_compaction` check). The deepest level
`num_levels − 1` is never a source — there is no Ln+1 to promote into.

Picking the highest-scored level (rather than the highest level above
threshold) keeps the most-overflowing level draining first, which prevents
backpressure from accumulating at a single level while shallower levels are
also full.

### Worked Example

Using a 4-level configuration (`num_levels = 4`,
`max_bytes_for_level_base = 256 MiB`, `max_bytes_for_level_multiplier = 10`)
and the per-segment list-position level mapping. Sorted-run ids are shown
just for traceability; level identity comes from list position.

Initial state of a single segment's `compacted` list:

```
list index 0: SR{id=42, size=300 MiB, ssts=[a..d, d..g, g..k]}    (= L1)
list index 1: SR{id=27, size=2.6 GiB,  ssts=[a..b, b..c, ..., x..y]}  (= L2)
list index 2: SR{id=11, size=12 GiB,   ssts=[...]}                    (= L3)
```

Scores:

- L1: 300 / 256 ≈ 1.17
- L2: 2600 / 2560 ≈ 1.02
- L3: deepest level, not eligible as a source

L1 has the highest score above 1.0. The scheduler picks L1's first SST
(`a..d`), finds overlapping L2 SSTs (say `a..b` and `b..c`), and emits:

```
CompactionSpec {
  sources: [SortedRun(42), SortedRun(27)],
  destination: 27,
  sr_sst_selections: [
    { sr_id: 42, view_ids: [a..d] },
    { sr_id: 27, view_ids: [a..b, b..c] },
  ],
  segment: <segment prefix>,
}
```

After the executor commits, the segment's `compacted` list is unchanged in
shape (still three SRs at the same list positions, so the level mapping is
preserved). The destination SR (id 27) has the merged output spliced in
where `a..b` and `b..c` used to live. SR 42 has `a..d` removed from its
view list.

### Validation

`LeveledCompactionScheduler::validate` accepts a spec only if:

- Specs with at least one L0 source are validated by the delegated
  size-tiered scheduler.
- Otherwise, the spec must reference 1 or 2 distinct SR ids that map to
  adjacent levels (via the per-segment level derivation), with the
  destination SR at the level immediately below the source. The destination
  level must exist (`< num_levels`).
- Both referenced SRs must currently exist in the target segment's
  `compacted` list.

Invalid specs are rejected with `Error::invalid` so that an external or
buggy scheduler cannot drive the hybrid into an inconsistent shape.

### Backpressure

The hybrid does not introduce a new backpressure mechanism. Two carry over
unchanged:

- **L0 length** drives writer pause via the existing per-tree
  `l0_max_ssts` policy from
  [RFC 0024 § Backpressure](./0024-segment-oriented-compaction.md#backpressure).
  The size-tiered L0→L1 path is the relief valve, exactly as it is today
  for the pure-tiered scheduler.
- **Lower-level overflow** does not pause writes. Levels can run hot
  (score > 1.0) for periods when compaction is bandwidth-limited; the
  score-based level selection ensures the most-overflowing level is
  always the next target.

The `level0_file_num_compaction_trigger` threshold must be strictly less
than `l0_max_ssts` so that the L0→L1 compaction fires before backpressure
engages, matching the existing alignment requirement called out in
[RFC 0024 § Default Scheduler](./0024-segment-oriented-compaction.md#default-scheduler).

## Composition with Segmented Compaction

[RFC 0024](./0024-segment-oriented-compaction.md) introduces per-segment
LSM state in the manifest. The leveled scheduler composes with segments
naturally because every primitive it relies on (`SortedRun`, `compacted`
list, `tables_covering_range`, `CompactionSpec.segment`,
`SortedRunSstSelection`) is already segment-scoped.

### Per-Segment Leveled Hierarchy

Each segment's `compacted` list is treated as an independent leveled
hierarchy with its own `num_levels` worth of SRs. The compatibility-encoded
`prefix=""` segment is the singleton case for unsegmented databases.

For each segment with non-empty L0 or `compacted`:

- Run the L0 → L1 (size-tiered) check against that segment's L0 list.
- Run the score-based Ln → Ln+1 selection against that segment's
  `compacted` list.

Each spec the scheduler emits carries `segment` set to the segment's
prefix. The executor's per-segment commit
([RFC 0024 § Compaction](./0024-segment-oriented-compaction.md#compaction))
applies the result to the matching `Segment` entry.

### Cross-Segment Prioritization

When more than one segment is eligible for compaction simultaneously, the
hybrid scheduler prefers, in order:

1. Segments with L0 length closest to the per-tree backpressure threshold
   (matches RFC 0024's default).
2. Among segments with no L0 pressure, the segment whose maximum
   per-level score (`max_n score(n)`) is highest.

This generalizes RFC 0024's L0-count tie-break to the leveled regime:
the segment closest to falling behind on writes gets attention first; the
segment closest to violating its leveled invariant comes next.

### Drain Interaction

A `DrainSegmentSpec` from the application takes precedence over any
leveled compaction the scheduler would otherwise propose for that segment.
The scheduler skips a segment whose drain marker is the only entry in
the manifest, and the writer/compactor merge protocol from RFC 0024
handles the Live → Marker → Absent transitions unchanged.

### Level Identity Under a Global SR Counter

The shipped implementation in `slatedb/src/leveled_compaction.rs` derives
SR id from level via `sr_id = num_levels − 1 − level`. This works for the
unsegmented (root-tree) case where the SR id space is small, contiguous,
and locally allocated.

[RFC 0024 § Per-Segment LSM State](./0024-segment-oriented-compaction.md#proposed-change-per-segment-lsm-state)
makes the SR id counter global and monotonic across all segments. Under
that counter, a segment's deepest SR cannot be assumed to have id 0 — its
id is whatever the global counter handed out the first time something
compacted into that segment. The fixed `sr_id = num_levels − 1 − level`
mapping no longer holds.

This RFC proposes deriving level from **list position within the segment's
`compacted` list**:

```
level(sr_at_list_index_i) = compacted.len() − i
```

so the segment's most recent SR is L1 and its oldest is L(compacted.len()).
This:

- requires no manifest change,
- preserves list-position read precedence exactly,
- works identically for the unsegmented `prefix=""` case (one logical
  segment, one `compacted` list), and
- lets two segments share the global SR id space without colliding on
  level identity.

The shipped implementation should migrate to this derivation before the
scheduler is wired up to segmented databases. For unsegmented databases,
the two derivations are equivalent at the steady state assumed by RFC 0002
(SR ids contiguous, deepest at id 0), so the migration is internal and
does not affect on-disk state.

## Read Path

Unchanged. Within each segment, the reader walks the `compacted` list in
list order (newest first), exactly as it does today. Because L1 sits at
list position 0 and the deepest level sits at the end of the list,
list-position precedence is the same as level precedence — newer levels
shadow older levels for overlapping keys.

Point reads still consult at most one SST per level (because each Ln, n ≥ 1,
is a single SR with non-overlapping SSTs), so the worst-case point-read I/O
falls from O(T·log N) under tiered to O(log N) under leveled.

Range scans read every level's overlapping SSTs and sort-merge as today.
The number of streams in the sort-merge falls with leveled, which reduces
CPU overhead per scanned range.

## Configuration

The four knobs already exist on `LeveledCompactionSchedulerOptions`
(`slatedb/src/config.rs:1206`):

| Option                                      | Default         | Description                                                                            |
|---------------------------------------------|-----------------|----------------------------------------------------------------------------------------|
| `level0_file_num_compaction_trigger`        | `4`             | Number of L0 SSTs that triggers an L0→L1 compaction (passed to the size-tiered scheduler as `min_compaction_sources`). |
| `max_bytes_for_level_base`                  | `256 * 1024^2`  | Target size for L1 in bytes.                                                          |
| `max_bytes_for_level_multiplier`            | `10.0`          | Target-size multiplier between successive levels.                                     |
| `num_levels`                                | `7`             | Number of levels including L0. Must be ≥ 2.                                            |

The defaults match RocksDB's leveled defaults so that operators familiar
with that tuning surface have a consistent starting point.

To enable, wire `LeveledCompactionSchedulerSupplier` into the
`CompactorOptions.compaction_scheduler_supplier` field, the same way
`SizeTieredCompactionSchedulerSupplier` is wired today.

## Impact Analysis

### Compaction

- [x] Compaction strategies — adds a new strategy.
- [ ] Compaction state persistence — no change. Partial-SR specs are
      already supported by `SortedRunSstSelection` and the
      `finish_compaction` splice path.
- [ ] Compactions format — no change.
- [ ] Distributed compaction — unchanged; same scheduler/executor split.

### Storage Engine Internals

- [ ] Write-ahead log (WAL) — no change.
- [ ] Block / object-store cache — read amplification falls; cache hit
      patterns shift toward the upper levels' working set.
- [ ] Indexing (bloom filters, metadata) — fewer SRs to consult per
      point read; bloom-filter false-positive rate matters less.
- [ ] SST format or block format — no change.

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format — no change for this RFC's initial rollout. Level
      identity is derived from per-segment `compacted` list position.

### Core API & Query Semantics

- [ ] No public API change. Read precedence rules are unchanged.

## Operations

### Performance & Cost

| Property                         | Tiered (today)    | Hybrid Leveled (this RFC)         |
|----------------------------------|-------------------|-----------------------------------|
| Sorted runs to consult (point)   | O(T·log N)        | O(log N)                          |
| Write amplification              | ~log N            | ~T·log N (L1+); same as tiered at L0 |
| Space amplification              | up to T× at last  | ~1.1× at last level               |
| Per-compaction work (Ln→Ln+1)    | full-SR merge     | one source SST + overlapping subset |

Hybrid keeps L0→L1 cost identical to tiered. The added write traffic comes
from the L1+ leveled compactions and is bounded by `T = max_bytes_for_level_multiplier`
per level transition. For the default `T = 10` and `num_levels = 7`, total
write amplification is roughly `1 (L0→L1) + 6·10 = 61`, comparable to
RocksDB's leveled defaults.

Object-store request profile: leveled produces fewer but larger reads at
compaction time (per Ln→Ln+1 job pulls one SST from Ln plus its overlapping
slice of Ln+1). PUT volume is dominated by the rewritten subset of Ln+1.

### Observability

New metrics worth adding (not blocking the initial rollout):

- `compaction.level.size_bytes{level=n}` — current size per level per segment.
- `compaction.level.score{level=n}` — current `actual / target` ratio.
- `compaction.leveled.compactions_total{level_pair=n→n+1}` — counter.
- `compaction.leveled.bytes_rewritten_total{level_pair=n→n+1}` — counter.

Existing compaction logs in `compactor.rs` already cover scheduler decisions;
the leveled scheduler should log the chosen level and its score on each
proposal.

### Compatibility

- **Existing data.** No on-disk change. Switching the scheduler from
  size-tiered to leveled on an existing database starts producing leveled
  compactions immediately; previously-written SRs participate as their
  current shape allows. The first few leveled passes may rewrite more data
  than steady-state as the existing run shape converges to the leveled
  invariant.
- **Public API.** Unchanged. The leveled supplier is selected the same way
  the size-tiered supplier is.
- **Rolling upgrades.** No mixed-version concerns — the change lives entirely
  in the compactor process. A rolling restart can flip schedulers safely.
- **Switching back.** A user can switch from leveled back to size-tiered
  without migration; size-tiered will accept the existing SR shape and
  begin its own scheduling.

## Testing

- **Unit tests.** Already present in `slatedb/src/leveled_compaction.rs`
  covering L0→L1 delegation, level selection, partial-SR overlap detection,
  validation rules, and the configuration roundtrip. Extend with cases for
  the per-segment list-position level derivation once that lands.
- **Integration tests.** End-to-end runs against a real ObjectStore that
  exercise (a) leveled-only databases, (b) leveled inside a single non-empty
  segment, (c) two segments compacting in parallel under the per-segment
  hierarchy, (d) leveled + drain interaction.
- **Stress tests.** Sustained write workload with mixed key ranges to
  validate that score-based selection prevents level overflow and that
  partial-SR compactions converge.
- **Deterministic simulation.** Existing scheduler-level DST harness can
  cover the propose/validate cycle.

## Rollout

1. **Phase 1.** Land the per-segment list-position level derivation and the
   segment-aware proposal loop. Until then, the existing scheduler is gated
   to unsegmented databases.
2. **Phase 2.** Add the metrics listed above.
3. **Phase 3.** Document tuning guidance and update the
   `compaction_scheduler` config option's docstring.

The supplier is opt-in (selected by the user's `CompactorOptions`), so
adoption is per-database and reversible.

## Alternatives

- **Pure leveled, including L0→L1.** Rejected. Every L0 flush would rewrite
  the overlapping slice of L1, producing a write-amp multiplier on the order
  of the L0 SST size relative to L1 SST size — pathological under SlateDB's
  small-L0-flush cadence.
- **Tiered everywhere (status quo).** Loses the read-amp win that motivates
  leveled. Tiered remains the right default for write-heavy workloads.
- **Explicit `level` field on each SR in the manifest.** Considered for
  level identity instead of list-position derivation. Rejected: requires a
  manifest format change, and list position already encodes the same
  information unambiguously since it carries read precedence.
- **Subcompactions / split-output compactions.** Could reduce per-job
  latency by parallelizing a single Ln→Ln+1 job. Deferred — partial-SR
  specs already give per-job bounded work, and parallelism comes for free
  across segments and across disjoint level pairs.
- **Picking the highest level above threshold instead of the highest score.**
  Simpler but less responsive: a deeply-overflowed L1 would wait while the
  scheduler drains a barely-over L3.

## Open Questions

- Should `target(level)` be tunable per-segment? For time-bucket workloads
  where older buckets are fixed-size and never grow, the same `base ·
  multiplier^(n-1)` schedule across all segments may misallocate compaction
  budget. A future extension could read a per-segment override from a
  scheduler-supplied policy hook.
- How should `num_levels` interact with segments that never grow beyond a
  handful of SSTs? The score check naturally skips levels that don't exist,
  so the answer is probably "leave levels sparse," but this is worth
  validating with a segmented-tsdb workload.
- Should the hybrid emit multiple Ln→Ln+1 compactions per propose cycle
  (across disjoint level pairs) or stay one-per-cycle? The current
  implementation emits one. Multiple may be worth it once metrics show
  scheduling latency dominating end-to-end compaction throughput.

## References

- [Issue #1598 — Implement leveled compaction](https://github.com/slatedb/slatedb/issues/1598)
- [RFC 0002 — SlateDB Compaction](./0002-compaction.md)
- [RFC 0024 — Segment-Oriented Compaction](./0024-segment-oriented-compaction.md)
- Commit `1863810` — initial `LeveledCompactionScheduler` implementation
- [RocksDB — Leveled Compaction](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction)
- [Dostoevsky — Better Space-Time Trade-offs for LSM-Tree Based KV Stores](https://stratos.seas.harvard.edu/publications/dostoevsky-better-space-time-trade-offs-lsm-tree-based-key-value-stores)
