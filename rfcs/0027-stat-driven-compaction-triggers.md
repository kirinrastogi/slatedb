# SlateDB RFC: Stat-Driven Compaction Triggers

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

<!-- TOC end -->

Status: Draft

Authors:

* [Kirin Rastogi](https://github.com/kirinrastogi)

## Summary

This RFC extends SlateDB's compaction subsystem with two new scheduler triggers — a **periodic-age** trigger and a **tombstone-ratio** trigger. To drive these decisions, `SsTableInfo` gains a `SstCompactionStats` block carrying per-SST metadata. Today's `SizeTieredCompactionScheduler` only fires on size/source-count heuristics, which leaves TTL/`expire_ts` correctness dependent on incidental compaction and gives no direct lever for reclaiming space from delete-heavy regions. The new triggers compose with the existing size trigger.

## Motivation

**Entry TTL correctness needs a forcing function.** SlateDB already supports `expire_ts` end-to-end: writes carry an expiry timestamp (`PutOptions::expire_ts_from`, `MergeOptions::expire_ts_from`), and `RetentionIterator` (`slatedb/src/retention_iterator.rs`) drops expired entries whenever a compaction runs. What it does not have is a *guarantee* that compaction runs on cold data. A sorted run or L0 SST that is never picked up by the size-tiered scheduler — because its segment isn't size-pressured, or because the workload became read-only — carries its expired entries indefinitely. A `periodic_compaction_seconds` trigger will be added.

**Tombstone-heavy regions need a direct lever.** A region that accumulates deletes faster than size pressure compacts it can sit on a large tombstone fraction for as long as the periodic interval. Read fanout grows with the count of live tombstones for any given point lookup, and space amplification grows with their cumulative size. A tombstone-ratio trigger acts on the metric directly, regardless of how long ago the data was written.

**The metadata to drive these decisions doesn't exist yet.** `SsTableInfo` today (`schemas/sst.fbs:28-65`) carries no timestamps and no top-level put/delete/merge counters. `SstStats` already aggregates `num_puts`/`num_deletes`/`num_merges` per SST and per block (`schemas/sst.fbs:77-91`), but it lives in a separately-fetched footer block referenced via `stats_offset`/`stats_len`. Loading it for every SST in every scheduler poll would add an object-store round trip per SST — unacceptable for a hot scheduling path. The triggers need a small, eagerly-loaded surface on `SsTableInfo` itself.

## Goals

- Support a periodic-age compaction trigger configured per database.
- Support a tombstone-ratio compaction trigger configured per database.
- Support other compaction triggers by defining a SstCompactionStats struct in the SsTableInfo.
- Keep the metadata generic enough that other compaction strategies can leverage this.

## Non-Goals

- **Subcompaction.** Splitting a single compaction into parallel sub-jobs is out of scope. It could be tracked somewhere else after 0025-distributed-compaction.md ships.
- **Leveled compaction itself.** Tracked in [issue #1598](https://github.com/slatedb/slatedb/issues/1598). This RFC only adds the metadata it would consume.
- **Settings to limit the amount of data per compaction** Instead of limiting compaction triggered by a new setting, distributed compaction can allow parallel work.

## Design

### 1. `SstCompactionStats` on `SsTableInfo`

`SsTableInfo` gains an optional nested table, embedded so the scheduler can read it from the manifest path without an additional object-store fetch. Schema addition in `schemas/sst.fbs`:

```flatbuffer
// New table: small, eagerly loaded per-SST metadata used by the compaction
// scheduler. Distinct from SstStats (which is referenced via stats_offset and
// includes the per-block array — too large to load on every scheduler poll).
table SstCompactionStats {
    // Number of put entries in the SST. Copied verbatim from SstStats.num_puts.
    num_puts: ulong;
    // Number of delete entries (tombstones). Copied from SstStats.num_deletes.
    num_deletes: ulong;
    // Number of merge entries. Copied from SstStats.num_merges.
    num_merges: ulong;
    // Unix epoch milliseconds at which the SST builder finalized this file.
    file_creation_time: ulong;
    // Unix epoch milliseconds tracking the age of the *data*, not the file.
    // Flush path: equal to file_creation_time (no ancestors).
    // Compaction path: min(oldest_ancestor_time) across all input SSTs.
    oldest_ancestor_time: ulong;
}

// Has metadata about a SST file.
table SsTableInfo {
    // ... existing fields ...

    // Present on SSTs written by versions that support stat-driven
    // compaction triggers; absent on older SSTs.
    compaction_stats: SstCompactionStats;
}
```

**Why embed instead of reference?** Putting it in the manifest instead of individual SST footers makes it easier to iterate. The existing `stats_offset`/`stats_len` indirection points to an `SstStats` block that contains the per-block array (`BlockStats[]`), which can be sizeable. The scheduler walks every SST in every tree on every poll — embedding the small, fixed-size compaction-stats table on `SsTableInfo` keeps it in the manifest-resident path and avoids a per-SST fetch. Additionally it can be a place for metadata that compaction strategies will consume.

**Why on the SsTableInfo instead of SortedRuns?** Other yet to be implemented compaction strategies like leveled compaction operate on individual SSTs instead of entire sorted runs. Setting compaction stats per SST allows them to be consumed by other strategies.

**Derivation rules.**

- **Num puts, deletes, merges** These will be duplicated from the SstStats, and set when the SSTBuilder creates an SST.
- **Flush path.** When a memtable flush produces an SST, `file_creation_time = oldest_ancestor_time = now_ms()`. The SST has no ancestors; the data is as old as the file.
- **Compaction path.** When a compaction produces an output SST, `file_creation_time = now_ms()` and `oldest_ancestor_time = min(input.oldest_ancestor_time)` across all input SSTs. The ancestor timestamp propagates forward so the age of the data is preserved through arbitrarily many compaction generations — that's the property the periodic trigger relies on.

**Rust-side wiring (concrete touch points).**

- `slatedb/src/sst_stats.rs` — aggregation of `num_puts`/`num_deletes`/`num_merges` per SST already happens during `finish_block()`. No change to aggregation; the new code just copies the totals onto `SstCompactionStats` at footer-build time.
- `slatedb/src/sst_builder.rs:149` — `EncodedSsTableBuilder::new` (or an analogous builder setter) accepts an `oldest_ancestor_time: Option<i64>`. The flush call site passes `None`; the compaction call site computes the min across input SSTs and passes that.
- `slatedb/src/sst_builder.rs:354` — at `build()`, record `file_creation_time` from the injected clock, resolve `oldest_ancestor_time` (defaulting to `file_creation_time` when `None`), and attach `SstCompactionStats` to the `SsTableInfo` constructed by the footer builder.
- `slatedb/src/format/sst.rs` — `EncodedSsTableFooterBuilder` threads the new optional table to flatbuffer encoding.
- `slatedb/src/compactor_executor.rs` — when issuing the output builder, compute `min(input.oldest_ancestor_time)` from `job_args.sst_views` plus the input SSTs of `job_args.sorted_runs`. Pass it into the builder.

**Clock source.** Use the system clock injected via SlateDB's existing `Clock` abstraction so deterministic-simulation tests can mock it. All times are Unix epoch milliseconds.

**Backwards compatibility.** The new field is appended to the end of the SsTableInfo. SSTs written by older versions decode `compaction_stats = None`. The scheduler treats absent stats as "ineligible for periodic and tombstone-ratio triggers" — the SST remains fully eligible for the existing size trigger. After a few size-driven compactions, all live SSTs have been re-emitted with the new field populated.

### 2. Periodic-age trigger

A new setting:

```rust
// in slatedb/src/config.rs, CompactorOptions
pub struct CompactorOptions {
    // ... existing fields ...

    /// If set, an SST or SR is eligible for periodic compaction once
    /// `now - oldest_ancestor_time >= periodic_compaction_interval`.
    /// Default `None` = periodic compaction disabled. Recommended: 7 days.
    pub periodic_compaction_interval: Option<Duration>,
}
```

**Eligibility.** An L0 SST or a sorted run is eligible when `now - oldest_ancestor_time >= periodic_compaction_interval`. For a sorted run, use `min(oldest_ancestor_time)` across its SSTs — the run is as old as its oldest data. Per-tree (per-segment) evaluation, mirroring the existing size-tiered loop.

**How it fires — `PickCompactionToOldest` semantics.** When a periodic trigger fires on source `S`, the resulting `CompactionSpec` includes `S` together with every sorted run at lower precedence than `S` in the same tree. The destination is the lowest-id SR among the sources (the existing bottom-most SR id, or a fresh id if `S` is itself the bottom). The destination is therefore always the bottom-most level, which means `is_dest_last_run = true` in the executor (`compactor_executor.rs:332`) and the existing `RetentionIterator` (`retention_iterator.rs:192`) drops expired tombstones unconditionally during the rewrite.

The degenerate cases this RFC's earlier draft notes called out are special instances of this rule:

- **`S` is already the bottom SR.** Sources = `[S]`, destination = `S.id`. A self-rewrite — no merging, just a tombstone-cleaning pass.
- **`S` is an L0 SST and no SRs exist yet.** Sources = `[S]`, destination = a fresh SR id that becomes the (only, hence bottom) SR. No merging needed.
- **`S` is an L0 SST or intermediate SR with other SRs below.** Sources span from `S` down through every SR below, output to the bottom SR id. This is a full down-compaction — expensive but correct. It is the only safe way to elide tombstones without leaving them shadowing older versions sitting in lower SRs.

**Why include everything below.** Dropping tombstones from a non-bottom rewrite would resurrect older versions of the deleted keys that still live in lower SRs. Either the rewrite must include those lower SRs (so the older versions get merged away alongside the tombstones), or the rewrite must keep the tombstones. The first option is what RocksDB universal compaction's periodic path does (`PickCompactionToOldest`), and it's what this RFC adopts.

**Cost bounding.** The trigger fires only on the **oldest eligible source per tree** per round-robin pass — the source with the smallest `oldest_ancestor_time`. That source is almost always near the bottom (it's the oldest data), so the down-compaction range is typically small.

**Validation.** The existing consecutive-sources validation (`size_tiered_compaction.rs:284-294`) is satisfied: source spans from `S` down to bottom are always a consecutive logical-order suffix. The "destination = lowest-id SR among sources" rule (`size_tiered_compaction.rs:296-313`) is satisfied by construction.

### 3. Tombstone-ratio trigger

A new option:

```rust
// in slatedb/src/config.rs, CompactorOptions
pub struct CompactorOptions {
    // ... existing fields ...

    /// If set, an SST or SR is eligible for tombstone-ratio compaction when
    /// its tombstone fraction is at least this value.
    /// Default `None` = disabled. Recommended: 0.25.
    pub max_tombstone_ratio: Option<f32>,
}
```

**Ratio definition.** For a single SST:

```
ratio = num_deletes / (num_puts + num_deletes + num_merges)
```

For a sorted run, aggregate `num_deletes` and the denominator across all SSTs in the run, then compute. Aggregation is cheap because `SsTableInfo.compaction_stats` is embedded.

**Eligibility.** An SST or SR is eligible when its computed `ratio >= max_tombstone_ratio`. SSTs lacking `compaction_stats` (e.g., written before this RFC shipped) are treated as ineligible.

**How it fires.** Identical mechanics to the periodic trigger — `PickCompactionToOldest` semantics. When the eligible source `S` is selected, sources span from `S` down to the bottom-most SR in the same tree, output to the bottom. Because the destination is always the bottom-most level, tombstones elide unconditionally during the rewrite — the explicit point of this trigger.

**Source selection.** The trigger picks the **highest-ratio** eligible source per tree per round-robin pass, where periodic picks the oldest. The two triggers have different priority orderings within their candidate sets.

### 4. Trigger ordering

Within each `propose()` round-robin pass (per tree):

1. **Size trigger** runs first (existing logic in `pick_next_compaction`, `size_tiered_compaction.rs:331-369`).
2. If size produced no spec, evaluate **periodic**: pick the single oldest eligible source in this tree (smallest `oldest_ancestor_time`, considering both L0 SSTs and SRs). Emit one `CompactionSpec` with `PickCompactionToOldest` semantics (§2).
3. If periodic produced no spec, evaluate **tombstone-ratio**: pick the single highest-ratio eligible source in this tree. Emit one `CompactionSpec` with `PickCompactionToOldest` semantics (§3).

**Rationale.** Size pressure is the only trigger that can also relieve write-side L0 backpressure, so it must win when it fires. Periodic runs before tombstone because periodic-eligibility implies the data is old enough that age-driven correctness (TTL expiry) takes precedence over a ratio-driven optimization on fresher data. The ordering reflects priority of concern: avoid write stalls, then guarantee TTL correctness, then reclaim tombstone space.

## Impact Analysis

SlateDB features and components that this RFC interacts with.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [x] Time to live (TTL) — periodic trigger turns existing TTL/`expire_ts` from best-effort into eventually-correct.
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [x] Manifest format — only indirectly. `SsTableInfo` lives inside `CompactedSsTableV2`, so the new optional field rides along automatically. FlatBuffers tolerates added optional fields; no version bump.
- [ ] Checkpoints
- [ ] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence — `CompactionSpec` shape is unchanged; the new triggers reuse the existing single-source primitive. No change to `compactions_store.rs`.
- [ ] Compaction filters
- [x] Compaction strategies and scheduling
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [x] SST format or block format — `SsTableInfo` gains an optional `SstCompactionStats` table.

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc) — non-breaking config additions only.
- [x] Observability (metrics/logging/tracing) — new per-trigger counters and oversized-emission counter.

## Operations

### Performance & Cost

- **Latency.** Minimal write-path latency. No read-path latency. Compactions themselves run the existing executor unchanged. Periodic and tombstone triggers add compaction frequency in steady state; the alternative — letting expired data and tombstones accumulate — has its own slow-burn costs.
- **Throughput.** Periodic compaction's write amplification is bounded by `(DB size / periodic_compaction_interval)`. For a 100 GB database with a 7-day interval, that's ~14 GB/day of additional rewrite work in the worst case. In practice the per-tree round-robin amortizes this across segments.
- **Object-store request / cost.** Mirrors the added write amplification above. `max_compaction_bytes` lets operators tune the burst profile and avoid saturating the compactor's network for too long.
- **Space amplification.** The tombstone trigger directly reduces space amp. The periodic trigger provides a floor on staleness-driven amp by guaranteeing a rewrite cadence.
- **Read amplification.** Tombstone trigger reduces read fanout for delete-heavy regions by collapsing tombstones at the bottom.

### Observability

- **Configuration.** Three new fields on `CompactorOptions`:
  - `periodic_compaction_interval: Option<Duration>` (default `None`).
  - `max_tombstone_ratio: Option<f32>` (default `None`).
  - `max_compaction_bytes: Option<u64>` (default `None`).
- **Metrics.**
  - `compaction.trigger.size.fires` (counter)
  - `compaction.trigger.periodic.fires` (counter)
  - `compaction.trigger.tombstone.fires` (counter)
  - `compaction.oversized_emissions` (counter) — incremented when a single source exceeds `max_compaction_bytes`.
- **Logs.** Every emitted `CompactionSpec` gains a `trigger` label (`size`, `periodic`, or `tombstone`) so operators can attribute compaction load to its driver.

### Compatibility

- **Existing data on object storage.** Old SSTs lack `compaction_stats` → the scheduler treats them as ineligible for periodic/tombstone triggers but they remain eligible for size. After a few size-driven compactions any live SST has been re-emitted with the field populated. No migration step required.
- **Existing public APIs (including bindings).** `CompactorOptions` additions are non-breaking — three new optional fields.
- **Rolling upgrades / mixed-version behavior.** Old reader + new writer: the new field is ignored (FlatBuffers tolerance). New reader + old writer: the reader sees `None` and evaluates only size triggers, falling back to today's behavior. Safe in both directions.

## Testing

- **Unit tests.**
  - `sst_builder` populates `compaction_stats` with correct counts and timestamps in the flush path (`oldest_ancestor_time == file_creation_time`).
  - Compaction-path builder computes `oldest_ancestor_time = min(inputs)`.
  - Scheduler picks the oldest eligible source for periodic given a mocked clock.
  - Scheduler picks the highest-ratio source for tombstone-ratio given handcrafted stats.
  - Trigger ordering: when all three triggers are eligible on the same tree, the emitted spec corresponds to size.
  - `max_compaction_bytes` clamps multi-source picks but lets single-source triggers through (with the `compaction.oversized_emissions` counter incremented).
- **Integration tests.**
  - Build a DB, age it with the deterministic clock, confirm periodic compaction fires and rewrites every SST at least once within the configured interval.
  - Build a DB with a high delete ratio, confirm the tombstone trigger fires and bottom-level tombstones drop.
  - Mixed-mode test: alongside the periodic and tombstone triggers, drive size-pressure compactions and confirm round-robin fairness across segments.
- **Fault-injection / chaos tests.** No new failure modes introduced — the new triggers reuse the existing compaction pipeline. Existing chaos coverage applies unchanged.
- **Deterministic simulation tests.** Existing DST harness picks up the new options via `CompactorOptions`. Add a scenario that toggles `periodic_compaction_interval` and `max_tombstone_ratio` and confirms deterministic firing.
- **Formal methods verification.** N/A.
- **Performance tests.** None required for this change; the additions are scheduling-level decisions and reuse the existing hot path.

## Rollout

- **Milestone 1: format + scheduler land together.** The `SstCompactionStats` schema addition and the new trigger code ship in a single release behind default-off options. Existing deployments are byte-for-byte equivalent unless they opt in.
- **Milestone 2: docs.** SlateDB book entry under Compaction with recommended values for the new options, behavior notes for mixed-version reads, and `compaction.oversized_emissions` interpretation.
- **Feature flags / opt-in.** Defaults are opt-in (`None` for all three options). No build-time feature flag required.

## Alternatives

1. **Store the new fields on `SstStats` instead of as a new `SstCompactionStats` table.** Rejected because `SstStats` lives in a separately-fetched footer block. Scheduling decisions touch every SST in every tree per poll, and a per-SST object-store fetch is not acceptable on that path. Embedding a small fixed-size table on `SsTableInfo` keeps the data in the always-resident manifest path.

2. **Use ULID-embedded creation timestamps instead of an explicit field.** ULIDs encode a 48-bit Unix-millisecond timestamp that the codebase already extracts in `garbage_collector.rs:1408`. Rejected because (a) the data's `oldest_ancestor_time` is logically distinct from any single SST's ULID time and must be tracked separately, and (b) once we are persisting one timestamp explicitly, persisting the other for free avoids a reader-side hidden assumption that the SST ID encoding is the durable source of truth for the creation time.

3. **A single combined "compaction priority score" trigger** (one knob, scheduler computes a score from age and tombstone ratio). Rejected because the two triggers serve different purposes — age is about TTL correctness, ratio is about space efficiency — and benefit from independent tuning. Combining them obscures the operator's intent and makes per-trigger metrics impossible.

4. **Status quo (no triggers).** Rejected because TTL expiry is not guaranteed and tombstone-heavy workloads have no mechanism to reclaim space short of full manual compaction.

## Open Questions

- **`max_compaction_bytes` enforcement on already-oversized single sources.** When the source picked by periodic or tombstone-ratio already exceeds the configured cap, the scheduler emits the spec anyway with a logged warning and a `compaction.oversized_emissions` metric increment. Skipping the source would let SR growth go unbounded; obeying the cap requires subcompaction, which is out of scope. **Proposed resolution:** emit + observability, revisit when subcompaction lands.

- **`PickCompactionToOldest` cost on a top-of-LSM source.** Picking the oldest eligible source per pass keeps the typical compaction range small, but a single L0 SST that has aged past the periodic interval still triggers a full down-compaction of every SR below it. Acceptable for steady-state TTL enforcement; potentially expensive after a long writer outage where many L0 SSTs age out together. Mitigations: `max_concurrent_compactions` bounds parallelism, the per-tree round-robin amortizes across segments, and operators can tune `periodic_compaction_interval` higher. A per-trigger sub-budget could be added later if real workloads suffer.

- **Clock skew across writer restarts.** `oldest_ancestor_time` propagates from input SSTs across compaction generations, so a single skewed write — e.g. clock jumping forward briefly — could pollute long-lived ancestor times in ways that are hard to recover from. Mitigation: clamp `oldest_ancestor_time = min(file_creation_time, computed_min)` so it can never be in the future relative to the building clock. Worth confirming with reviewers; the implementation cost is small.

## References

- [RFC 0002: SlateDB Compaction](./0002-compaction.md) — original compaction design.
- [RFC 0024: Segment-Oriented Compaction](./0024-segment-oriented-compaction.md) — per-segment trigger evaluation; new triggers respect segment scoping unchanged.
- [Issue #1598 — Leveled compaction](https://github.com/slatedb/slatedb/issues/1598) — the future consumer of per-SST `compaction_stats`.
- RocksDB `periodic_compaction_seconds` and `PickCompactionToOldest` — the prior art this RFC's semantics mirror.
- `slatedb/src/size_tiered_compaction.rs` — scheduler extension point.
- `slatedb/src/sst_builder.rs`, `slatedb/src/format/sst.rs` — SST format extension surface.
- `slatedb/src/retention_iterator.rs` — existing TTL/tombstone elision; unchanged by this RFC.

## Updates
