# Cross-platform th3index benchmark readiness — MobilityDuck & MobilitySpark

Sibling of `MobilityDB_BerlinMOD_th3index_bench.md`.  Inventory of what
each platform needs to run the same BerlinMOD chapter-1 + index-matrix
benchmark and converge on the same row counts.  The MobilityDB
measurements are the reference; any divergence on a sibling platform
is the test signal.

---

## Benchmark requirements per platform

1. Shared CSV produced by `berlinmod_portability_export()` carrying the
   `trip_h3` column.
2. Native registration of `th3index` and its accessors / casts.
3. `h3_latlng_to_cell(tgeompoint, integer)` SQL function.
4. `geoToH3IndexSet(geometry, integer)` and
   `everIntersectsH3IndexSet_Th3Index(h3indexset, th3index)` SQL
   functions.
5. GiST / SP-GiST equivalents.  Where the platform has no spatial-index
   API (Spark), the columnar `trip_h3` is the prefilter mechanism.

---

## MobilityDuck

### Available

- BerlinMOD shared CSV ingestion is wired through the portability path.
- Temporal / geo parity complete.
- DuckDB's per-chunk zone maps provide automatic bbox-style filtering
  on tgeompoint columns; equivalent to GiST on MobilityDB for the
  Q1/Q6 access pattern.

### Required

| Item | Estimate | Notes |
|---|---|---|
| Register `th3index` type | 1–2 days | New base type binding; reuse MEOS serialisation directly. |
| Register `h3_latlng_to_cell(tgeompoint, int)` | 0.5 day | MEOS → DuckDB UDF wrapper. |
| Register `geoToH3IndexSet(geometry, int)` + `h3indexset` set type | 1 day | DuckDB array surface for `h3indexset`. |
| Register `everIntersectsH3IndexSet_Th3Index(h3indexset, th3index)` | 0.5 day | Boolean scalar UDF. |
| Zone-map predicate pushdown for `trip_h3` | 1 day | Verify the columnar value is used as a chunk-skip predicate. |
| CI test entries in `mobilityduck/test/` | 0.5 day | th3index pytest pipeline. |

Estimated total: ~4–5 person-days.

---

## MobilitySpark

### Available

- BerlinMOD shared CSV ingestion validated (`feat/edge-to-cloud-quickstart`).
- JMEOS 1.4 jar exposes the bulk of the MEOS API.
- MobilitySpark **PR #9** (open) carries `Th3IndexUDFs` with 86 UDFs covering
  `meos_h3.h`, including `geoToH3IndexSet` and `everIntersectsH3IndexSetTh3Index`.
- `local[2]` correctness floor on the Spark master URL (per
  `feedback_mobilityspark_local2_constraint.md`).
- BerlinMOD `bench.sh` driver and `BerlinMODBench.java` exist.

### Required

| Item | Estimate | Notes |
|---|---|---|
| JMEOS regen against latest MEOS | in flight | Parallel session; brings MEOS-level fixes to the jar. |
| Rebuild MobilitySpark against the regenerated jar | 0.5 day | Maven. |
| CI wiring on PR #9 | 0.5 day | Compilation depends on the regen. |
| Remove `preprocessForSpark` th3index injection rules | 0.5 day | Portable SQL on PR #24 carries the prefilter; injection layer is redundant. |
| Spatial-index work | n/a | Spark exposes no spatial-index API; the columnar `trip_h3` prefilter is the substitute. |

Estimated total: ~1.5 person-days after JMEOS regen lands.

---

## Coordination

```
                       MobilityDB
                            │
                ┌───────────┴────────────┐
                │ MEOS-side fixes        │
                │  (PR #940 lift,        │
                │   PR #938 polygon)     │
                └───────────┬────────────┘
                            │
            ┌───────────────┴───────────────┐
            │ MobilityDB benchmark numbers   │
            │ (this document's sibling)      │
            └───────────────┬───────────────┘
                            │
        ┌────────────────┬──┴───────────────┐
        ▼                ▼                  ▼
   JMEOS regen      MobilityDuck      MobilitySpark
   (parallel)       th3index port    PR #9 CI unblock
                          │                  │
                          └────────┬─────────┘
                                   ▼
                       Three-platform comparison
                       Reference: MobilityDB numbers
```

---

## Tracking pointers

- **MobilityDB PR #940** — lift framework helper (master).
- **MobilityDB PR #938** — static-geometry → H3 cell set public API
  (open; bench-driving branch).
- **MobilityDB-BerlinMOD PR #24** — shared CSV ships `trip_h3` +
  th3index-variant chapter-1 SQL (open).
- **MobilityDB-BerlinMOD PR #26** — bench documents and reproduce
  script (open).
- **MobilitySpark PR #9** — `Th3IndexUDFs` (open, CI gated on JMEOS
  regen).
- MobilityDuck th3index port — open work; no PR filed yet.
