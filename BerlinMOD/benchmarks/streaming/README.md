# BerlinMOD streaming benchmark (3-stream)

The streaming sibling of the cross-platform BerlinMOD benchmark. The 3-DB
benchmark (`../`) runs the BerlinMOD/r queries as batch SQL on PostgreSQL
(MobilityDB), DuckDB (MobilityDuck), and Spark (MobilitySpark) and requires
identical results. This benchmark runs a streaming query set on the three
stream platforms — Flink (MobilityFlink), Kafka (MobilityKafka), and
NebulaStream (MobilityNebula) — in three streaming forms, and measures
throughput.

The Flink and Kafka platforms reach MEOS through a single `MEOSBridge` over
[JMEOS](https://github.com/MobilityDB/JMEOS); MobilityNebula calls MEOS C
directly. Every spatial predicate is a MEOS operator (`edwithin_tgeo_geo`,
`eintersects_tgeo_geo`, `geog_distance`) — the streaming layer contributes no
spatial mathematics of its own.

## Streaming forms

| Form | Question | Notes |
|---|---|---|
| Continuous | "At every moment, which holds now?" | per-event output, watermark-independent |
| Windowed | "Per tumbling window, what holds?" | event-time tumbling window |
| Snapshot | "At time T, what holds?" | watermark-driven; the parity oracle |

The **snapshot form is the bridge to the 3-DB benchmark**: by contract, for a
spatial-selection query the streaming snapshot at watermark `T` equals the batch
BerlinMOD result on the same data up to `T` at the same scale factor. Where a
streaming query has a batch analog, its snapshot output cardinality is checked
against the 3-DB result as the oracle.

## Streaming query set

The streaming set has its own numbering and intents; it is **not** the
BerlinMOD/r numbering. It covers a 9-query subset of the BerlinMOD intents in
forms suited to streaming. The `r-query` column records the canonical
BerlinMOD/r analog where one exists, and is left blank for streaming-only
queries.

| # | Streaming intent | MEOS operator | BerlinMOD/r analog |
|---|---|---|---|
| Q1 | Which vehicles have been seen | — | — (stream enumeration) |
| Q2 | A target vehicle's positions | id filter | cf. r-Q3 (where have given vehicles been) |
| Q3 | Vehicles within `d` of point `P` | `edwithin_tgeo_geo` | cf. r-Q11 / r-Q15 (passed a point) |
| Q4 | Vehicles entering region `R` | `eintersects_tgeo_geo` | cf. r-Q13 / r-Q14 (within regions) |
| Q5 | Pairs of vehicles meeting near `P` | `edwithin_tgeo_geo` + `geog_distance` | cf. r-Q12 (met at a point) |
| Q6 | Cumulative distance per vehicle | `geog_distance` | ≈ r-Q8 (overall travelled distance) |
| Q7 | Vehicles near points of interest | `edwithin_tgeo_geo` | cf. r-Q4 / r-Q17 (passed / visited points) |
| Q8 | Vehicles within `d` of a road segment | `edwithin_tgeo_geo` | cf. r-Q4 (passed points) |
| Q9 | Distance between two vehicles | `geog_distance` | cf. r-Q5 (minimum distance) |

The within-distance query (Q3) is the canonical shared cell: it is
[`MobilityNebula/Queries/Query1.yaml`](https://github.com/MobilityDB/MobilityNebula/blob/main/Queries/Query1.yaml)
(`edwithin_tgeo_geo … WINDOW TUMBLING`) and
[`MobilityFlink` `Q3{Continuous,Windowed,Snapshot}Function`](https://github.com/MobilityDB/MobilityFlink/tree/main/flink-processor/src/main/java/berlinmod),
expressed through the same MEOS operator on both engines.

## Shared result schema

Each platform's harness emits one row per (query, form) with these fields, so
the three emissions join into the comparison table without hand-merging:

| Field | Meaning |
|---|---|
| `engine` | flink \| kafka \| nebula |
| `query` | Q1 … Q9 |
| `form` | continuous \| windowed \| snapshot |
| `events_in` | input events fed to the job |
| `output_rows` | rows emitted to the sink |
| `throughput_eps` | `events_in` ÷ wall-clock, events per second |
| `snapshot_equals_batch` | for the snapshot form: whether output matches the 3-DB oracle (else blank) |

```
engine,query,form,events_in,output_rows,throughput_eps,snapshot_equals_batch
flink,Q3,snapshot,30000,3120,34722,
```

## Per-platform harnesses

- **Flink** — [`BerlinMODBenchmark`](https://github.com/MobilityDB/MobilityFlink/blob/main/flink-processor/src/main/java/berlinmod/BerlinMODBenchmark.java); figures in [`benchmark-results.md`](https://github.com/MobilityDB/MobilityFlink/blob/main/flink-processor/docs/benchmark-results.md).
- **Nebula** — `systest -b -g benchmark` → [bench.nebula.stream](https://bench.nebula.stream); queries in [`MobilityNebula/Queries`](https://github.com/MobilityDB/MobilityNebula/tree/main/Queries).
- **Kafka** — shares the Flink `MEOSBridge` over JMEOS.

The cross-platform comparison is in
[`CrossPlatform_streaming_timings_2026-05-29.md`](CrossPlatform_streaming_timings_2026-05-29.md).
