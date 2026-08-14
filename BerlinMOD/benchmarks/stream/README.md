# BerlinMOD streaming benchmark

The MobilityDB ecosystem runs the same BerlinMOD spatial predicates on **six
platforms** through a single shared kernel (MEOS). The platforms split into two
families by operational mode:

| Family | Platforms | Query model | Data model | Metric |
|---|---|---|---|---|
| **Batch SQL** | MobilityDB · MobilityDuck · MobilitySpark | SQL over stored data; GiST / SP-GiST indexed | Complete trips, stored and indexed | Query latency (ms – s) |
| **Stream** | MobilityFlink · MobilityKafka · MobilityNebula | Continuous / windowed / snapshot over arriving events | Instant stream, no prior storage required | Throughput (events/s) |

Within each family the spatial predicate is the same MEOS call — platform choice
is operational, not predicate-driven. The **snapshot** form bridges the two
families: by contract, the stream snapshot at watermark `T` equals the batch
result on the same data up to `T`, so snapshot output is checked against the
3-DB batch result as the correctness oracle.

Results by family:

| Family | Results |
|---|---|
| Batch SQL | [`../batch/CrossPlatform_timings.md`](../batch/CrossPlatform_timings.md) |
| Stream | [`CrossPlatform_timings.md`](CrossPlatform_timings.md) |

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

## Streaming query set

The streaming set covers a 9-query subset of the BerlinMOD intents in forms
suited to streaming. The `r-query` column records the canonical BerlinMOD/r
analog where one exists.

| # | Streaming intent | MEOS operator | BerlinMOD/r analog |
|---|---|---|---|
| Q1 | Which vehicles have been seen | — | — (stream enumeration) |
| Q2 | A target vehicle's positions | id filter | cf. r-Q3 |
| Q3 | Vehicles within `d` of point `P` | `edwithin_tgeo_geo` | cf. r-Q11 / r-Q15 |
| Q4 | Vehicles entering region `R` | `eintersects_tgeo_geo` | cf. r-Q13 / r-Q14 |
| Q5 | Pairs of vehicles meeting near `P` | `edwithin_tgeo_geo` + `geog_distance` | cf. r-Q12 |
| Q6 | Cumulative distance per vehicle | `geog_distance` | ≈ r-Q8 |
| Q7 | Vehicles near points of interest | `edwithin_tgeo_geo` | cf. r-Q4 / r-Q17 |
| Q8 | Vehicles within `d` of a road segment | `edwithin_tgeo_geo` | cf. r-Q4 |
| Q9 | Distance between two vehicles | `geog_distance` | cf. r-Q5 |

The within-distance query (Q3) is the canonical shared cell: it is
[`MobilityNebula/Queries/Query1.yaml`](https://github.com/MobilityDB/MobilityNebula/blob/main/Queries/Query1.yaml)
(`edwithin_tgeo_geo … WINDOW TUMBLING`) and
[`MobilityFlink` `Q3{Continuous,Windowed,Snapshot}Function`](https://github.com/MobilityDB/MobilityFlink/tree/main/benchmark/src/main/java/berlinmod),
expressed through the same MEOS operator on both engines.

## Shared result schema

Each platform's harness emits one row per (query, form):

| Field | Meaning |
|---|---|
| `engine` | flink \| kafka \| nebula |
| `query` | Q1 … Q9 |
| `form` | continuous \| windowed \| snapshot |
| `events_in` | input events fed to the job |
| `output_rows` | rows emitted to the sink |
| `throughput_eps` | `events_in` ÷ wall-clock, events per second |
| `snapshot_equals_batch` | for the snapshot form: whether output matches the 3-DB oracle |

## Per-platform harnesses

- **Flink** — [`BerlinMODBenchmark`](https://github.com/MobilityDB/MobilityFlink/blob/main/benchmark/src/main/java/berlinmod/BerlinMODBenchmark.java)
- **Nebula** — `systest -b -g benchmark` → [bench.nebula.stream](https://bench.nebula.stream); queries in [`MobilityNebula/Queries`](https://github.com/MobilityDB/MobilityNebula/tree/main/Queries)
- **Kafka** — [`EmbeddedBrokerBenchmark`](https://github.com/MobilityDB/MobilityKafka/blob/main/benchmark/src/test/java/berlinmod/EmbeddedBrokerBenchmark.java)
