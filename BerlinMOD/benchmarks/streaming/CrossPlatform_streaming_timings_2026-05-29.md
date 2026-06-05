# BerlinMOD three-platform stream benchmark

## What this benchmark measures

The streaming benchmark runs the BerlinMOD query set as continuous stream jobs
rather than batch SQL: each query evaluated event-by-event over a corpus of
vehicle instants, in three forms — continuous, windowed, snapshot.

This benchmark measures per-query throughput across three stream engines —
MobilityFlink, MobilityKafka, and MobilityNebula — that share the same MEOS
kernel and evaluate the same predicates over the same corpus. The aim is a
diagnostic, not a leaderboard: where each engine pays its cost, with the batch
result as the correctness oracle.

## Workload

The query set is the BerlinMOD R-queries recast in the forms suited to a stream
(see [`README.md`](README.md) for the full set and each query's R-query lineage).
Every query runs in three forms:

- **continuous** — emits `predicate(event)` for every event as it arrives.
- **windowed** — aggregates the predicate over tumbling windows.
- **snapshot** — samples each vehicle's last-known position at tick instants; by
  contract the snapshot at watermark `T` equals the batch result at `T`, which is
  the bridge to the DB benchmark.

The cell shared with the DB benchmark is the within-distance predicate
(`edwithin_tgeo_geo`), evaluated through the same MEOS call on every platform.

## Dataset, hardware, methodology

The corpus is BerlinMOD `berlinmod_instants.csv` — 216 075 instants, 5 vehicles,
~11 days — reprojected EPSG:3857→EPSG:4326 through MEOS `geo_transform` at load.
The point `P`, region box, road segment, points of interest, and target vehicle
ids derive from the corpus (`P` = centroid), and the window/tick granularity
scales to the corpus span. libmeos is built `-DMEOS=ON -DCBUFFER=ON -DNPOINT=ON
-DPOSE=ON -DRGEO=ON`; hardware is a 16-core x86-64 Linux machine, Java 21.

- **Flink** — Flink 1.16 single-node local mini-cluster, parallelism 1. Harness
  [`MobilityFlink` `BerlinMODBenchmark`](https://github.com/MobilityDB/MobilityFlink/blob/main/flink-processor/src/main/java/berlinmod/BerlinMODBenchmark.java).
- **Kafka** — Kafka Streams 3.6, one stream thread, each cell a `KafkaStreams`
  application against its own fresh in-process `EmbeddedKafkaCluster` (a real
  `KafkaServer` over loopback). Harness
  [`MobilityKafka` `EmbeddedBrokerBenchmark`](https://github.com/MobilityDB/MobilityKafka/blob/main/kafka-streams-app/src/test/java/berlinmod/EmbeddedBrokerBenchmark.java).
- **Nebula** — NebulaStream harness.

## Invariants held fixed

These hold for every (query, form, engine) cell.

- **Same MEOS predicate.** Every platform evaluates the identical MEOS spatial
  call; the per-cell query shape is the same across engines.
- **Throughput definition.** Each cell runs as one streaming job over the shared
  corpus, terminated by a counting sink; throughput is input events ÷ wall-clock
  and `output rows` is the sink cardinality.
- **Corpus-derived parameters.** All query parameters and the window/tick
  granularity derive from the same corpus, identical on every platform.
- **Batch oracle.** The continuous form is checked event-for-event against a
  batch pass over the same corpus through the same MEOS call (see Parity).

## Results — throughput (events/s)

Each engine fills its column; see [Contributing your numbers](#contributing-your-numbers).
The charts below regenerate once columns land.

| Query | Form | MobilityFlink | MobilityKafka | MobilityNebula |
|---|---|---:|---:|---:|
| Q1 | continuous | — | — | — |
| Q1 | windowed | — | — | — |
| Q1 | snapshot | — | — | — |
| Q2 | continuous | — | — | — |
| Q2 | windowed | — | — | — |
| Q2 | snapshot | — | — | — |
| Q3 | continuous | — | — | — |
| Q3 | windowed | — | — | — |
| Q3 | snapshot | — | — | — |
| Q4 | continuous | — | — | — |
| Q4 | windowed | — | — | — |
| Q4 | snapshot | — | — | — |
| Q5 | continuous | — | — | — |
| Q5 | windowed | — | — | — |
| Q5 | snapshot | — | — | — |
| Q6 | continuous | — | — | — |
| Q6 | windowed | — | — | — |
| Q6 | snapshot | — | — | — |
| Q7 | continuous | — | — | — |
| Q7 | windowed | — | — | — |
| Q7 | snapshot | — | — | — |
| Q8 | continuous | — | — | — |
| Q8 | windowed | — | — | — |
| Q8 | snapshot | — | — | — |
| Q9 | continuous | — | — | — |
| Q9 | windowed | — | — | — |
| Q9 | snapshot | — | — | — |

### Throughput charts (events/s, log scale, higher is better)

One grouped bar chart per streaming form, all three engines on the same corpus.

![Continuous-form streaming throughput](streaming_continuous.svg)

![Windowed-form streaming throughput](streaming_windowed.svg)

![Snapshot-form streaming throughput](streaming_snapshot.svg)

## Reading the results

Filled once the runs land. Expected shape: the per-event spatial cells
(Q3/Q8/Q9-continuous) are the throughput floor; the non-spatial Q1/Q2 cells the
ceiling. Q5-continuous enumerates every meeting pair across all vehicles on each
event (O(V²) per event), so it is the floor on each engine. The snapshot form is
sampled, so a within-`P` snapshot can be empty when no vehicle is within `d` of
`P` at a tick boundary even though the continuous form reports near-`P` events
between boundaries.

## Parity with the DB benchmark

The continuous form emits `predicate(event)` for every event, checked
event-for-event against a batch pass over the same corpus through the same MEOS
call — the cross-family link to the
[3-DB benchmark](../CrossPlatform_timings_2026-05-12.md), whose batch result is
the oracle. A query is `exact` when streaming-true equals batch-true with zero
mismatches.

| Query | Events | Streaming-true | Batch-true | Mismatches | Parity |
|---|---:|---:|---:|---:|---|
| Q3 (`edwithin_tgeo_geo`, within `d` of `P`) | — | — | — | — | — |
| Q8 (`edwithin_tgeo_geo`, within `d` of segment) | — | — | — | — | — |

## Contributing your numbers

Each engine owns one column of the throughput grid and its parity rows.

1. Run your harness over the corpus (links under [Methodology](#dataset-hardware-methodology)),
   producing one throughput value per (query, form).
2. Fill your engine's column in the **Results** grid and, for the continuous
   spatial queries, your **Parity** counts.
3. Add your series to
   [`scripts/render_streaming_chart.py`](scripts/render_streaming_chart.py) and run
   `python3 scripts/render_streaming_chart.py` to replace the empty charts.

## Reproduce

Regenerate the charts from
[`scripts/render_streaming_chart.py`](scripts/render_streaming_chart.py) — run
`python3 scripts/render_streaming_chart.py` to refresh all three SVGs. The
per-engine run harnesses are linked under Methodology.
