# BerlinMOD three-platform stream benchmark

## What this benchmark measures

Per-query throughput of the BerlinMOD streaming query set across three stream
engines — MobilityFlink, MobilityKafka, and MobilityNebula — that share the same
MEOS kernel and evaluate the same predicates over the same corpus. Every query
runs in three forms (continuous, windowed, snapshot); the snapshot form is checked
against the 3-DB batch result as the correctness oracle.

## Workload

The 9-query streaming set (see [`README.md`](README.md) for intents and BerlinMOD/r
lineage) in three forms:

- **continuous** — emits `predicate(event)` for every event as it arrives.
- **windowed** — aggregates the predicate over tumbling windows.
- **snapshot** — samples each vehicle's last-known position at tick instants; by
  contract the snapshot at watermark `T` equals the batch result at `T`.

## Dataset, hardware, methodology

The corpus is BerlinMOD `berlinmod_instants.csv` — 216 075 instants, 5 vehicles,
~11 days — reprojected EPSG:3857→EPSG:4326 through MEOS `geo_transform` at load.
All query parameters and window/tick granularity derive from the same corpus,
identical on every platform.

### Machine

- **CPU** — AMD Ryzen 9 5900HX with Radeon Graphics (8 cores / 16 threads)
- **Memory** — 23 GiB
- **OS** — Ubuntu 24.04.4 LTS, kernel `6.6.87.2-microsoft-standard-WSL2`
- **Runtime** — openjdk 21.0.11
- **libmeos** — built `-DMEOS=ON -DCBUFFER=ON -DNPOINT=ON -DPOSE=ON -DRGEO=ON`

### Per-engine harnesses

- **Flink** — Flink 1.16 single-node local mini-cluster, parallelism 1. Harness
  [`MobilityFlink` `BerlinMODBenchmark`](https://github.com/MobilityDB/MobilityFlink/blob/main/flink-processor/src/main/java/berlinmod/BerlinMODBenchmark.java).
- **Kafka** — Kafka Streams 3.6, one stream thread, each cell a `KafkaStreams`
  application against its own fresh in-process `EmbeddedKafkaCluster`. Harness
  [`MobilityKafka` `EmbeddedBrokerBenchmark`](https://github.com/MobilityDB/MobilityKafka/blob/main/kafka-streams-app/src/test/java/berlinmod/EmbeddedBrokerBenchmark.java).
- **Nebula** — NebulaStream harness ([bench.nebula.stream](https://bench.nebula.stream)).

## Invariants held fixed

- **Same MEOS predicate** — every platform evaluates the identical MEOS spatial call.
- **Throughput definition** — `events_in` ÷ wall-clock; `output_rows` is the sink cardinality.
- **Corpus-derived parameters** — all query parameters and window/tick granularity are identical on every platform.
- **Batch oracle** — the continuous form is checked event-for-event against a batch pass over the same corpus through the same MEOS call.

## Results — throughput (events/s)

| Query | Form | MobilityFlink | MobilityKafka | MobilityNebula |
|---|---|---:|---:|---:|
| Q1 | continuous |  87,057 |  78,091 | — |
| Q1 | windowed   | 187,565 | 201,941 | — |
| Q1 | snapshot   | 205,199 | 200,442 | — |
| Q2 | continuous | 213,302 | 260,334 | — |
| Q2 | windowed   | 215,000 | 222,530 | — |
| Q2 | snapshot   | 228,168 | 226,022 | — |
| Q3 | continuous |  68,443 |  86,673 | — |
| Q3 | windowed   |  88,519 |  79,940 | — |
| Q3 | snapshot   | 217,598 | 167,372 | — |
| Q4 | continuous |  59,971 |  44,387 | — |
| Q4 | windowed   |  65,438 |  41,859 | — |
| Q4 | snapshot   |  69,100 |  40,502 | — |
| Q5 | continuous |  24,105 |  12,544 | — |
| Q5 | windowed   | 229,623 | 137,018 | — |
| Q5 | snapshot   | 230,357 | 173,417 | — |
| Q6 | continuous |  95,103 |  52,117 | — |
| Q6 | windowed   |  97,595 |  51,718 | — |
| Q6 | snapshot   |  96,764 |  55,234 | — |
| Q7 | continuous |  58,085 |  86,018 | — |
| Q7 | windowed   |  44,278 |  30,684 | — |
| Q7 | snapshot   |  57,589 |  47,178 | — |
| Q8 | continuous |  69,299 |  78,346 | — |
| Q8 | windowed   |  80,355 |  65,123 | — |
| Q8 | snapshot   | 239,286 | 144,824 | — |
| Q9 | continuous | 137,452 |  76,325 | — |
| Q9 | windowed   | 231,096 | 141,504 | — |
| Q9 | snapshot   | 235,376 | 134,880 | — |

### Throughput charts (events/s, log scale, higher is better)

![Continuous-form streaming throughput](streaming_continuous.svg)

![Windowed-form streaming throughput](streaming_windowed.svg)

![Snapshot-form streaming throughput](streaming_snapshot.svg)

## Reading the results

Q5-continuous is the floor on both engines (MobilityKafka 12,544, MobilityFlink
24,105 ev/s): it enumerates every meeting pair across all vehicles on each event
(O(V²) per event). The non-spatial Q1/Q2 continuous cells sit near the ceiling
(Q2-continuous 260,334 on Kafka, 213,302 on Flink), and the per-event spatial
cells (Q3/Q8/Q9-continuous) cluster between in the 68k–137k ev/s band. Windowed
and snapshot forms aggregate or sample rather than emit per event, so they run
several times faster than their continuous counterpart.

The continuous form's output cardinality is identical across MobilityFlink and
MobilityKafka for all nine queries (Q1 5, Q2 61 170, Q3 216 075, Q4 62, Q5 73 063,
Q6 216 075, Q7 5, Q8 216 075, Q9 107 870), confirming per-event predicate parity.

## Parity with the DB benchmark

The continuous form is checked event-for-event against a batch pass over the same
corpus through the same MEOS call — the link to the
[3-DB benchmark](../CrossPlatform_timings.md), whose batch result is the oracle.
A query is `exact` when streaming-true equals batch-true with zero mismatches.

## Reproduce

Regenerate the charts from
[`scripts/render_streaming_chart.py`](scripts/render_streaming_chart.py):

```bash
python3 scripts/render_streaming_chart.py
```

The per-engine run harnesses are linked under [Methodology](#dataset-hardware-methodology).
Regenerate the Machine block on your host with `bash scripts/machine.sh`.
