# BerlinMOD three-platform stream benchmark

## What this benchmark measures

Per-query throughput of the BerlinMOD streaming query set across three stream
engines — [MobilityFlink](https://github.com/MobilityDB/MobilityFlink),
[MobilityKafka](https://github.com/MobilityDB/MobilityKafka), and
[MobilityNebula](https://github.com/MobilityDB/MobilityNebula) — that share the
same MEOS kernel and evaluate the same predicates over the same canonical corpus.

Every query runs in three forms: **continuous** (per-event output),
**windowed** (aggregate over tumbling windows), and **snapshot** (watermark-
driven sample). The snapshot form is checked against the
[3-DB batch result](../batch/CrossPlatform_timings.md) as the correctness oracle.

## Workload

The 9-query streaming set (see [`README.md`](README.md) for intents and
BerlinMOD/r lineage) in three forms:

| Form | Question | Notes |
|---|---|---|
| **Continuous** | "At every moment, which holds now?" | per-event output, watermark-independent |
| **Windowed** | "Per tumbling window, what holds?" | event-time tumbling window |
| **Snapshot** | "At time T, what holds?" | watermark-driven; the batch parity oracle |

## Dataset, hardware, methodology

The corpus is BerlinMOD `berlinmod_instants.csv` — 216 075 instants, 5 vehicles,
~11 days — reprojected EPSG:3857→EPSG:4326 through MEOS `geo_transform` at load.
All query parameters and window/tick granularity are derived from the same corpus
and are identical on every platform.

### Machine

- **CPU** — AMD Ryzen 9 5900HX with Radeon Graphics (8 cores / 16 threads)
- **Memory** — 23 GiB
- **OS** — Ubuntu 24.04.4 LTS, kernel `6.6.87.2-microsoft-standard-WSL2`
- **Runtime** — openjdk 21.0.11
- **libmeos** — built `-DMEOS=ON -DCBUFFER=ON -DNPOINT=ON -DPOSE=ON -DRGEO=ON`

### Per-engine harnesses

- **Flink** — Flink 1.16 single-node local mini-cluster, parallelism 1. Harness:
  [`BerlinMODBenchmark`](https://github.com/MobilityDB/MobilityFlink/blob/main/flink-processor/src/main/java/berlinmod/BerlinMODBenchmark.java).
- **Kafka** — Kafka Streams 3.6, one stream thread, each cell a `KafkaStreams`
  application against its own fresh in-process `EmbeddedKafkaCluster`. Harness:
  [`EmbeddedBrokerBenchmark`](https://github.com/MobilityDB/MobilityKafka/blob/main/kafka-streams-app/src/test/java/berlinmod/EmbeddedBrokerBenchmark.java).
- **Nebula** — NebulaStream harness ([bench.nebula.stream](https://bench.nebula.stream)).

## Invariants held fixed

- **Same MEOS predicate** — every platform evaluates the identical MEOS spatial call.
- **Throughput definition** — `events_in` ÷ wall-clock; `output_rows` is the sink cardinality.
- **Corpus-derived parameters** — all query parameters and window/tick granularity are identical on every platform.
- **Batch oracle** — the snapshot form is checked against the 3-DB batch result on the same corpus.

---

## Section 1 — Continuous throughput (baseline)

The continuous form is the baseline: every arriving event is evaluated and emits
a result immediately, with no aggregation or sampling. Platform is the sole
variable.

![Continuous-form streaming throughput (log scale, higher is better)](streaming_continuous.svg)

| Query | MobilityFlink | MobilityKafka | MobilityNebula |
|---|---:|---:|---:|
| Q1 |  87,057 |  78,091 | — |
| Q2 | 213,302 | 260,334 | — |
| Q3 |  68,443 |  86,673 | — |
| Q4 |  59,971 |  44,387 | — |
| Q5 |  24,105 |  12,544 | — |
| Q6 |  95,103 |  52,117 | — |
| Q7 |  58,085 |  86,018 | — |
| Q8 |  69,299 |  78,346 | — |
| Q9 | 137,452 |  76,325 | — |

MobilityNebula runs the same corpus and query set via
[bench.nebula.stream](https://bench.nebula.stream) and fills its column when
the harness reports.

Q5-continuous is the floor on both engines (MobilityKafka 12,544,
MobilityFlink 24,105 ev/s): it enumerates every meeting pair across all vehicles
on each event (O(V²) per event). The non-spatial Q1/Q2 cells sit near the
ceiling, and the per-event spatial cells (Q3/Q8/Q9) cluster at 68k–137k ev/s.

---

## Section 2 — Form acceleration: windowed and snapshot

Windowed and snapshot forms aggregate or sample rather than emit per event, so
they run several times faster than their continuous counterpart for queries with
aggregation-amenable predicates.

![Windowed-form streaming throughput (log scale, higher is better)](streaming_windowed.svg)

![Snapshot-form streaming throughput (log scale, higher is better)](streaming_snapshot.svg)

### All forms (events/s, higher is better)

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

### Reading the form acceleration

- **Q5** shows the largest lift: continuous 24k–12k ev/s (O(V²) pair
  enumeration per event) versus windowed 230k–137k ev/s (window aggregates the
  pair set once per window boundary). Windowed is **9–11×** faster than continuous.
- **Q3, Q8, Q9** — snapshot jumps 2–3× over continuous (snapshot samples the
  predicate at tick instants; most events are irrelevant between ticks).
- **Q6, Q4** — minimal form difference; the predicate cost dominates and is
  evaluated once per event regardless of form.

---

## Section 3 — Snapshot parity with the DB benchmark

The snapshot form bridges the two benchmark families. By contract, the stream
snapshot at watermark `T` equals the batch result on the same data up to `T`.
The [3-DB batch result](../batch/CrossPlatform_timings.md) is the oracle; the
batch document's execution times are the latency counterpart to the stream
throughput figures above.

The continuous form is checked event-for-event against a batch pass over the
same corpus through the same MEOS call.

### Per-query parity (MobilityFlink and MobilityKafka)

Output cardinality is identical across MobilityFlink and MobilityKafka for all
nine queries, confirming per-event predicate parity:

| Query | Output rows (continuous) | Parity |
|---|---:|---|
| Q1 |        5 | exact |
| Q2 |   61,170 | exact |
| Q3 |  216,075 | exact |
| Q4 |       62 | exact |
| Q5 |   73,063 | exact |
| Q6 |  216,075 | exact |
| Q7 |        5 | exact |
| Q8 |  216,075 | exact |
| Q9 |  107,870 | exact |

MobilityNebula fills its parity column when the harness reports.

---

## Reading the results

- **Platform choice** — Flink leads on Q3/Q5/Q6/Q8/Q9 continuous; Kafka leads on
  Q2/Q4/Q7 continuous. The platform decision is operational (fault tolerance,
  deployment model), not predicate-driven.
- **Form choice** — continuous for per-event alerting; windowed for periodic
  aggregates (Q5 9–11× win); snapshot for watermark-aligned state that must
  match a batch oracle.
- **Cross-family** — the snapshot form is the bridge: snapshot throughput at T
  equals batch latency on data up to T. The stream and batch benchmarks are the
  same workload on the same canonical data, not separate experiments.

## Filling your engine's column

Each engine fills the grids that apply to it, leaving the rest as `—`.

1. **Section 1** — run all 9 queries in the continuous form; fill your engine's
   throughput column.
2. **Section 2** — run windowed and snapshot forms; fill all three form rows per
   query.
3. **Section 3** — record output row counts and confirm `snapshot_equals_batch`
   against the [batch oracle](../batch/CrossPlatform_timings.md).
4. Add your series to [`scripts/render_chart.py`](scripts/render_chart.py) and
   run `python3 scripts/render_chart.py` to refresh the SVGs.

## Reproduce

Regenerate the charts:

```bash
python3 scripts/render_chart.py
```

The per-engine run harnesses are linked under
[Per-engine harnesses](#per-engine-harnesses). Regenerate the Machine block on
your host with `bash scripts/machine.sh`.
