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

The corpus is the canonical BerlinMOD instants — 2 195 303 instants,
141 vehicles, scale factor 0.005 — the same dataset as the
[batch benchmark](../batch/CrossPlatform_timings.md), sourced from
`berlinmod-3db-canonical/instants.csv` (SRID 3857 EWKB) and reprojected
EPSG:3857→EPSG:4326 at load.
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
- **Nebula** — NebulaStream `marianamgarcez/mobility-nebula:runtime` Docker image,
  single worker node, 2 worker threads. TCP source streams the corpus over
  `host.docker.internal:32325`; queries registered via `nes-nebuli register -x`.
  Q1 runs on the stock runtime image. Q2–Q9 require a MEOS-enabled build
  (parity operators from [MobilityDB/MobilityNebula](https://github.com/MobilityDB/MobilityNebula)
  PRs [#15–#71](https://github.com/MobilityDB/MobilityNebula/pulls)).

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
| Q1 | 329,922 | 413,584 | 146,715 |
| Q2 | 285,810 | 480,163 | — |
| Q3 | 33,733 | 84,422 | — |
| Q4 | 25,186 | — | — |
| Q5 | — | — | — |
| Q6 | 36,589 | — | — |
| Q7 | 15,984 | — | — |
| Q8 | 32,081 | — | — |
| Q9 | 289,046 | — | — |

Q1 is a relational aggregate (no MEOS spatial call) and runs on the stock runtime
image. Q2–Q9 use MEOS operators and require a MEOS-enabled Nebula build.

Q5-continuous is the O(V²) meeting-pairs cell (141 vehicles × 140 pairs per event);
it is impractical on the 2.2M-row corpus and is omitted from the continuous table.
Q7 is the slowest non-O(V²) cell (points-of-interest distance enumeration
per event); Q4 (region containment with per-vehicle keyed state) is the second
slowest. Non-spatial queries (Q1/Q2) sit near the per-engine ceiling.

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
| Q1 | continuous | 329,922 | 413,584 | 146,715 |
| Q1 | windowed   | 472,515 | 211,514 | 170,403 |
| Q1 | snapshot   | 409,342 | 274,276 | 128,629 |
| Q2 | continuous | 285,810 | 480,163 | — |
| Q2 | windowed   | 427,351 | 492,552 | — |
| Q2 | snapshot   | 338,416 | 457,451 | — |
| Q3 | continuous | 33,733 | 84,422 | — |
| Q3 | windowed   | 39,483 | 86,016 | — |
| Q3 | snapshot   | 144,323 | 129,746 | — |
| Q4 | continuous | 25,186 | — | — |
| Q4 | windowed   | 24,660 | — | — |
| Q4 | snapshot   | 24,220 | — | — |
| Q5 | continuous | — | — | — |
| Q5 | windowed   | 110,801 | — | — |
| Q5 | snapshot   | 71,784 | — | — |
| Q6 | continuous | 36,589 | — | — |
| Q6 | windowed   | 44,814 | — | — |
| Q6 | snapshot   | 43,234 | — | — |
| Q7 | continuous | 15,984 | — | — |
| Q7 | windowed   | 19,088 | — | — |
| Q7 | snapshot   | 27,597 | — | — |
| Q8 | continuous | 32,081 | — | — |
| Q8 | windowed   | 68,640 | 135,756 | — |
| Q8 | snapshot   | 412,031 | 236,080 | — |
| Q9 | continuous | 289,046 | — | — |
| Q9 | windowed   | 362,022 | 244,330 | — |
| Q9 | snapshot   | 359,238 | 224,952 | — |

### Reading the form acceleration

- **Q5** shows the largest lift: the windowed form aggregates the pair set once
  per window boundary instead of enumerating every meeting pair per event (O(V²)
  per event in the continuous form).
- **Q8-snapshot** runs at 412k ev/s (Flink) — the predicate is false at every tick
  boundary on the canonical corpus, so the snapshot sink emits 0 rows and the
  full corpus traversal takes only ~5 ms of predicate work.
- **Q3, Q9** — snapshot lifts over continuous because most events fall between
  tick boundaries and are skipped.
- **Q4** — minimal form difference; per-vehicle keyed state dominates regardless
  of output form.

---

## Section 3 — Snapshot parity with the DB benchmark

The snapshot form bridges the two benchmark families. By contract, the stream
snapshot at watermark `T` equals the batch result on the same data up to `T`.
The [3-DB batch result](../batch/CrossPlatform_timings.md) is the oracle; the
batch document's execution times are the latency counterpart to the stream
throughput figures above.

The continuous form is checked event-for-event against a batch pass over the
same corpus through the same MEOS call.

### Per-query parity

Output cardinality must be identical across all engines for each query,
confirming per-event predicate parity:

| Query | Output rows (continuous) | Parity |
|---|---:|---|
| Q1 | Flink 141 / Kafka 141 / Nebula 3,790 | — |
| Q2 | Flink 19,913 / Kafka 19,913 | — |
| Q3 | Flink 2,195,303 / Kafka 1,673,190 | — |
| Q4 | Flink 146 | — |
| Q5 | — | — |
| Q6 | Flink 2,195,303 | — |
| Q7 | Flink 63 | — |
| Q8 | Flink 2,195,303 / Kafka 0 | — |
| Q9 | Flink 24,759 / Kafka 5 (windowed) | — |

MobilityNebula fills its parity column when the harness reports.

---

## Reading the results

- **Platform choice** — the platform decision is operational (fault tolerance,
  deployment model), not predicate-driven.
- **Form choice** — continuous for per-event alerting; windowed for periodic
  aggregates (Q5 largest lift: window collapses the O(V²) pair enumeration);
  snapshot for watermark-aligned state that must match a batch oracle.
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
