# Cross-platform streaming timings — 2026-05-29

Throughput of the streaming query set across the three stream platforms, per
(query, form). The Flink and Kafka columns are measured on the same real
BerlinMOD instants corpus; the Nebula column is not yet measured. See
[`README.md`](README.md) for the query set, the streaming forms, and the shared
result schema.

## Method

Each cell runs as one streaming job over a shared corpus and is terminated by a
counting sink; throughput is input events ÷ wall-clock and `output rows` is the
sink cardinality. The spatial predicates evaluate through MEOS. The Flink figures
are the BerlinMOD `berlinmod_instants.csv` (216 075 instants, 5 vehicles, ~11
days), reprojected EPSG:3857→EPSG:4326 through MEOS `geo_transform` at load, on a
single-node Flink 1.16 local mini-cluster, parallelism 1, Java 21, 16-core
x86-64 Linux, libmeos built `-DMEOS=ON -DCBUFFER=ON -DNPOINT=ON -DPOSE=ON
-DRGEO=ON`. The point `P`, region box, road segment, points of interest, and
target vehicle ids are derived from the corpus (`P` = centroid), and the
window/tick granularity is scaled to the corpus span. The harness is
[`MobilityFlink` `BerlinMODBenchmark`](https://github.com/MobilityDB/MobilityFlink/blob/main/flink-processor/src/main/java/berlinmod/BerlinMODBenchmark.java).

The Kafka figures use the same corpus and the same corpus-derived parameters on
Kafka Streams 3.6: each cell runs as one `KafkaStreams` application against its
own fresh in-process `EmbeddedKafkaCluster` (a real `KafkaServer` over the
loopback network), one stream thread, throughput = events consumed ÷ wall-clock
once the application has read the whole input topic. The harness is
[`MobilityKafka` `EmbeddedBrokerBenchmark`](https://github.com/MobilityDB/MobilityKafka/blob/main/kafka-streams-app/src/test/java/berlinmod/EmbeddedBrokerBenchmark.java).

## Throughput (events/s) and output rows

| Query | Form | Flink events in | Flink output rows | Flink ev/s | Nebula ev/s | Kafka ev/s |
|---|---|---:|---:|---:|---:|---:|
| Q1 | continuous | 216075 | 5 | 86,154 | — | 113,785 |
| Q1 | windowed | 216075 | 86 | 166,982 | — | 130,956 |
| Q1 | snapshot | 216075 | 274 | 204,616 | — | 127,029 |
| Q2 | continuous | 216075 | 61170 | 201,187 | — | 167,631 |
| Q2 | windowed | 216075 | 50 | 210,394 | — | 126,806 |
| Q2 | snapshot | 216075 | 71 | 219,365 | — | 128,388 |
| Q3 | continuous | 216075 | 216075 | 73,796 | — | 46,790 |
| Q3 | windowed | 216075 | 86 | 86,189 | — | 51,594 |
| Q3 | snapshot | 216075 | 0 | 233,342 | — | 82,883 |
| Q4 | continuous | 216075 | 62 | 66,403 | — | 23,095 |
| Q4 | windowed | 216075 | 98 | 66,814 | — | 22,182 |
| Q4 | snapshot | 216075 | 1944 | 67,042 | — | 20,581 |
| Q5 | continuous | 216075 | 73063 | 23,586 | — | 9,440 |
| Q5 | windowed | 216075 | 6 | 226,494 | — | 74,612 |
| Q5 | snapshot | 216075 | 0 | 236,148 | — | 107,715 |
| Q6 | continuous | 216075 | 216075 | 90,712 | — | 24,814 |
| Q6 | windowed | 216075 | 203 | 81,940 | — | 34,473 |
| Q6 | snapshot | 216075 | 274 | 97,595 | — | 29,771 |
| Q7 | continuous | 216075 | 5 | 54,386 | — | 50,006 |
| Q7 | windowed | 216075 | 53 | 43,180 | — | 18,587 |
| Q7 | snapshot | 216075 | 288 | 54,967 | — | 28,860 |
| Q8 | continuous | 216075 | 216075 | 74,948 | — | 44,950 |
| Q8 | windowed | 216075 | 86 | 75,445 | — | 36,340 |
| Q8 | snapshot | 216075 | 126 | 232,839 | — | 76,487 |
| Q9 | continuous | 216075 | 107870 | 116,294 | — | 47,375 |
| Q9 | windowed | 216075 | 22 | 233,847 | — | 90,636 |
| Q9 | snapshot | 216075 | 95 | 217,818 | — | 94,729 |

## Throughput charts (events/s, log scale, higher is better)

One grouped bar chart per streaming form, Flink against Kafka on the same corpus.
The Nebula column is not yet measured and is omitted from the bars. The charts
are regenerated from
[`scripts/render_streaming_chart.py`](scripts/render_streaming_chart.py) — run
`python3 scripts/render_streaming_chart.py` to refresh all three SVGs.

![Continuous-form streaming throughput, Flink vs Kafka](streaming_continuous.svg)

![Windowed-form streaming throughput, Flink vs Kafka](streaming_windowed.svg)

![Snapshot-form streaming throughput, Flink vs Kafka](streaming_snapshot.svg)

## Parity — streaming ≡ batch on the same MEOS predicate

The continuous form emits `predicate(event)` for every event, checked
event-for-event against a batch pass over the same corpus through the same MEOS
call. On the 216 075-event corpus both spatial-membership queries match exactly,
which is the cross-family link to the 3-DB benchmark (the batch result is the
oracle).

| Query | Events | Streaming-true | Batch-true | Mismatches | Parity |
|---|---:|---:|---:|---:|---|
| Q3 (`edwithin_tgeo_geo`, within `d` of `P`) | 216075 | 56086 | 56086 | 0 | exact |
| Q8 (`edwithin_tgeo_geo`, within `d` of segment) | 216075 | 118498 | 118498 | 0 | exact |

## Characteristics

Q5-continuous enumerates every meeting pair across all vehicles on each event
(O(V²) per event, keyed to a single subtask) — the lowest throughput. The
snapshot form is a sampled form (each vehicle's last-known position at tick
instants), so a within-`P` snapshot can be empty when no vehicle is within `d` of
`P` at a tick boundary even though the continuous form reports near-`P` events
between boundaries.

Kafka Streams routes every record through the broker (produce then fetch), so its
per-event spatial cells (Q3/Q8/Q9-continuous) run below Flink's in-JVM
mini-cluster pipeline on the same corpus, while the per-cell shape matches across
both engines — the O(V²) Q5-continuous is the floor and the non-spatial Q1/Q2
cells the ceiling.
