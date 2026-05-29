# Cross-platform streaming timings — 2026-05-29

Throughput of the streaming query set across the three stream platforms, per
(query, form). The Flink column is measured on the real BerlinMOD instants
corpus; the Nebula and Kafka columns are not yet measured. See
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

## Throughput (events/s) and output rows

| Query | Form | Flink events in | Flink output rows | Flink ev/s | Nebula ev/s | Kafka ev/s |
|---|---|---:|---:|---:|---:|---:|
| Q1 | continuous | 216075 | 5 | 86,154 | — | — |
| Q1 | windowed | 216075 | 86 | 166,982 | — | — |
| Q1 | snapshot | 216075 | 274 | 204,616 | — | — |
| Q2 | continuous | 216075 | 61170 | 201,187 | — | — |
| Q2 | windowed | 216075 | 50 | 210,394 | — | — |
| Q2 | snapshot | 216075 | 71 | 219,365 | — | — |
| Q3 | continuous | 216075 | 216075 | 73,796 | — | — |
| Q3 | windowed | 216075 | 86 | 86,189 | — | — |
| Q3 | snapshot | 216075 | 0 | 233,342 | — | — |
| Q4 | continuous | 216075 | 62 | 66,403 | — | — |
| Q4 | windowed | 216075 | 98 | 66,814 | — | — |
| Q4 | snapshot | 216075 | 1944 | 67,042 | — | — |
| Q5 | continuous | 216075 | 73063 | 23,586 | — | — |
| Q5 | windowed | 216075 | 6 | 226,494 | — | — |
| Q5 | snapshot | 216075 | 0 | 236,148 | — | — |
| Q6 | continuous | 216075 | 216075 | 90,712 | — | — |
| Q6 | windowed | 216075 | 203 | 81,940 | — | — |
| Q6 | snapshot | 216075 | 274 | 97,595 | — | — |
| Q7 | continuous | 216075 | 5 | 54,386 | — | — |
| Q7 | windowed | 216075 | 53 | 43,180 | — | — |
| Q7 | snapshot | 216075 | 288 | 54,967 | — | — |
| Q8 | continuous | 216075 | 216075 | 74,948 | — | — |
| Q8 | windowed | 216075 | 86 | 75,445 | — | — |
| Q8 | snapshot | 216075 | 126 | 232,839 | — | — |
| Q9 | continuous | 216075 | 107870 | 116,294 | — | — |
| Q9 | windowed | 216075 | 22 | 233,847 | — | — |
| Q9 | snapshot | 216075 | 95 | 217,818 | — | — |

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
