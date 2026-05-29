# Cross-platform streaming timings — 2026-05-29

Throughput of the streaming query set across the three stream platforms, per
(query, form). The Flink column is measured; the Nebula and Kafka columns are
not yet measured. See [`README.md`](README.md) for the query set, the streaming
forms, and the shared result schema.

## Method

Each cell runs as one streaming job over a shared synthetic BerlinMOD corpus and
is terminated by a counting sink; throughput is input events ÷ wall-clock and
`output rows` is the sink cardinality. The spatial predicates evaluate through
MEOS. The Flink figures below are a 30 000-event corpus (50 vehicles × 600
events over 600 s of event time) on a single-node Flink 1.16 local mini-cluster,
parallelism 1, Java 21, 16-core x86-64 Linux, with libmeos built
`-DMEOS=ON -DCBUFFER=ON -DNPOINT=ON -DPOSE=ON -DRGEO=ON`. The wall-clock includes
the per-job mini-cluster startup (~0.7–0.8 s), so these are end-to-end job
throughputs; the steady-state row below amortises startup over a larger corpus.

## Throughput (events/s) and output rows

| Query | Form | Flink events in | Flink output rows | Flink ev/s | Nebula ev/s | Kafka ev/s |
|---|---|---:|---:|---:|---:|---:|
| Q1 | continuous | 30000 | 50 | 13,435 | — | — |
| Q1 | windowed | 30000 | 60 | 28,169 | — | — |
| Q1 | snapshot | 30000 | 6000 | 33,520 | — | — |
| Q2 | continuous | 30000 | 600 | 37,594 | — | — |
| Q2 | windowed | 30000 | 60 | 39,894 | — | — |
| Q2 | snapshot | 30000 | 120 | 38,810 | — | — |
| Q3 | continuous | 30000 | 30000 | 19,023 | — | — |
| Q3 | windowed | 30000 | 60 | 23,603 | — | — |
| Q3 | snapshot | 30000 | 3120 | 34,722 | — | — |
| Q4 | continuous | 30000 | 8 | 27,248 | — | — |
| Q4 | windowed | 30000 | 480 | 26,525 | — | — |
| Q4 | snapshot | 30000 | 960 | 27,959 | — | — |
| Q5 | continuous | 30000 | 7171498 | 403 | — | — |
| Q5 | windowed | 30000 | 14359 | 32,787 | — | — |
| Q5 | snapshot | 30000 | 29640 | 27,804 | — | — |
| Q6 | continuous | 30000 | 30000 | 29,326 | — | — |
| Q6 | windowed | 30000 | 3000 | 31,283 | — | — |
| Q6 | snapshot | 30000 | 6000 | 32,293 | — | — |
| Q7 | continuous | 30000 | 15 | 23,585 | — | — |
| Q7 | windowed | 30000 | 766 | 23,006 | — | — |
| Q7 | snapshot | 30000 | 1790 | 20,422 | — | — |
| Q8 | continuous | 30000 | 30000 | 28,763 | — | — |
| Q8 | windowed | 30000 | 60 | 29,528 | — | — |
| Q8 | snapshot | 30000 | 3720 | 36,320 | — | — |
| Q9 | continuous | 30000 | 1199 | 39,012 | — | — |
| Q9 | windowed | 30000 | 60 | 37,927 | — | — |
| Q9 | snapshot | 30000 | 120 | 42,493 | — | — |

## Steady-state per-event predicate

Q3-continuous applies one MEOS `edwithin_tgeo_geo` per event. Over a
200 000-event corpus with the per-job startup amortised:

| Query | Form | Flink events in | Flink output rows | Flink ev/s |
|---|---|---:|---:|---:|
| Q3 | continuous | 200000 | 200000 | 45,096 |

## Characteristics

Q5-continuous enumerates every meeting pair across all vehicles on each event
(O(V²) per event, keyed to a single subtask), producing 7 171 498 rows from
30 000 events — the lowest throughput, inherent to the all-pairs meeting query
rather than to the predicate path.
