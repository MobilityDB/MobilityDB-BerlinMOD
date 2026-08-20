BerlinMOD Benchmark for MobilityDB
==================================

<img src="docs/images/mobilitydb-logo.svg" width="200" alt="MobilityDB Logo" />

[MobilityDB](https://github.com/MobilityDB/MobilityDB) is an open source
software program that adds support for temporal and spatio-temporal objects to
the [PostgreSQL](https://www.postgresql.org/) database and its spatial
extension [PostGIS](http://postgis.net/).

This repository contains code and documentation for running the
[BerlinMOD](https://secondo-database.github.io/BerlinMOD/BerlinMOD.html)
benchmark on MobilityDB.


Benchmark results
-----------------

Benchmark reports for the BerlinMOD query sets on each ecosystem
platform (MobilityDB, MobilityDuck, MobilitySpark) live in
**[`BerlinMOD/benchmarks/`](BerlinMOD/benchmarks/)**.  The cross-platform q01–q17 + qrt reproducer (MobilityDB + MobilityDuck + MobilitySpark) is in [`BerlinMOD/benchmarks/batch/bench/`](BerlinMOD/benchmarks/batch/bench/).  Start at the directory [README](BerlinMOD/benchmarks/README.md), or jump directly to:

- **[CrossPlatform_timings.md](BerlinMOD/benchmarks/batch/CrossPlatform_timings.md)**
  — cross-platform timings, with the `cross_platform_*.svg` figures.
- **[streaming/](BerlinMOD/benchmarks/stream/)** — the streaming benchmark
  (continuous / windowed / snapshot) with its figures.
- **[BETA_TESTING.md](BerlinMOD/benchmarks/batch/REPRODUCE.md)** — entry
  point for testers: query files, expected row counts, report-back
  template.
- **[MobilityDB_rqueries.md](BerlinMOD/benchmarks/batch/MobilityDB_rqueries.md)**
  — 17 R-queries × index matrix on MobilityDB.

Headline result (MobilityDB, BerlinMOD scalefactor 0.005, single run):
17 R-queries total wall-clock 334.30 s baseline → 173.23 s with GiST on
trip + trajectory (~1.9× total speedup; per-query highlights up to
Q14 51×, Q10 / Q15 8×, Q13 6×).  Row counts identical across the
three platforms.

## 1. Requirements

- [PostgreSQL](https://www.postgresql.org/) 14 or later
- [PostGIS](http://postgis.net/) 3.0 or later
- [MobilityDB](https://github.com/MobilityDB/MobilityDB) 1.1 or later
- [pgRouting](https://pgrouting.org/) (for data generation)
- [osm2pgrouting](https://github.com/pgRouting/osm2pgrouting) and
  [osm2pgsql](https://osm2pgsql.org/) (for importing OSM road network data)

## 2. Building / Installation

**Create the database and load the road network:**

```bash
createdb berlinmod
psql -d berlinmod -c 'CREATE EXTENSION MobilityDB CASCADE'
psql -d berlinmod -c 'CREATE EXTENSION pgRouting'
```

Import OSM data for Brussels (or another city) using `osm2pgrouting` and
`osm2pgsql`:

```bash
osm2pgrouting -f brussels.osm --dbname berlinmod -c BerlinMOD/mapconfig.xml
osm2pgsql -c -d berlinmod brussels.osm
```

Prepare the road network graph:

```bash
psql -d berlinmod -f BerlinMOD/brussels_preparedata.sql
```

Alternatively, use the optimized graph builder:

```bash
psql -d berlinmod -f BerlinMOD/brussels_creategraph.sql
```

## 3. Using

**Generate BerlinMOD synthetic data:**

Load the data generator and call it with a scale factor:

```sql
\i BerlinMOD/berlinmod_datagenerator.sql
SELECT berlinmod_datagenerator(scaleFactor := 0.005);
```

**Generate Deliveries synthetic data:**

```sql
\i BerlinMOD/deliveries_datagenerator.sql
SELECT deliveries_datagenerator(scaleFactor := 0.005);
```

**Run all steps with the shell script:**

```bash
cd BerlinMOD
bash berlinmod_runall.sh
```

**Run benchmark queries:**

After loading data (see [Generated datasets](#5-examples) below for
pre-generated CSV files), execute the benchmark queries:

```sql
-- Chapter 1 ad-hoc queries (range, temporal aggregate, distance)
\i BerlinMOD/berlinmod_chapter1_queries.sql

-- Range queries (17 BerlinMOD/R queries)
\i BerlinMOD/berlinmod_load.sql
SELECT berlinmod_R_queries(1, true);

-- Nearest-neighbor queries (9 BerlinMOD/NN queries)
SELECT berlinmod_NN_queries(1, true);
```

**Load pre-generated CSV data:**

```sql
\i BerlinMOD/berlinmod_load.sql
SELECT berlinmod_load('/path/to/csv/files/', true);
```

## 4. Cross-Platform Portability

BerlinMOD queries can run unchanged on all three platforms of the MobilityDB
ecosystem using the **portable SQL dialect** (named functions only, no
platform-specific operator symbols):

| Platform | Engine | Extension |
|---|---|---|
| [MobilityDB](https://github.com/MobilityDB/MobilityDB) | PostgreSQL | `CREATE EXTENSION mobilitydb` |
| [MobilityDuck](https://github.com/MobilityDB/MobilityDuck) | DuckDB | `LOAD mobilitydb` |
| [MobilitySpark](https://github.com/MobilityDB/MobilitySpark) | Apache Spark | `MobilitySparkSession.create(spark)` |

**Run portable Chapter 1 queries (Q1–Q6) on MobilityDB:**

```sql
\i BerlinMOD/berlinmod_chapter1_queries_portable.sql
```

**Export data for MobilityDuck / MobilitySpark:**

The `berlinmod_portability_export()` function writes seven CSV files in the
shared cross-platform schema:

```sql
\i BerlinMOD/berlinmod_export.sql
SELECT berlinmod_portability_export('/path/to/output/', 3812);
```

This produces:

| File | Contents |
|------|----------|
| `vehicles.csv` | `vehId, licence, type, model` |
| `trips.csv` | `tripId, vehId, trip` — tgeompoint as hex-EWKB (SRID embedded) |
| `query_licences.csv` | `licenceId, licence` |
| `query_instants.csv` | `instantId, instant` |
| `query_points.csv` | `pointId, geom` — geometry as EWKT (SRID-tagged) |
| `query_periods.csv` | `periodId, period` — tstzspan as text |
| `query_regions.csv` | `regionId, geom` — geometry as EWKT (SRID-tagged) |

These files are loaded by the cross-platform runners in
[`BerlinMOD/benchmarks/batch/bench/`](BerlinMOD/benchmarks/batch/bench/) —
`bench_mbdb.sh` (PostgreSQL), `bench_mduck.sh` (DuckDB) and `bench_mspark.sh`
(Spark) — which share the canonical query set `queries.sql`.

## 5. Running the Tests

After running the benchmark queries, compare results against expected output
to validate correctness. The documentation (see below) specifies expected
result counts for each query at each scale factor.

Generate the documentation from source to obtain the full reference:

```bash
cd docs
dblatex -s texstyle.sty -T native -t pdf -o mobilitydb-berlinmod.pdf mobilitydb-berlinmod.xml
```

Pre-generated documentation is available online:

- HTML: https://mobilitydb.github.io/MobilityDB-BerlinMOD/html/index.html
- PDF: https://mobilitydb.github.io/MobilityDB-BerlinMOD/mobilitydb-berlinmod.pdf
- EPUB: https://mobilitydb.github.io/MobilityDB-BerlinMOD/mobilitydb-berlinmod.epub

## 6. Examples

The generator produces two benchmark scenarios:

**BerlinMOD** — vehicles moving through the Brussels road network.

| Scale Factor | Vehicles | Days | Trips | File | Size |
|:-------------|--------:|-----:|------:|:-----|-----:|
| SF 0.1 | 632 | 11 | 18,910 | [brussels_sf0.1.zip](https://docs.mobilitydb.com/pub/brussels_sf0.1.zip) | 539 MB |
| SF 0.2 | 894 | 15 | 35,319 | [brussels_sf0.2.zip](https://docs.mobilitydb.com/pub/brussels_sf0.2.zip) | 937 MB |
| SF 0.5 | 1,414 | 22 | 81,584 | [brussels_sf0.5.zip](https://docs.mobilitydb.com/pub/brussels_sf0.5.zip) | 2.2 GB |
| SF 1 | 2,000 | 30 | 157,565 | [brussels_sf1.zip](https://docs.mobilitydb.com/pub/brussels_sf1.zip) | 4.2 GB |

**Deliveries** — vehicles making deliveries from warehouses to customers.

| Scale Factor | Warehouses | Vehicles | Customers | Days | Deliveries | File | Size |
|:-------------|----------:|---------:|----------:|-----:|-----------:|:-----|-----:|
| SF 0.1 | 32 | 632 | 3,162 | 11 | 6,320 | [deliveries_sf0.1.zip](https://docs.mobilitydb.com/pub/deliveries_sf0.1.zip) | 1.4 GB |
| SF 0.2 | 45 | 894 | 4,472 | 15 | 11,622 | [deliveries_sf0.2.zip](https://docs.mobilitydb.com/pub/deliveries_sf0.2.zip) | 2.6 GB |
| SF 0.5 | 71 | 1,414 | 7,071 | 22 | 26,866 | [deliveries_sf0.5.zip](https://docs.mobilitydb.com/pub/deliveries_sf0.5.zip) | 6.1 GB |
| SF 1 | 100 | 2,000 | 10,000 | 30 | 26,866 | [deliveries_sf1.zip](https://docs.mobilitydb.com/pub/deliveries_sf1.zip) | 11.8 GB |

**Docker container:**

A Docker image with all dependencies pre-installed is available:

```bash
docker pull mobilitydb/mobilitydb:15-3.4-1.1-BerlinMOD
docker volume create mobilitydb_data
docker run --name mobilitydb -e POSTGRES_PASSWORD=mysecretpassword \
  -p 25432:5432 -v mobilitydb_data:/var/lib/postgresql \
  -d mobilitydb/mobilitydb:15-3.4-1.1-BerlinMOD
psql -h localhost -p 25432 -U postgres
```

BerlinMOD scripts are available in the `BerlinMOD/` directory inside the
container. See the [Docker documentation](docker/) for further details.

## 7. Project Structure

```
MobilityDB-BerlinMOD/
├── BerlinMOD/
│   ├── berlinmod_datagenerator.sql      # BerlinMOD data generator
│   ├── deliveries_datagenerator.sql     # Deliveries data generator
│   ├── berlinmod_load.sql               # Load pre-generated CSV data
│   ├── deliveries_load.sql              # Load pre-generated deliveries CSV
│   ├── berlinmod_export.sql             # Export data to CSV (incl. berlinmod_portability_export)
│   ├── deliveries_export.sql            # Export deliveries data to CSV
│   ├── berlinmod_chapter1_queries.sql   # Ad-hoc benchmark queries
│   ├── berlinmod_chapter1_queries_portable.sql  # Portable dialect queries
│   ├── berlinmod_r_queries.sql          # BerlinMOD/R range queries
│   ├── berlinmod_r_queries_citus.sql    # Range queries for Citus
│   ├── berlinmod_nn_queries.sql         # BerlinMOD/NN nearest-neighbor queries
│   ├── berlinmod_d_vehicleid.sql        # Citus distribution by vehicle ID
│   ├── berlinmod_runall.sh              # Shell script to run all steps
│   ├── brussels_preparedata.sql         # Prepare Brussels road network
│   └── brussels_creategraph.sql         # Optimized graph builder
├── docs/                                # DocBook documentation source
├── docker/                              # Docker setup files
└── README.md
```

## Contributing

This repository uses a single perennial **`master`** branch — the same model
as [MobilityDB](https://github.com/MobilityDB/MobilityDB) itself:

1. Fork this repository.
2. Create a feature or fix branch on your fork.
3. Open a pull request against `MobilityDB/MobilityDB-BerlinMOD:master`.

There are no long-lived release branches; tags mark stable snapshots.

## License

The SQL scripts in this repository are provided under the
[PostgreSQL License](LICENSE).

The documentation of this benchmark is licensed under a
[Creative Commons Attribution-Share Alike 3.0 License](https://creativecommons.org/licenses/by-sa/3.0/).
