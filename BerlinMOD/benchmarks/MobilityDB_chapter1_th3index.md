# MobilityDB — BerlinMOD Chapter 1 — th3index + GiST/SP-GiST Benchmark

**Platform**: MobilityDB on PostgreSQL 17.8
**Build**: `feat/h3-static-geo-coverage` (MobilityDB PR #938 — th3index
temporal type + `geoToH3IndexSet` / `everIntersectsH3IndexSet_Th3Index`
static-geometry-to-cell-set public API; PR #940 applied to master via
the same MEOS link).
**Dataset**: BerlinMOD scalefactor 0.005 — 1620 trips × 141 vehicles
× 100 regions × 100 points × 100 periods.  Trip and region geometries
in EPSG:3857; `trip_h3` and `geoToH3IndexSet` arguments in EPSG:4326
(reprojected on load).
**H3 resolution**: 7 (cell edge ≈ 1.2 km).

---

## Result matrix (median of 3 runs per cell)

| Query | Index config | Time | Rows | Speedup |
|---|---|---:|---:|---:|
| **Q1** — vehicles passed regions (10) | none | 5951 ms | 77 | 1.0× |
| | GiST(trip) | 2397 ms | 77 | 2.48× |
| | SP-GiST(trip) | 1927 ms | 77 | **3.09×** |
| | GiST(trip_h3) + h3 prefilter | 5300 ms | 77 | 1.12× |
| | GiST(trip) + GiST(trip_h3) + h3 prefilter | 1918 ms | 77 | 3.10× |
| | SP-GiST(trip) + GiST(trip_h3) + h3 prefilter | 1881 ms | 77 | **3.16×** |
| **Q2** — regions × periods (10×10) | none | 31177 ms | 759 | 1.0× |
| | GiST(trip) | 31809 ms | 759 | 0.98× |
| | SP-GiST(trip) | 32366 ms | 759 | 0.96× |
| **Q4** — first-visit per point (10) | none | 5399 ms | 24 | 1.0× |
| | GiST(trip) | 5323 ms | 24 | 1.01× |
| | SP-GiST(trip) | 5363 ms | 24 | 1.01× |
| **Q6** — per-region wCount (10) | none | 7076 ms | 8 | 1.0× |
| | GiST(trip) | 3821 ms | 8 | 1.85× |
| | SP-GiST(trip) | 4083 ms | 8 | 1.73× |

All Q1 configurations return the same 77 rows — the h3 prefilter is
sound.

## Prefilter selectivity (the h3 cell-set as a `WHERE` clause)

| | Count |
|---|---:|
| Candidate trip × region pairs (cross-join) | 162 000 |
| `everIntersectsH3IndexSet_Th3Index(geoToH3IndexSet(R.geom4326, 7), T.trip_h3)` accepts | 55 720 |
| True `eIntersects(trip, region.geom)` hits | 3 836 |
| False negatives of the prefilter | 0 |

The prefilter accepts a strict superset of the semantic predicate and
reduces the cross-join to ≈ 35 % of candidates.  The downstream
`eIntersects` reduces those to 3 836 true hits.

## Soundness contract

`geoToH3IndexSet(P, res)` returns the set of H3 cells at the chosen
resolution whose interior intersects polygon P (or covers each
LINESTRING / POINT component).  Coverage is layered:

- `polygonToCells` (centroid containment) of each polygon ring, each
  result cell expanded by `gridDisk(c, 1)` to capture cells the polygon
  overlaps without containing their centroid.
- Each polygon vertex's containing cell, also expanded by
  `gridDisk(c, 1)`.  Covers polygons smaller than a hexagon at the
  chosen resolution (centroid containment contributes no cell in that
  regime).
- LINESTRING and POINT components covered by the per-type walkers.

`everIntersectsH3IndexSet_Th3Index(cells, th3idx)` returns true iff
any cell in the set appears in the trip's th3index value sequence.

The composition is sound (no false negatives) for the BerlinMOD
workload (resolution 7, trip sample spacing ≈ 80 m, cell edge ≈
1.2 km).  At higher H3 resolutions or sparser trip sampling, trip-side
densification (sampling the trip path between consecutive instants)
would be required to keep recall at 100 %.

## Index recommendations

- **Q1 / Q6** (eIntersects-style predicates against static geometry):
  spatial index on `trip` delivers the headline speedup.  GiST and
  SP-GiST are within run-to-run noise of each other; both are reasonable
  defaults.  Adding the h3 prefilter on top of either gives a marginal
  further reduction (3.16× vs 3.09×) at the cost of a second index and
  the prefilter clause.
- **Q1 with h3 alone** (GiST(trip_h3) without bbox index on trip):
  on this dataset the prefilter alone is not faster than a sequential
  scan — the per-row prefilter check trades off against the cross-join
  loop.  The h3 prefilter pays off in combination with a bbox index.
- **Q2** (`eIntersects(atTime(trip, period), region)`): no spatial index
  applies — `atTime` materializes a new tgeompoint per pair, so the
  pre-built bbox is not consulted.  Planner-aware fusion of the
  temporal restriction with the spatial check would help.
- **Q4** (`ST_Contains(trajectory(trip), point)`): trajectory
  materialization dominates; bbox indexes on the temporal type are
  not consulted for the static-trajectory path.

## Reproduce locally

```bash
# 1. Build mobilitydb on the right branch
git checkout feat/h3-static-geo-coverage
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DH3=ON \
      -DH3_LIBRARY=/usr/lib/x86_64-linux-gnu/libh3.so \
      -DH3_INCLUDE_DIR=/usr/include ..
make -j$(nproc) && sudo make install
/usr/local/pgsql/17/bin/pg_ctl -D /usr/local/pgsql/17/data restart -m fast

# 2. Prepare bench DB
dropdb berlinmod_h3bench 2>/dev/null
createdb berlinmod_h3bench
pg_dump berlinmod_gen | psql berlinmod_h3bench  # source DB has BerlinMOD
                                                #  generated at SF 0.005

# 3. Add 4326 columns + compute trip_h3 + rename vehicleid → vehid
psql berlinmod_h3bench <<SQL
ALTER TABLE trips RENAME COLUMN vehicleid TO vehid;
ALTER TABLE vehicles RENAME COLUMN vehicleid TO vehid;
ALTER TABLE trips ADD COLUMN trip4326 tgeompoint;
ALTER TABLE regions ADD COLUMN geom4326 geometry;
ALTER TABLE points ADD COLUMN geom4326 geometry;
UPDATE trips SET trip4326 = transform(trip, 4326);
UPDATE regions SET geom4326 = ST_Transform(geom, 4326);
UPDATE points SET geom4326 = ST_Transform(geom, 4326);
ALTER TABLE trips ADD COLUMN trip_h3 th3index;
UPDATE trips SET trip_h3 = h3_latlng_to_cell(trip4326, 7);
SQL

# 4. Run the matrix
./run_bench.sh
```

## Open work

- libh3 ≥ 4.2 enables `polygonToCellsExperimental` with
  `CONTAINMENT_OVERLAPPING`; the vertex+ring formulation here can be
  replaced with that single call once Ubuntu ships 4.2.
- Trip-side densification (sampling between consecutive instants) is
  required for sound prefiltering at higher H3 resolutions or sparser
  trip sampling.  The static `linestring_to_cells_into` walker already
  uses Nyquist-step sampling at edge/2 — the temporal converter can
  reuse the same primitive.
- Three-platform extension (MobilityDuck, MobilitySpark) — see the
  sibling readiness document.
