/*-----------------------------------------------------------------------------
-- BerlinMOD th3index Setup -- one-time, idempotent
-------------------------------------------------------------------------------

This file is part of MobilityDB.
Copyright(c) 2020-2026, Université libre de Bruxelles and MobilityDB
contributors

Run once after the standard BerlinMOD load, before sourcing
`berlinmod_chapter1_queries_th3index_portable.sql`.  The script is
idempotent — safe to re-run.

What it does:
  1. Adds `trip_h3 th3index` to the `Trips` table (NULL if absent).
  2. Populates it by converting the `Trip tgeompoint` column to a
     `th3index` at H3 resolution 7 via `h3_latlng_to_cell`.  Resolution
     7 (cell edge ~ 1.2 km) is the default for BerlinMOD; lower
     resolutions trade selectivity for coverage and may be preferable
     at larger scale factors.
  3. Builds a GiST index on `trip_h3` so the th3index prefilter
     predicates can be pushed down by the planner.
  4. Re-analyses the table.

This script is MobilityDB/PostgreSQL specific.  Each platform derives
`trip_h3` at ingest the same way — `h3_latlng_to_cell(trip, 7)` is an
O(1)-per-point conversion through the shared MEOS kernel (the same
vendored libh3), so the cells are identical on every engine.  The
MobilityDuck and MobilitySpark loaders run the equivalent step in their
own dialect; the column is not carried in the shared CSV.

-----------------------------------------------------------------------------*/

\timing OFF

ALTER TABLE Trips
  ADD COLUMN IF NOT EXISTS trip_h3 th3index;

UPDATE Trips
   SET trip_h3 = h3_latlng_to_cell(Trip, 7)
 WHERE trip_h3 IS NULL;

DROP INDEX IF EXISTS Trips_trip_h3_gist_idx;
CREATE INDEX Trips_trip_h3_gist_idx
  ON Trips USING GIST(trip_h3);

ANALYZE Trips;
