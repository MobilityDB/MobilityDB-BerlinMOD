/*****************************************************************************
 * This file distributes the Brussels synthetic dataset generated with the
 * BerlinMOD generator using the vehicleid as distribution column.
 * The file presupposes that the data generation has been done before
 * using the berlinmod_datagenerator function.
 *****************************************************************************/

-- Change in postgresql.conf max_worker_processes 8 (default) -> 16

CREATE EXTENSION IF NOT EXISTS citus;

-- TripId is the primary key of Trips and it is not the distribution column
ALTER TABLE Trips DROP CONSTRAINT trips_pkey;
-- cannot truncate a table referenced in a foreign key constraint by a local table
ALTER TABLE Licences DROP CONSTRAINT licences_vehicleid_fkey;

SELECT create_distributed_table('vehicles', 'vehicleid');
SELECT create_distributed_table('trips', 'vehicleid');

SELECT truncate_local_data_after_distributing_table($$public.vehicles$$);
SELECT truncate_local_data_after_distributing_table($$public.trips$$);

-- Views cannot be distributed or reference tables
DROP VIEW Licences1;
DROP VIEW Licences2;

CREATE TABLE Licences1 (LicenceId, Licence, VehicleId) AS
SELECT LicenceId, Licence, VehicleId
FROM Licences
LIMIT 10;
CREATE TABLE Licences2 (LicenceId, Licence, VehicleId) AS
SELECT LicenceId, Licence, VehicleId
FROM Licences
LIMIT 10 OFFSET 10;

DROP VIEW Instants1;
CREATE TABLE Instants1 (InstantId, Instant) AS
SELECT InstantId, Instant
FROM Instants
LIMIT 10;

DROP VIEW Periods1;
CREATE TABLE Periods1 (PeriodId, Period) AS
SELECT PeriodId, Period
FROM Periods
LIMIT 10;

DROP VIEW Points1;
CREATE TABLE Points1 (PointId, Geom) AS
SELECT PointId, Geom
FROM Points
LIMIT 10;

DROP VIEW Regions1;
CREATE TABLE Regions1 (RegionId, Geom) AS
SELECT RegionId, Geom
FROM Regions
LIMIT 10;

SELECT create_reference_table('municipalities');
SELECT create_reference_table('roadsegments');
SELECT create_reference_table('licences');
SELECT create_reference_table('licences1');
SELECT create_reference_table('licences2');
SELECT create_reference_table('instants');
SELECT create_reference_table('instants1');
SELECT create_reference_table('periods');
SELECT create_reference_table('periods1');
SELECT create_reference_table('points');
SELECT create_reference_table('points1');
SELECT create_reference_table('regions');
SELECT create_reference_table('regions1');

SELECT truncate_local_data_after_distributing_table($$public.municipalities$$);
SELECT truncate_local_data_after_distributing_table($$public.roadsegments$$);
SELECT truncate_local_data_after_distributing_table($$public.licences$$);
SELECT truncate_local_data_after_distributing_table($$public.licences1$$);
SELECT truncate_local_data_after_distributing_table($$public.licences2$$);
SELECT truncate_local_data_after_distributing_table($$public.instants$$);
SELECT truncate_local_data_after_distributing_table($$public.instants1$$);
SELECT truncate_local_data_after_distributing_table($$public.periods$$);
SELECT truncate_local_data_after_distributing_table($$public.periods1$$);
SELECT truncate_local_data_after_distributing_table($$public.points$$);
SELECT truncate_local_data_after_distributing_table($$public.points1$$);
SELECT truncate_local_data_after_distributing_table($$public.regions$$);
SELECT truncate_local_data_after_distributing_table($$public.regions1$$);

/*
  Problems with the 17 BerlinMOD Range queries

Q10:
ERROR:  complex joins are only supported when all distributed tables are co-located and joined on their distribution columns
-> CANNOT BE DONE

Q16:
ERROR:  the query contains a join that requires repartitioning
HINT:  Set citus.enable_repartition_joins to on to enable repartitioning
-> AFTER CHANGING THE SETTING IT WORKS

*/

