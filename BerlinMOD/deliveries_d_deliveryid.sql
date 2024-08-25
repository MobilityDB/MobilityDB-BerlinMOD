/*****************************************************************************
 * This file distributes the Deliveries synthetic dataset generated with the
 * BerlinMOD generator using the deliveryid as distribution column.
 * The file presupposes that the data generation has been done before
 * using the deliveries_datagenerator function.
 *****************************************************************************/

-- Change in postgresql.conf max_worker_processes 8 (default) -> 16

CREATE EXTENSION IF NOT EXISTS citus;

ALTER TABLE Deliveries DROP CONSTRAINT deliveries_pkey;
ALTER TABLE Segments DROP CONSTRAINT segments_pkey;

SELECT create_distributed_table('deliveries', 'deliveryid');
SELECT truncate_local_data_after_distributing_table($$public.deliveries$$);

SELECT create_distributed_table('segments', 'deliveryid');
SELECT truncate_local_data_after_distributing_table($$public.segments$$);

DROP VIEW Instants1;
CREATE TABLE Instants1 (InstantId, Instant) AS
SELECT InstantId, Instant
FROM Instants
LIMIT 10;

DROP VIEW Periods1;
CREATE TABLE Periods1 (PeriodId, StartTime, EndTime, Period) AS
SELECT PeriodId, StartTime, EndTime, Period
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

CREATE TABLE Trips1 AS
SELECT *
FROM Trips
LIMIT 100;

SELECT create_reference_table('date');
SELECT create_reference_table('municipalities');
SELECT create_reference_table('roadsegments');
SELECT create_reference_table('vehiclebrands');
SELECT create_reference_table('vehicleclasses');
SELECT create_reference_table('vehicles');
SELECT create_reference_table('instants');
SELECT create_reference_table('instants');
SELECT create_reference_table('periods');
SELECT create_reference_table('periods1');
SELECT create_reference_table('points');
SELECT create_reference_table('points1');
SELECT create_reference_table('regions');
SELECT create_reference_table('regions1');

SELECT truncate_local_data_after_distributing_table($$public.date$$);
SELECT truncate_local_data_after_distributing_table($$public.municipalities$$);
SELECT truncate_local_data_after_distributing_table($$public.roadsegments$$);
SELECT truncate_local_data_after_distributing_table($$public.vehiclebrands$$);
SELECT truncate_local_data_after_distributing_table($$public.vehicleclasses$$);
SELECT truncate_local_data_after_distributing_table($$public.vehicles$$);
SELECT truncate_local_data_after_distributing_table($$public.instants$$);
SELECT truncate_local_data_after_distributing_table($$public.instants1$$);
SELECT truncate_local_data_after_distributing_table($$public.periods$$);
SELECT truncate_local_data_after_distributing_table($$public.periods1$$);
SELECT truncate_local_data_after_distributing_table($$public.points$$);
SELECT truncate_local_data_after_distributing_table($$public.points1$$);
SELECT truncate_local_data_after_distributing_table($$public.regions$$);
SELECT truncate_local_data_after_distributing_table($$public.regions1$$);

------------------------------------------------------------------------------
