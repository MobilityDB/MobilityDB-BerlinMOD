#!/bin/bash
# A script to run the BerlinMOD generator
# Run this script inside the BerlinMOD/ folder.
# This script expects the file brussels.osm
# to be in the BerlinMOD/ folder.

# Moodify these configs as needed
host=localhost
port=5432
dbowner=postgres
database=berlinmod
scalefactor=0.005

dropdb -h $host -p $port -U $dbowner $database
createdb -h $host -p $port -U $dbowner $database

psql -h $host -p $port -U $dbowner -d $database -c 'CREATE EXTENSION MobilityDB CASCADE'
psql -h $host -p $port -U $dbowner -d $database -c 'CREATE EXTENSION pgRouting'
osm2pgrouting -h $host -p $port -U $dbowner -f ./brussels.osm --dbname $database -c mapconfig.xml
osm2pgsql -c -H $host -P $port -U $dbowner -d $database ./brussels.osm

# pg_dump -h localhost -p 5433 -U $dbowner -W -F t brussels > brussels.tar
# pg_restore --dbname=$database --create --verbose ./brussels.tar

psql -h $host -p $port -U $dbowner -d $database -f ./brussels_preparedata.sql
psql -h $host -p $port -U $dbowner -d $database -f ./berlinmod_datagenerator.sql
psql -h $host -p $port -U $dbowner -d $database -c 'select berlinmod_datagenerator(scaleFactor := '$scalefactor')'
# psql -h $host -p $port -U $dbowner -d $database -f ./tests.sql
