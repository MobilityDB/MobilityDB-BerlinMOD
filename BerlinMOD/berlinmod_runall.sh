#!/bin/bash
# A script to run the BerlinMOD generator

host=localhost
port=5433
dbowner=mahmoud
database=brussels
datapath=~/Desktop/MobilityDB/MobilityDB_BeforeNewGit/BerlinMOD/data/
scriptpath=~/Desktop/MobilityDB/MobilityDB-BerlinMOD/BerlinMOD/
scalefactor=0.005

dropdb -h $host -p $port -U $dbowner $database
createdb -h $host -p $port -U $dbowner $database
#psql -h $host -p $port -U $dbowner -d $database -c 'CREATE EXTENSION MobilityDB CASCADE'
#psql -h $host -p $port -U $dbowner -d $database -c 'CREATE EXTENSION pgRouting'
#osm2pgrouting -h $host -p $port -U $dbowner -f $datapath/brussels.osm --dbname $database -c mapconfig_$database.xml
#osm2pgsql -c -H $host -P $port -U $dbowner -d $database $datapath/brussels.osm
#pg_dump -h localhost -p 5433 -U mahmoud -W -F t brussels > brussels.tar
pg_restore --dbname=$database --create --verbose $datapath/brussels.tar
psql -h $host -p $port -U $dbowner -d $database -f $scriptpath/brussels_preparedata.sql
psql -h $host -p $port -U $dbowner -d $database -f $scriptpath/berlinmod_datagenerator_batch.sql
psql -h $host -p $port -U $dbowner -d $database -c 'select berlinmod_generate(scaleFactor := '$scalefactor')'
psql -h $host -p $port -U $dbowner -d $database -f $scriptpath/tests.sql
