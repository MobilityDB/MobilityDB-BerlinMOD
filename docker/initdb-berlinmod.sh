#!/bin/bash

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

# Load mobilitydb into both template_database and $POSTGRES_DB
echo "Loading Berlimod extension into $POSTGRES_DB"
"${psql[@]}" --dbname="$POSTGRES_DB" <<- 'EOSQL'
	CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;
	create extension pgrouting;
	create extension hstore;
EOSQL
