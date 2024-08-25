ARG POSTGRES_VERSION
ARG POSTGIS_VERSION
ARG MOBILITYDB_VERSION

FROM mobilitydb/mobilitydb:$POSTGRES_VERSION-$POSTGIS_VERSION-$MOBILITYDB_VERSION

# Configuration Parameters
LABEL maintainer="MobilityDB Project - https://github.com/MobilityDB/MobilityDB"

# Fix the Release file expired problem
RUN echo "Acquire::Check-Valid-Until \"false\";\nAcquire::Check-Date \"false\";" | cat > /etc/apt/apt.conf.d/10no--check-valid-until

# 1. Install Prerequisites
# 2. Get, build and install MobilityDB
# 3. Uninstall prerequisites
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    wget \
    postgresql-"${PG_MAJOR}"-pgrouting \
    osm2pgrouting \
    osm2pgsql \
  && rm -rf /var/lib/apt/lists/* \
  && cp ../docker/initdb-mobilitydb.sh /docker-entrypoint-initdb.d/12_berlinmod.sh \
  && mkdir /BerlinMOD \
  && cd /BerlinMOD \
  && wget -O - https://github.com/MobilityDB/MobilityDB-BerlinMOD/archive/master.tar.gz | tar -xz --strip=2 "MobilityDB-BerlinMOD-master/BerlinMOD" \
  && wget https://docs.mobilitydb.com/data/brussels_osm.zip \
  && unzip brussels_osm.zip && \
  && rm -f brussels_osm.zip \
  && cd / \
  && apt-get purge -y --auto-remove \
    build-essential \
    cmake \
    wget
