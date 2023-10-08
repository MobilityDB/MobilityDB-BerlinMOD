ARG POSTGRES_VERSION
ARG POSTGIS_VERSION

FROM postgis/postgis:$POSTGRES_VERSION-$POSTGIS_VERSION

# Configuration Parameters
LABEL maintainer="MobilityDB Project - https://github.com/MobilityDB/MobilityDB"
ENV POSTGRES_DB=mobilitydb
ENV POSTGRES_USER=docker
ENV POSTGRES_PASSWORD=docker
ENV MOBILITYDB_VERSION $MOBDB_VERSION

# Fix the Release file expired problem
RUN echo "Acquire::Check-Valid-Until \"false\";\nAcquire::Check-Date \"false\";" | cat > /etc/apt/apt.conf.d/10no--check-valid-until


# Install Prerequisites
RUN apt-get update \
 && apt-get install -y \
    build-essential \
    cmake \
    git \
    libproj-dev \
    g++ \
    wget \
    unzip \
    autoconf \
    autotools-dev \
    libgeos-dev \
    libpq-dev \
    libproj-dev \
    libjson-c-dev \
    protobuf-c-compiler \
    xsltproc \
    libgsl-dev \
    libgslcblas0 \
    postgresql-server-dev-${PG_MAJOR} \
    postgresql-15-pgrouting \ 
    osm2pgrouting \
    osm2pgsql \
  && rm -rf /var/lib/apt/lists/*

# Install MobilityDB
RUN git clone https://github.com/MobilityDB/MobilityDB.git -b develop /usr/local/src/MobilityDB && \
    mkdir -p /usr/local/src/MobilityDB/build && \
    cd /usr/local/src/MobilityDB/build && \
    cmake .. && \
    make -j$(nproc) && \
    make install

# Download BerlinMOD Data
RUN mkdir /BerlinMOD
RUN cd /BerlinMOD && \
    wget -O - https://github.com/MobilityDB/MobilityDB-BerlinMOD/archive/master.tar.gz | tar -xz --strip=2 "MobilityDB-BerlinMOD-master/BerlinMOD" && \
    wget -O - https://docs.mobilitydb.com/data/brussels_osm.zip && \
    unzip brussels_osm.zip && \
    rm -f brussels_osm.zip

RUN rm /docker-entrypoint-initdb.d/10_postgis.sh
COPY ./docker/initdb-mobilitydb.sh /docker-entrypoint-initdb.d/mobilitydb.sh
RUN chmod +x /docker-entrypoint-initdb.d/mobilitydb.sh
