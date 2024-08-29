BerlinMOD Benchmark for MobilityDB
==================================

<img src="docs/images/mobilitydb-logo.svg" width="200" alt="MobilityDB Logo" />

[MobilityDB](https://github.com/ULB-CoDE-WIT/MobilityDB) is an open source software program that adds support for temporal and spatio-temporal objects to the [PostgreSQL](https://www.postgresql.org/) database and its spatial extension [PostGIS](http://postgis.net/).

This repository contains code and the documentation for running the [BerlinMOD](http://dna.fernuni-hagen.de/secondo/BerlinMOD/BerlinMOD.html) benchmark on MobilityDB.

Documentation
-------------

You can generate the benchmark documentation from the sources.
*  In HTML format

        xsltproc --stringparam html.stylesheet "docbook.css" --xinclude -o index.html /usr/share/xml/docbook/stylesheet/docbook-xsl/html/chunk.xsl mobilitydb-berlinmod.xml
*  In PDF format

        dblatex -s texstyle.sty -T native -t pdf -o mobilitydb-berlinmod.pdf mobilitydb-berlinmod.xml
* In EPUB format

        dbtoepub -o mobilitydb-berlinmod.epub mobilitydb-berlinmod.xml

In addition, pregenerated versions of them are available.

*  In HTML format: https://mobilitydb.github.io/MobilityDB-BerlinMOD/html/index.html
*  In PDF format: https://mobilitydb.github.io/MobilityDB-BerlinMOD/mobilitydb-berlinmod.pdf
* In EPUB format: https://mobilitydb.github.io/MobilityDB-BerlinMOD/mobilitydb-berlinmod.epub

Docker container
-----------------

The dependencies and scripts of the MobilityDB-BerlinMOD Project are available in a Docker container running PostgreSQL-15, PostGIS-2.5 and MobilityDB-develop.

*  Pull the prebuilt image from the [Docker Hub Registry](https://hub.docker.com/r/mobilitydb/mobilitydb).

        docker pull mobilitydb/mobilitydb:15-3.4-1.1-BerlinMOD

*  Create a Docker volume to preserve the PostgreSQL database files outside of the container.

        docker volume create mobilitydb_data
        
 *  Run the Docker container.

        docker run --name mobilitydb -e POSTGRES_PASSWORD=mysecretpassword \
        -p 25432:5432 -v mobilitydb_data:/var/lib/postgresql -d mobilitydb/mobilitydb:15-3.4-1.1-BerlinMOD 
        
 *  Connect to the database  (db=postgres,username=postgres,pw=_mysecretpassword_).

        psql -h localhost -p 25432 -U postgres 

 *  BerlinMOD scripts are available in the BerlinMOD directory inside the container.
      
Generated datasets
---------------------

The generator has two scenarios, the original one from BerlinMOD, and another one concerning deliveries pertaining to mobility data warehouses. We have generated the benchmark data for different scale factors (SF) for the two scenarios The different datasets and their characteristics are given in the tables below, that also provides links to download the data in compressed CSV files.

BerlinMOD synthetic data using OSM data from Brussels.

| Scale Factor | Vehicles | Days | Trips | File | Size |
|--------------|---------:|-----:|------:|-----|-----:|
| SF 0.1 |   632 | 11 |  18,910 | [brussels_sf0.1.zip](https://docs.mobilitydb.com/pub/brussels_sf0.1.zip) | 5.5 MB |
| SF 0.2 |   894 | 15 |  35,319 | [brussels_sf0.2.zip](https://docs.mobilitydb.com/pub/brussels_sf0.2.zip) | 9.6 MB |
| SF 0.5 | 1,414 | 22 |  81,584 | [brussels_sf0.5.zip](https://docs.mobilitydb.com/pub/brussels_sf0.5.zip) | 2.2 GB |
| SF 1   | 2,000 | 30 | 157,565 | [brussels_sf1.zip](https://docs.mobilitydb.com/pub/brussels_sf1.zip) | 4.2 GB |


Deliveries synthetic data using OSM data from Brussels.


| Scale Factor | Warehouses | Vehicles | Customers | Days | Deliveries | File | Size |
|--------------|-----------:|---------:|----------:|------|-----------:|-----|-----:|
| SF 0.1       |  32 |   632 |  3,162 | 11 |  6,320 | [deliveries_sf0.1.zip](https://docs.mobilitydb.com/pub/deliveries_sf0.1.zip) | 1.4 GB |
| SF 0.2       |  45 |   894 |  4,472 | 15 | 11,622 | [deliveries_sf0.2.zip](https://docs.mobilitydb.com/pub/deliveries_sf0.2.zip) | 2.6 GB |
| SF 0.5       |  71 | 1,414 |  7,071 | 22 | 26,866 | [deliveries_sf0.5.zip](https://docs.mobilitydb.com/pub/deliveries_sf0.5.zip) | 6.1 GB |
| SF 1         | 100 | 2,000 | 10,000 | 30 | 26,866 | [deliveries_sf1.zip](https://docs.mobilitydb.com/pub/deliveries_sf1.zip) | 11.8 GB |
  

License
-------

The documentation of this benchmark is licensed under a [Creative Commons Attribution-Share Alike 3.0 License](https://creativecommons.org/licenses/by-sa/3.0/)
