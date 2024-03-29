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

*  In HTML format: https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/
*  In PDF format: https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/mobilitydb-berlinmod.pdf
* In EPUB format: https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/mobilitydb-berlinmod.epub

Docker container
-----------------

The dependencies and scripts of the MobilityDB-BerlinMOD Project are available in a Docker container running PostgreSQL-12, PostGIS-2.5 and MobilityDB-develop.

*  Pull the prebuilt image from the [Docker Hub Registry](https://hub.docker.com/r/mobilitydb/mobilitydb).

        docker pull mobilitydb/mobilitydb:15-3.4-1.1-BerlinMOD

*  Create a Docker volume to preserve the PostgreSQL database files outside of the container.

        docker volume create mobilitydb_data
        
 *  Run the Docker container.

        docker run --name "mobilitydb" -d -p 5432 -v mobilitydb_data:/var/lib/postgresql mobilitydb/mobilitydb:15-3.4-1.1-BerlinMOD 
        
 *  Enter into the Docker container.

        docker exec -it mobilitydb bash
        
 *  Connect to the database  (username=docker,db=mobilitydb).

        psql -U docker -d mobilitydb 

 *  BerlinMOD scripts are available in the BerlinMOD directory inside the container.
        
        
License
-------

The documentation of this benchmark is licensed under a [Creative Commons Attribution-Share Alike 3.0 License](https://creativecommons.org/licenses/by-sa/3.0/)
