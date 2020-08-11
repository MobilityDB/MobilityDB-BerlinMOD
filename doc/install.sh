#!/bin/bash

if [ "$#" != "1" ]; then 
	echo "Usage: $0 <install-path>"
	exit 1
fi

installpath=$1

cd `dirname $0`

mkdir -p $installpath

xsltproc --stringparam html.stylesheet "docbook.css" --xinclude -o $installpath/index.html /usr/share/xml/docbook/stylesheet/docbook-xsl-ns/html/chunk.xsl mobilitydb-berlinmod.xml
dblatex -s texstyle.sty -T native -t pdf -o $installpath/mobilitydb-berlinmod.pdf mobilitydb-berlinmod.xml
dbtoepub -o $installpath/mobilitydb-berlinmod.epub mobilitydb-berlinmod.xml

cp docbook.css $installpath/
cp -r images $installpath/

