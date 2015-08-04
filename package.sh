#!/bin/bash
set -e
bin/sbt "project zipkin-web" "debian:gen-changes"
cd zipkin-web/target
for i in `ls *.changes`
do
    debsign --no-re-sign $i
done

cd ../../
bin/sbt "project zipkin-query-service" "debian:gen-changes"
cd zipkin-query-service/target
for i in `ls *.changes`
do
    debsign --no-re-sign $i
done

cd ../../

