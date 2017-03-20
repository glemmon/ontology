#!/usr/bin/env bash

service neo4j-phevor stop

mvn clean package

cp --no-preserve mode,ownership target/phevor_plugins-1.0.jar /opt/neo4j-community/3.1.1-ontos/plugins/Plugins.jar

service neo4j-phevor start
