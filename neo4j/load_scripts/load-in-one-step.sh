#!/bin/bash
#export NEO4J_HOME=/home/zhiyi/ecosys/neo4j-community-3.5.0
#export NEO4J_DATA_DIR=/home/zhiyi/raw/snb/neo4j/social_network-1000
#export NEO4J_DB_DIR=$NEO4J_HOME/data/databases/snb-1000.db
#export POSTFIX=_0_0.csv

. ./path.sh

./delete-neo4j-database.sh && ./convert-data.sh && ./import-to-neo4j.sh && ./restart-neo4j.sh

./show-db-size.sh
