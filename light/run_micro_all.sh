#!/bin/bash

DB=$1
MICRO_SEED_PATH=$2

./run_ii.sh $DB $MICRO_SEED_PATH
./run_is.sh $DB $MICRO_SEED_PATH
./run_ch.sh $DB $MICRO_SEED_PATH
./run_id.sh $DB $MICRO_SEED_PATH
