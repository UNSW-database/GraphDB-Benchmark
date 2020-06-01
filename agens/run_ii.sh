#!/bin/bash

DB=$1
SEED_PATH=$2

for((i=1;i<=8;i++))
do
    python run_ii.py $DB $SEED_PATH $i > result/ii_$i.out
done
