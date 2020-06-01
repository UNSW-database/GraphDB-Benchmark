#!/bin/bash

DB=$1
SEED_PATH=$2

for((i=1;i<=25;i++))
do
    python run_bi.py $DB $SEED_PATH $i > result/bi_$i.out
done
