#!/bin/bash

DB=$1
SEED_PATH=$2

for((i=1;i<=14;i++))
do
    python run_ic.py $DB $SEED_PATH $i > result/ic_$i.out
done
