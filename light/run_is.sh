#!/bin/bash

DB=$1
SEED_PATH=$2

for((i=1;i<=7;i++))
do
    python run_is.py $DB $SEED_PATH $i > result/is_$i.out
done
