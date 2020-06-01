#!/bin/bash

DB=$1
SEED_PATH=$2

for((i=1;i<=8;i++))
do
    python run_ch.py $DB $SEED_PATH $i > result/ch_$i.out
done
