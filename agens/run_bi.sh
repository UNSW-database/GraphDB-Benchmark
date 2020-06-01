#!/bin/bash

DB=$1
SEED_PATH=$2

for((i=1;i<=18;i++))
do
    python run_bi.py $DB $SEED_PATH $i > result/bi_$i.out
done

for((i=20;i<=25;i++))
do
    python run_bi.py $DB $SEED_PATH $i > result/bi_$i.out
done

python run_bi.py $DB $SEED_PATH 19 > result/bi_19.out