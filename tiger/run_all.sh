#!/bin/bash

DB=$1
MICRO_SEED_PATH=$2
MACRO_SEED_PATH=$3

python driver_new.py -p $MICRO_SEED_PATH -n 3 -q ii -d 1 > result/i_insert_$DB
python driver_new.py -p $MICRO_SEED_PATH -n 3 -q is -d 1 > result/i_short_$DB
python driver_new.py -p $MICRO_SEED_PATH -n 3 -q id -d 1 > result/i_delete_$DB

python driver_new.py -p $MACRO_SEED_PATH -n 3 -q ic -d 1 > result/i_complex_$DB
python driver_new.py -p $MACRO_SEED_PATH -n 3 -q bi -d 1 > result/business_i_$DB
