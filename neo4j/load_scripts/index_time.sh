#!/bin/bash
. ./path.sh

grep -n -r 'Index population started' $NEO4J_HOME/logs/debug.log > index-t0.out
sed -i 's/[^0-9]//g' index-t0.out
sed -i 's/^...//g' index-t0.out
sed -i 's/......$//g' index-t0.out

grep -n -r 'Index creation finished' $NEO4J_HOME/logs/debug.log > index-tn.out
sed -i 's/[^0-9]//g' index-tn.out
sed -i 's/^...//g' index-tn.out
sed -i 's/......$//g' index-tn.out
