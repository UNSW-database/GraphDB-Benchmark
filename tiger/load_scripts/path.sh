
### change to raw data file folder
export LDBC_SNB_DATA_DIR=/home/tigergraph/ldbc_snb_data/social_network/
### somehow LDBC SNB datagen doesn't get any benefit from multithreads. fix it to the single file for each vertex/edge
export LDBC_SNB_DATA_POSTFIX=_0_0.csv
