# TigerGraph Installation & Evaluation Guide

Note: The most scripts of loading data and running queries are from [TigerGraph benchmark](https://github.com/tigergraph/ecosys/tree/ldbc/ldbc_benchmark/tigergraph). We add and modify some code to adapt them to our experiments. The queries in directory ./queries/interactive_insert/ are implemented by ourselves, other are provided by TigerGraph Team.

## Installation

Environment:

​	OS: Ubuntu 16.04 LTS

​	Java version: 1.8.0_242

A few basic software packages can be installed with one of the following commands：	

```
sudo apt install tar curl cron iproute util-linux uuid-runtime net-tools coreutils openssh-client openssh-server sshpass policycoreutils
```

Install TigerGraph according the [official guide](https://docs.tigergraph.com/admin/admin-guide/installation-and-configuration/installation-guide#single-node-installation) :

Download the latest version of TigerGraph Community Edition from [TigerGraph website](https://dl.tigergraph.com/download.html) and extract the .tar file：

```
tar -xvf tigergraph-latest-developer.tar.gz 
```

Install TigerGraph Community Edition：

```
cd tigergraph-latest-developer
sudo ./install.sh
```

Note: The installer will ask you a few questions. It will create a new user, default is tigergraph(default password: tigergraph).

Use 'su' to switch to the tigergraph user account and confirm correct operation:

```
su tigergraph
gadmin status 
```

​	Note: If the system installed correctly, the command should report that zk , kafka , dict, ts3, nginx, gsql, and Visualization are up and ready. Since there is no graph data loaded yet, gse , gpe , and restpp are not initialized. 



## Load Data

Note: TigerGraph Community Edition only support single graph. So you must drop the loaded graph before loading a new graph. 

Load Data:

Set path to raw data directory:

```
cd load_scripts
. ./path.sh
```

Load in one step:

```
./one_step_load.sh
```

At the beginning of the loading job, you can find the following lines:

```
JobName: load_ldbc_snb, jobid: ldbc_snb_m1.<START_TIME_EPOCH>
Loading log: '<TIGERGRAPH_HOME>/logs/restpp/restpp_loader_logs/ldbc_snb/ldbc_snb_m1.<START_TIME_EPOCH>.log'
```

Then, you can check the time spent by running a python script with the full path to the log:

```
python time.py <TIGERGRAPH_HOME>/logs/restpp/restpp_loader_logs/ldbc_snb/ldbc_snb_m1.<START_TIME_EPOCH>.log
```

Get the storage size of loaded graph:

```
du -sh <TIGERGRAPH_HOME>/gstore/
```



## Run benchmark

Before running queries, TigerGraph requests to install queries:

```
cd queries
./install_queries.sh
```

Create a new folder to store results:

```
mkdir result
```

Run all queries can follow this command, $SEED_PATH  is the directory of parameters, $NUM_RUN_MACRO is the number of running IC & BI queries(can be any value for IU & IS), $QUERY_TYPE indicates the type of query(ii, id, is, ic or bi), if you want to  specify a query, you can add a number(like is_1,bi_2) , if not, you will run all queries of one type:

```
python driver_new.py -p  $SEED_PATH -n $NUM_RUN_MACRO -q $QUERY_TYPE -d 1 > result/$QUERY_TYPE_$DB
```

For example, run micro queries IU & IS for DG1:

As the parameters of IS queries are associated with the parameters of IU queries, you should firstly run IU queries, then run IS queries:

```
run IU: python driver_new.py -p  ./../seeds/seeds_1/ -n 3 -q ii -d 1 > result/i_insert_DG1
run IS: python driver_new.py -p  ./../seeds/seeds_1/ -n 3 -q is -d 1 > result/i_short_DG1
```

IU queries will destroy the original structure of datasets,  delete inserted vertexes and edges by ID queries:

```
run ID: python driver_new.py -p  ./../seeds/seeds_1/ -n 3 -q id -d 1 > result/i_delete_DG1
```

Note : As the inserted data must not contained in the original dataset, we adopt different parameters for micro queries in different scale datasets, such as seeds_1 for DG1, seeds_10 for DG10 and seeds_100 for DG100.



Run macro queries IU & IS:

Before running macro queries, make sure the datasets are original. If you insert data by IU queries, run ID queries to delete them. Then,  run IC or BI queries:

```
run IC: python driver_new.py -p  ./../seeds/seeds_macro/ -n 3 -q ic -d 1 > result/i_complex_DG1
run BI: python driver_new.py -p  ./../seeds/seeds_macro/ -n 3 -q bi -d 1 > result/business_i_DG1
```

Additionally, run all queries:

```
run all: ./run_all.sh DG1 ./../seeds/seeds_1/ ./../seeds/seeds_macro/
```

Note: We adopt the same parameters in three scale datasets and run 3 times for each query.