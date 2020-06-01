# AgensGraph Installation & Evaluation Guide

Note: Most scripts are implemented by ourselves. But some scripts such as proc.sql and weight_precal.sql are got from [AgensGraph benchmark](https://github.com/bitnine-oss/ldbc-snb-agensgraph). 

## Installation

Environment:

​	OS: Ubuntu 16.04 LTS

​	Java version: 1.8.0_242

AgensGraph Python [driver](https://github.com/bitnine-oss/agensgraph-python):

```
sudo apt-get update
sudo pip install -U pip
sudo apt-get install libpq-dev
sudo pip install psycopg2
wget https://github.com/bitnine-oss/agensgraph-python/archive/master.zip
unzip master.zip
python /path/to/agensgraph-python-master/setup.py install
```



Install AgensGraph according to [official website](https://bitnine.net/documentations/manual/agens_graph_quick_guide.html#installation):

Note: AgensGraph must be installed under the non-root user!

We build AgensGraph from the [source code](https://github.com/bitnine-oss/agensgraph):

```
wget https://github.com/bitnine-oss/agensgraph/archive/master.zip
unzip agensgraph-master.zip
cd agensgraph-master
sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison
./configure
make install
make install-world
```

Or you can get the pre-compiled binary from the AgensGraph [download page](https://bitnine.net/downloads).



Post-Installation Setup and Configuration:

Create a new folder in directory /path/to/agensgraph-master/

```
mkdir agdata
```

Add these commands into a shell start-up file, such as the ~/.bashrc.

```
export LD_LIBRARY_PATH=/path/to/agensgraph-master/lib:$LD_LIBRARY_PATH
export PATH=/path/to/agensgraph-master/bin:$PATH
export AGDATA=/path/to/agensgraph-master/agdata
```

Make it effective:

```
source ~/.bashrc
```

Create database cluster and initial it:

```
initdb -D /path/to/agensgraph-master/agdata
```

Start(stop) the server:

```
ag_ctl -D /path/to/agensgraph-master/agdata/ -l logfile start(stop)
```

Creating a database:

```
createdb [dbname]
```

​	Note: If *dbname* is not specified, a database with the same name as the **current user** is created, by default.

Execute the interactive terminal:

```
agens [dbname]
```



## Load Data

Preprocess data and set path to raw data directory:

```
cd load_scripts
. ./path.sh
```

Note: make sure you have the read and write permission to the raw data directory

Convert data and load data:

```
. ./convert-data.sh
nohup time agens -v cwd=$AGENS_DATA_DIR -f agens-import-ldbc.sql [dbname] > agens_load.log 2>&1 &
```

Create index and reindex:

```
nohup time agens -f create_vertex_indexes.sql [dbname] > agens_index.log 2>&1  &
nohup time agens -f reindex.sql [dbname] > agens_reindex.log 2>&1  &
```

The loaded data is stored in directory: /path/to/agensgraph-master/agdata/base/***

Delete or clean loaded data:

```
agens -f agens-clean.sql [dbname] 
```

Note:  we adopt the name 'ldbc_graph' as the default name of graph. If you want to load a new graph, you must delete the loaded graph, or rename graph and change  'graph_path' in all *.sql files.



## Run Benchmark

Create a new folder to store results:

```
mkdir result
```

Run micro queries IU & IS:

As the parameters of IS queries are associated with the parameters of IU queries, you should firstly run IU queries, then run IS queries:

```
run IU: ./run_ii.sh DG1 ./../seeds/seeds_1/
run IS: ./run_is.sh DG1 ./../seeds/seeds_1/
```

CH queries can check inserted results. IU queries will destroy the original structure of datasets,  delete inserted vertexes and edges by ID queries:

```
run CH: ./run_ch.sh DG1 ./../seeds/seeds_1/
run ID: ./run_id.sh DG1 ./../seeds/seeds_1/
```

Note : As the inserted data must not contained in the original dataset, we adopt different parameters for micro queries in different scale datasets, such as seeds_1 for DG1, seeds_10 for DG10 and seeds_100 for DG100.



Run macro queries IU & IS:

Before running macro queries, make sure the datasets are original. If you insert data by IU queries, run ID queries to delete them. Then,  run IC or BI queries:	

```
run IC: ./run_ic.sh DG1 ./../seeds/seeds_macro/
run BI: ./run_bi.sh DG1 ./../seeds/seeds_macro/
```



Additionally, run all queries:

```
run all: ./run_all.sh DG1 ./../seeds/seeds_1/ ./../seeds/seeds_macro/
```

Note: 

1. We adopt the same parameters in three scale datasets and run 3 times for each query. 
2.  IU & BI queries are implemented by ourselves, while the implementation of IS & IC queries is referred to AgensGraph's implementation, you can find more information from their [experiments](https://github.com/bitnine-oss/ldbc-snb-agensgraph).
3. When running some queries, like BI_19,  an error 'server is closed' will happen. This error will cause the later queries cannot be executed normally. We suggest put it at the end to execute.