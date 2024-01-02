# Neo4j Installation & Evaluation Guide

 Note: The loading data scripts and some scripts of running queries are from [other benchmark](https://github.com/zhuang29/graph_database_benchmark/tree/master/neo4j). We add and modify some code to adapt them to our experiments. 

## Installation

Environment:

​	OS: Ubuntu 16.04 LTS

​	Java version: 1.8.0_242

Python modules:

```
sudo apt-get install python-pip python-dev build-essential 
sudo pip install --upgrade pip 
sudo pip install --upgrade virtualenv 
sudo pip install tornado
sudo pip install neo4j-driver
sudo pip install requests
sudo apt-get install libcurl4-openssl-dev
sudo apt-get install libssl-dev
sudo pip install pycurl
```



Install Neo4j:

Download the latest version of Neo4j Community Edition from [Neo4j website]( https://neo4j.com/download-center/#releases) and extract the .tar file under a directory, eg: /data/database/Neo4j/

```
wget https://neo4j.com/artifact.php?name=neo4j-community-3.5.14-unix.tar.gz
tar -xvf neo4j-community-3.5.14-unix.tar.gz
```

Set enviroment variable $NEO4J_HOME:

```
export NEO4J_HOME=/data/database/Neo4j/neo4j-community-3.5.14-unix
```

Download APOC:

```
wget https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/3.5.0.2/apoc-3.5.0.2-all.jar
mv apoc-3.5.0.2-all.jar $NEO4J_HOME/plugins/
```

Configure Neo4j memory:	

```
$NEO4J_HOME/bin/neo4j-admin memrec
```

Based on the above, the following memory settings are recommended. Append the 3 lines to $NEO4J_HOME/conf/neo4j.conf:

```
dbms.memory.heap.initial_size=28000m
dbms.memory.heap.max_size=28000m
dbms.memory.pagecache.size=54500m
```

Append following lines in $NEO4J_HOME/conf/neo4j.conf:

```
dbms.security.procedures.unrestricted=apoc.*
```



Start server:	

```
$NEO4J_HOME/bin/neo4j start 
```

Create/change username and password cypher-shell, then exit:

1. Firstly login:
   
   ```
   $NEO4J_HOME/bin/cypher-shell
   user:neo4j
   pass:neo4j
   ```
   
2. change password:

   ```
   neo4j> CALL dbms.changePassword('benchmark')
   ```

3. exit shell:

   ```
   neo4j> :exit
   ```

4. log in again:

   ```
   $NEO4J_HOME/bin/cypher-shell -u neo4j -p benchmark
   ```

5. Alternatively, login with no password

   ```
   vim $NEO4J_HOME/conf/neo4j.conf
   uncomment line 26
   ```

Stop server:

```
$NEO4J_HOME/bin/neo4j stop
```



## Load Data

Preprocess and Set path to Neo4j installation directory, Neo4j database directory and raw data directory::

```
cd load_scripts
. ./path.sh
```

Note: make sure you have the read and write permission to the raw data directory

Before loading new data, you can backup the loaded data and rollback when use them:

```
backup: cp -r graph.db graph.db.bak
rollback: rm -r graph.db; cp -r graph.db.bak graph.db
```

Convert data and load data:

```
./convert-data.sh
./delete-neo4j-database.sh
./import-to-neo4j.sh
./restart-neo4j .sh
```

​	or

```
./load-in-one-step.sh (including the above 4 steps)
```

Record Neo4j loaded data size:

```
du -sh $NEO4j_HOME/data/database/graph.db
```



Create Index

To create all indexes:	

```
$NEO4J_HOME/bin/cypher-shell < ./index-ldbc.cql
```

Or to create indexes one by one, execute each statement in cypher-shell:

```
neo4j>CREATE INDEX ON :Message(id);
......
```

Monitor indexes population, wait until 100% finished:

```
neo4j>CALL db.indexes;
```

Record Neo4j index creation time:

```
./index_time.sh
```

Record Neo4j index size:

```
du -sh $NEO4J_HOME/data/databases/graph.db/schema/index/
```



## Run benchmark

Warm up NEO4J, wait until finshed and keep the cypher-shell open(warm up may take a long time):

```
$NEO4J_HOME/bin/cypher-shell
neo4j>CALL apoc.warmup.run(true, true);
```

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

Note: We adopt the same parameters in three scale datasets and run 3 times for each query.
