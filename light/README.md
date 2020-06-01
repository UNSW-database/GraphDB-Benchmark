# LightGraph Installation & Evaluation Guide

## For IU & IS queries

Note: LightGraph is not free. You must apply for a trial license from the [official website](https://fma-ai.cn/). The trial version of LightGraph only can be installed and used via **docker**. The following guide is only for **IU & IS** queries,  the guide for IC & BI queries is presented later in this doc.



### Installation

Environment:

​	OS: Ubuntu 16.04 LTS

​	Java version: 1.8.0_242

Get the docker image of LightGraph:

```
docker pull fmacloud/lgraph:latest 
```

​	or	

```
wget https://fma-ai.cn/download/lgraph_latest.tar 
docker load -i lgraph_latest.tar
```

Start docker, -v is the mapping directory, {host_data_dir} is the directory where you want to save the data, -p is used for port mapping, {container_id} is the container id of Docker, which can be obtained through **docker ps**:	

```
docker run -d -v {host_data_dir}:/mnt -p 7090:7090 -it fmacloud/lgraph:latest
docker exec -it {container_id} bash  
```

I usually enter docker container to do experiments. As the queries are executed vis REST API, you should install *curl* in docker container:

```
apt-get update
apt-get install curl
```



### Load Data & Start Server

Copy raw data to docker container and enter docker container:

```
docker cp /path/to/DG1/ {container_id}:/root/light/data/
docker exec -it {container_id} bash  
```

Preprocess and Set path to raw data directory, loaded graph directory and license directory:

```
cd load_scripts
. ./path.sh
./convert_data.sh
cp  data_import.conf $LIGHT_DATA_DIR
cd $LIGHT_DATA_DIR
```

Load data:	

```
lgraph_import -c $LIGHT_DATA_DIR/data_import.conf --dir $LGRAPH_DB_DIR --overwrite 1 --online false
```

Record LightGraph loaded data size:

```
du -sh $LGRAPH_DB_DIR
```

Start LightGraph server:

```
cd ./light/load_scripts
lgraph_server -c lgraph_server.json --license $LIC_DIR --directory $LGRAPH_DB_DIR -d start
```



Create Index

LightGraph automatically create index for ID attributes. Thus, for IU&IS, you do not need to create other indexes. But if want to create indexes, you could following:

```
Login to get ${jwt}: curl -XPOST -H "Content-Type: application/json" -s "http://127.0.0.1:7071/login" -d'{"user":"admin","password":"admin123456"}' 

Replace ${jwt} and create index: curl -XPOST -H "Authorization:Bearer ${jwt}" -H "Content-Type: application/json" "http://127.0.0.1:7071/db/default/index" -d'{"label": "Person", "field": "firstName", "is_unique" : false}'
```



### Run benchmark

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

Optionally, run all micro queries in one step:

```
run all micro queries: ./run_micro_all.sh DG1 ./../seeds/seeds_1/
```



## For IC & BI queries

The related docker image and operation guide is provide by the [LightGraph staff](https://fma-ai.cn/). It is a temp version, not public.  The formal version will be released soon.