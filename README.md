# Graph_DB_Benchmark

This project evaluates 4 graph databases - [Neo4j](https://neo4j.com/), [AgensGraph](https://bitnine.net/), [LightGraph](https://fma-ai.cn/) and [TigerGraph](https://www.tigergraph.com/), based on the LDBC_SNB benchmark. Scripts and experimental results are included so you can reproduce the benchmark results following our guide.

## Data Generation

You can use [ldbc_snb_datagen](https://github.com/ldbc/ldbc_snb_datagen) to generate LDBC raw data. The [`params.ini`](params.ini) is our configuration file, in which you can change the value of  **scaleFactor** to generate datasets with different scales. In this evaluation, we generate 3 datasets with scale factor 1, 10 and 100.

## Evaluation

### Scripts

The installation and evaluation method of each database is listed in the corresponding folder. Most scripts and query statements are obtained from their websites or staff.

### Parameters

 Seeds folder contains our experimental parameters. `Seeds_1`, `seeds_10`, `seeds_100` contains parameters used in micro queries(interactive update and short query)  for different scale datasets. Seeds_macro contains parameters used in macro queries(interactive complex and business intelligence query), which are same for different datasets.

## Results

We conducted all experiments on a single machine with *two 20-core processors Intel Xeon E5-2680 v2 2.80GHz*, *96GB main memory*, and *960G NVMe SSD*, runnig *Ubuntu 16.04.5* operating system. You can find our results in [`result-all.csv`](result-all.csv).

## Publication

Ran Wang, Zhengyi Yang, Wenjie Zhang, and Xuemin Lin. "[An Empirical Study on Recent Graph Database Systems](https://link.springer.com/chapter/10.1007/978-3-030-55130-8_29)". The 13th International Conference on Knowledge Science, Engineering and Management (KSEM 2020).

## Issues

Please send to wangransei@gmail.com for any further questions.
