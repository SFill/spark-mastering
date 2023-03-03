# Spark mastering
Spark pet project for mastering spark

## Download data
Nice collection of [log data](https://github.com/logpai/loghub/)  

Hadoop dataset  
```sh
wget  https://zenodo.org/record/3227177/files/Hadoop.tar.gz?download=1 > data/Hadoop.tar.gz

```

## Enviroment
pyspark
```
docker build -t pyspark  .
docker run --rm -it --volume $(pwd):/app --network host pyspark 
```
ElasticSearch
```
docker-compose -f es/docker-compose.yaml up

```

Run py files, for instance main.py

```
docker run --rm -it --volume $(pwd):/app --network host pyspark main.py
```
