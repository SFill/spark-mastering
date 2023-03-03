from pyspark.sql import SparkSession

from pyspark import SparkConf

from pyspark.sql import SparkSession, functions as F

import os


spark_config = {
    'spark.jars.packages': [
        # 'groupId:artifactId:version',
        # https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-hadoop
        'org.elasticsearch:elasticsearch-hadoop:8.6.2'

    ]
}

ELASTIC_DEFAULT_CONFIG = {
    "es.nodes.ports": "9200",
    "es.nodes": "http://localhost",
    "es.nodes.wan.only": "true",
    "es.nodes.discovery": "false"
}


def create_spark_config(config_dict: dict):
    config_dict = {**config_dict}
    config_dict['spark.jars.packages'] = ','.join(
        config_dict['spark.jars.packages'])
    return SparkConf(False).setAll(config_dict.items())


spark = SparkSession.builder.appName("SimpleApp").config(
    conf=create_spark_config(spark_config)
).getOrCreate()

sc = spark._sc


sc.setLogLevel("INFO")


logFile = f"{os.path.abspath('.')}/data/**/*.log"
# logFile = f"{os.path.abspath('.')}/data/application_1445087491445_0001/*.log"

logData = spark.read.text(logFile).toDF("text").withColumn(
    'filepath', F.input_file_name()).cache()


# 2015-10-17 15:37:57,720 INFO [main] org.apache.hadoop.mapreduce.v2.app.MRAppMaster: OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
pattern = r"(\S+\s\S+)\s(INFO|DEBUG|WARNING|ERROR)\s\[(.+)\]\s(\S+)\:\s(.+)"


logData = logData.withColumn('filepath', F.split(F.col("filepath"), '/'))

logData = logData.withColumn(
    'file_folder', logData['filepath'].getItem(F.size('filepath')-2))


parsed_log = logData.select(
    F.to_timestamp(
        F.regexp_extract(
            logData['text'], pattern, 1
        ), "yyyy-MM-dd HH:mm:ss,SSS"
    ).alias("created_at"),
    F.regexp_extract(logData['text'], pattern, 2).alias("level"),
    F.regexp_extract(logData['text'], pattern, 3).alias("component"),
    F.regexp_extract(logData['text'], pattern, 4).alias("cls"),
    F.regexp_extract(logData['text'], pattern, 5).alias("msg"),
    F.col('file_folder')
)

parsed_log = parsed_log.filter(parsed_log["msg"] != "")

parsed_log.write.format('org.elasticsearch.spark.sql').options(
    **ELASTIC_DEFAULT_CONFIG
).mode("append").save(
    'parsed-log/data'
)

# rdd API
# parsed_log.rdd.map(lambda x: (None, x.asDict())).saveAsNewAPIHadoopFile(
#     path='-',
#     outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
#     keyClass="org.apache.hadoop.io.NullWritable",
#     valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
#     conf=ELASTIC_DEFAULT_CONFIG
# )


word_count_machine_down_files = spark.read.text(
    'data/word_count_machine_down.txt').toDF('file_folder')

word_count_machine_down_log = parsed_log.join(
    word_count_machine_down_files, 'file_folder', 'right').drop('file_folder')

word_count_machine_down_log.write.format('org.elasticsearch.spark.sql').options(
    **ELASTIC_DEFAULT_CONFIG
).mode("append").save(
    'parsed-log-word-count-machine-down/data'
)
