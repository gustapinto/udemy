from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('WindowStreaming') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    logs = spark.readStream.text('/app/datasets/logs')

    content_size_exp = r'\s(\d+)$'
    status_exp = r'\s(\d{3})\s'
    general_exp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    time_exp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
    host_exp = r'(^\S+\.[\S+\.]+\S+)\s'

    logs_df = logs.select(f.regexp_extract('value', host_exp, 1).alias('host'),
                          f.regexp_extract('value', time_exp, 1).alias('timestamp'),
                          f.regexp_extract('value', general_exp, 1).alias('method'),
                          f.regexp_extract('value', general_exp, 2).alias('endpoint'),
                          f.regexp_extract('value', general_exp, 3).alias('protocol'),
                          f.regexp_extract('value', status_exp, 1).cast('integer').alias('status'),
                          f.regexp_extract('value', content_size_exp, 1).cast('integer').alias('content_size')) \
                  .withColumn('event_time', f.current_timestamp())

    '''
    Usa groupby para criar um select com windows

    - window() organiza as linhas da consulta em janelas de tempo
    '''
    endpoint_count_df = logs_df.groupBy(f.window(f.col('event_time'),
                                               windowDuration='30 seconds',
                                               slideDuration='10 seconds'),
                                        f.col('endpoint')) \
                               .count() \
                               .orderBy(f.col('count').desc())

    endpoint_count_df.writeStream.outputMode('complete') \
                                 .format('console') \
                                 .queryName('counts') \
                                 .start() \
                                 .awaitTermination()

    spark.stop()
