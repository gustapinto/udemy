from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('MostPopularSuperhero') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    schema = StructType([StructField('id', IntegerType(), True),
                         StructField('name', StringType(), True)])
    names = spark.read.schema(schema) \
                      .option('sep', ' ') \
                      .csv('/app/datasets/Marvel_Names.txt')
    graph = spark.read.text('/app/datasets/Marvel_Graph.txt')

    connections = graph.withColumn('id', f.split(f.col('value'), ' ')[0]) \
                       .withColumn('connections', f.size(f.split(f.col('value'), ' ')) -1) \
                       .groupBy('id') \
                       .agg(f.sum('connections').alias('connections'))
    min_connection_count = connections.agg(f.min('connections')).first()[0]

    # <dataframe>.join() realiza uma operação de join entre dois
    # dataframes com base em uma coluna em comum
    connections.filter(f.col('connections') == min_connection_count) \
               .join(names, 'id') \
               .show()

    spark.stop()
