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
    
    most_popular_count = connections.sort(f.col('connections').desc()) \
                                    .show()
                                    # .first() \
    most_popular_name = names.filter(f.col('id') == most_popular_count[0]) \
                             .select('name') \
                             .first()

    print(f'{most_popular_name["name"]} - {most_popular_count["count"]}')

    spark.stop()
