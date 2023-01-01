from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType
)


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('PopularMovies') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    schema = StructType([StructField('user_id', IntegerType(), True),
                         StructField('movie_id', IntegerType(), True),
                         StructField('raing', IntegerType(), True),
                         StructField('timestamp', LongType(), True)])
    
    # <>.option('sep') define o separador usado para ler o arquivo
    # csv
    df = spark.read.option('sep', '\t') \
                   .schema(schema) \
                   .csv('/app/datasets/ml-100k/u.data')

    df.groupBy('movie_id') \
      .count() \
      .orderBy(f.desc('count')) \
      .show(10)

    spark.stop()
