from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType
)


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('MinTemperaturesSQL') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Monsta um schema manualmente, para os casos em que os datasets
    # não possuem cabeçalhos
    schema = StructType([StructField('station_id', StringType(), True),
                         StructField('date', IntegerType(), True),
                         StructField('measure_type', StringType(), True),
                         StructField('temperature', FloatType(), True)])
    df = spark.read.schema(schema).csv('/app/datasets/1800.csv')

    df.filter(df.measure_type == 'TMIN') \
      .select('station_id', (col('temperature') * 0.1).alias('temperature')) \
      .groupBy('station_id') \
      .min('temperature') \
      .show()

    spark.stop()
