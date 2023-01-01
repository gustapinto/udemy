from os import getenv

from pyspark.sql import SparkSession, Row


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('SparkSQL') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    ...

    spark.stop()
