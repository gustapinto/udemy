from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round, col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
)


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('TotalSpentByCustomerSQL') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    schema = StructType([StructField('customer_id', IntegerType(), True),
                         StructField('product_id', IntegerType(), True),
                         StructField('value', FloatType(), True)])

    df = spark.read.schema(schema).csv('/app/datasets/customer-orders.csv')
    amount_spent_df = df.select('customer_id', 'value') \
                        .groupBy('customer_id') \
                        .agg(round(sum(col('value')), 2).alias('amount_spent')) \
                        .sort('amount_spent')

    amount_spent_df.show(amount_spent_df.count())

    spark.stop()
