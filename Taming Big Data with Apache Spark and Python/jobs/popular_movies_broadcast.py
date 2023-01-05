import codecs
from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType
)


def load_movie_names():
    movie_names = {}

    with codecs.open('/app/datasets/ml-100k/u.item', 'r',
                     encoding='iso-8859-1', errors='ignore') as file:
        for line in file:
            fields = line.split('|')
            movie_id = int(fields[0])
            movie_name = fields[1]
            movie_names[movie_id] = movie_name

    return movie_names


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('PopularMoviesBroadcast') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Cria um Broadcast e faz o upload dos dados para o cluster,
    # retornando um objeto broadcast e não o dicionário
    movie_name_dict = spark.sparkContext.broadcast(load_movie_names())

    schema = StructType([StructField('user_id', IntegerType(), True),
                         StructField('movie_id', IntegerType(), True),
                         StructField('raing', IntegerType(), True),
                         StructField('timestamp', LongType(), True)])

    df = spark.read.option('sep', '\t') \
                   .schema(schema) \
                   .csv('/app/datasets/ml-100k/u.data')
    movie_counts = df.groupBy('movie_id').count()

    # Define uma UDF (User Defiined Function) e a registra
    #
    # <broadcast>.value acessa o valor do broadcast
    lookup_movie_name_udf = f.udf(lambda movie_id: movie_name_dict.value[movie_id])

    # Usa a UDF para realizar um "join" entre o broadcast e o dataframe
    # em memória
    #
    # <dataframe>.withColumn() adiciona uma nova coluna ao dataframe
    movie_counts.withColumn('movie_title', lookup_movie_name_udf(f.col('movie_id'))) \
                .orderBy(f.desc('count')) \
                .show(10)

    spark.stop()
