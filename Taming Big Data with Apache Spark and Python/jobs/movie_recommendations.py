import codecs
from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
)
from pyspark.ml.recommendation import ALS


USER_ID = 168


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
                        .appName('MovieRecommendationALS') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    movies_schema = StructType([StructField('user_id', IntegerType(), True),
                                StructField('movie_id', IntegerType(), True),
                                StructField('rating', IntegerType(), True),
                                StructField('timestamp', LongType(), True)])
    names = load_movie_names()
    movies = spark.read.option('sep', '\t') \
                       .schema(movies_schema) \
                       .csv('/app/datasets/ml-100k/u.data')

    '''
    Monta o modelo de processamento

    - <als>.setMaxIter() define a quantidade máxima de iterações do
      modelo
    - <als>.setRegParam() define a propriedade regParam
    - <als>.setUserCol() define a coluna de identificação do user
    - <als>.setItemCol() define a coluna de identifcação do item
    - <als>.setRatingCol() define a coluna de pontuações
    - <als>.fit() enciaxa o modelo
    '''
    model = ALS().setMaxIter(5) \
                 .setRegParam(0.01) \
                 .setUserCol('user_id') \
                 .setItemCol('movie_id') \
                 .setRatingCol('rating') \
                 .fit(movies)

    user_schema = StructType([StructField('user_id', IntegerType(), True)])
    users = spark.createDataFrame([[USER_ID,]], user_schema)

    # <transform>.recommendForUserSubset() gera um dataframe de
    # recomendações
    recommendations = model.recommendForUserSubset(users, 10) \
                           .collect()

    for recommendation in recommendations:
        my_recommendations = recommendation[1]

        for rec in my_recommendations:
            movie = rec[0]
            rating = rec[1]
            movie_name = names[movie]

            print(f'{movie_name} = {rating}')


    spark.stop()
