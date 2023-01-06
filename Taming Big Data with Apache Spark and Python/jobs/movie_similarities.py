from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
)

SCORE_THRESHOLD = 0.97  # 97%
CO_OCCURRRENCE_THRESHOLD = 50.0  # 50%
MOVIE_ID = 50  # Star Wars


def compute_cosine_similarity(data):
    '''
    Função que calcula a similaridade dos filmes passados
    '''
    pair_scores = data.withColumn('xx', f.col('rating_1') * f.col('rating_1')) \
                      .withColumn('yy', f.col('rating_2') * f.col('rating_2')) \
                      .withColumn('xy', f.col('rating_1') * f.col('rating_2'))
    calculate_similarity = pair_scores.groupBy('movie_1', 'movie_2') \
                                      .agg(f.sum(f.col('xy')).alias('numerator'), \
                                           (f.sqrt(f.sum(f.col('xx'))) * f.sqrt(f.sum(f.col('yy')))).alias('denominator'), \
                                           f.count(f.col('xy')).alias('num_pairs'))
    result = calculate_similarity.withColumn('score', \
                                             f.when(f.col('denominator') != 0, \
                                                    f.col('numerator') / f.col('denominator')) \
                                              .otherwise(0)) \
                                  .select('movie_1', 'movie_2', 'score', 'num_pairs')

    return result


def get_movie_name(data, movie_id):
    return data.filter(f.col('movie_id') == movie_id) \
               .select('movie_title') \
               .collect()[0][0]


def get_movie_names_dataframe():
    schema = StructType([StructField('movie_id', IntegerType(), True),
                         StructField('movie_title', StringType(), True)])
    df = spark.read.option('sep', '|') \
                   .option('charset', 'iso-8859-1') \
                   .schema(schema) \
                   .csv('/app/datasets/ml-100k/u.item')

    return df


def get_movies_dataframe():
    schema = StructType([StructField('user_id', IntegerType(), True),
                         StructField('movie_id', IntegerType(), True),
                         StructField('rating', IntegerType(), True),
                         StructField('timestamp', LongType(), True)])
    df = spark.read.option('sep', '\t') \
                   .schema(schema) \
                   .csv('/app/datasets/ml-100k/u.data')

    return df


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('MovieSimilarities') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    movie_names_df = get_movie_names_dataframe()
    movies_df = get_movies_dataframe()

    ratings = movies_df.select('user_id', 'movie_id', 'rating')

    # Usa self-join (join de um dataframe com ele mesmo) para explorar
    # todas as combinações
    movie_pairs = ratings.alias('ratings_1') \
                         .join(ratings.alias('ratings_2'),
                               (f.col('ratings_1.user_id') == f.col('ratings_2.user_id')) & \
                               (f.col('ratings_1.movie_id') < f.col('ratings_2.movie_id'))) \
                         .select(f.col('ratings_1.movie_id').alias('movie_1'),
                                 f.col('ratings_2.movie_id').alias('movie_2'),
                                 f.col('ratings_1.rating').alias('rating_1'),
                                 f.col('ratings_2.rating').alias('rating_2'))
    movie_pairs_similarities = compute_cosine_similarity(movie_pairs).cache()

    results = movie_pairs_similarities.filter(((f.col('movie_1') == MOVIE_ID) | (f.col('movie_2') == MOVIE_ID)) & \
                                              ((f.col('score') > SCORE_THRESHOLD) & (f.col('num_pairs') > CO_OCCURRRENCE_THRESHOLD))) \
                                      .sort(f.col('score').desc()) \
                                      .take(10)

    print('Movie - Score - Srength')

    for result in results:
        similar_movie_id = result.movie_1

        if similar_movie_id == MOVIE_ID:
            similar_movie_id = result.movie_2

        movie_name = get_movie_name(movie_names_df, similar_movie_id)

        print(f'{movie_name} - {result.score} - {result.num_pairs}')


