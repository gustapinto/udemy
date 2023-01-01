from os import getenv

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import avg, round


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('FriendsByAgeSQL') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Realiza a leitura de um arquivo CSV inferindo cabeçalho e schema
    people = spark.read.option('header', True) \
                        .option('inserSchema', True) \
                        .csv('/app/datasets/fakefriends-header.csv')

    '''
    Filtrando o dataframe usando sintaxe de métodos, sem funções

    - <col>.cast(type) altera o tipo de uma coluna para o tipo passado
    - <col>.alias() define um alias para uma coluna
    - <dataframe>.avg() calcula o valor médio de uma coluna
    - <dataframe>.orderBy() ordena o dataframe pela coluna
    - <dataframe>.withColumnRenamed() renomeia uma coluna
    '''
    people.select('age', people.friends.cast('int').alias('friends')) \
          .groupBy('age') \
          .avg('friends') \
          .orderBy('age') \
          .withColumnRenamed("avg(friends)", 'avg_friends') \
          .show()

    '''
    Filtrando usando sintaxe de métodos e funções do PySpark SQL

    - <datframe>.agg() aplica uma operação de agregação, permitindo usar
      funções nas queries
    - round() função de arredondamento de valores
    - avg() função de média por coluna
    '''
    people.select('age', 'friends') \
          .groupBy('age') \
          .agg(round(avg('friends'), 2)) \
          .orderBy('age') \
          .withColumnRenamed("round(avg(friends), 2)", 'avg_friends') \
          .show()

    '''
    Filtrando usando SQL
    '''
    people.createOrReplaceTempView('people')

    spark.sql('''
            SELECT age,
                ROUND(AVG(friends), 2) AS avg_friends
            FROM people
            GROUP BY age
            ORDER BY age;
        ''') \
        .show()

    spark.stop()
