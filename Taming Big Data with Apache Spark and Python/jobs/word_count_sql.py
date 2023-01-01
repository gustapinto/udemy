from os import getenv

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, split, lower, col


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('WordCountSQL') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    file = spark.read.text('/app/datasets/Book.txt')

    '''
    - explode() retorna uma nova linha para cada elemento na coleção
      passada
    - split() quebra palavras a partir de uma expressão regex
    - <dataframe>.value acessa os valores internos do dataframe
    '''
    words = file.select(explode(split(file.value, '\\W+')).alias('word'))

    '''
    - lower() converte uma string para lowercase
    - col() permite acessar as linhas a partir do nome de uma coluna
    - <dataframe>.sort() ordena os resultados da mesma forma que
      <dataframe>.orderBy
    '''
    normalized_words = words.select(lower(col('word')).alias('word')) \
                            .filter(col('word') != '')
    word_count = normalized_words.groupBy('word') \
                                 .count() \
                                 .sort('count')
    
    # Usa um valor dentro de count para exibir mais resultados além
    # do limite de 20
    word_count.show(word_count.count())

    spark.stop()
