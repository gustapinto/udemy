from os import getenv

from pyspark.sql import SparkSession, Row


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('FriendsSQL') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Realiza a leitura de um arquivo CSV inferindo cabeçalho e schema
    friends = spark.read.option('header', True) \
                        .option('inserSchema', True) \
                        .csv('/app/datasets/fakefriends-header.csv')

    # <dataframe>.printSchema() exibe o schema do dataframe
    friends.printSchema()

    # Usa operadores SQL Like (API parecida com Pandas) para exibir dados
    # do dataframe
    #
    # <dataframe>.select() retorna um dataframe apenas com os dados 
    # selecionados
    #
    # <dataframe>.show() exibe uma parcela dos resultados
    friends.select('name', 'age').show()

    # <dataframe>.select() tmabém permite mutar os valores retornados
    friends.select(friends.name, friends.age + 10).show()

    # <dataframe>.filter() filtra um dataframe com base no predicado
    # passado
    friends.filter(friends.age > 18).show()

    # <dataframe>.groupBy() agrupa os elementos de um dataframe por
    # coluna ou operador
    #
    # <dataframe>.count() conta os elementos dentro de um dataframe
    friends.groupBy('age').count().show()

    spark.stop()
