'''
Consume um dataset usando o SparkSQL
'''
from os import getenv

from pyspark.sql import SparkSession, Row


def mapper(line):
    fields = line.split(',')

    # Row atua mapeando **kwargs para colunas, tal como uma tabela
    return Row(id=int(fields[0]),
               name=str(fields[1]).encode('utf-8'),
               age=int(fields[2]),
               number_of_friends=int(fields[3]))


if __name__ == '__main__':
    # Cria uma SparkSession ao invés de um SparkContext
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('SparkSQL') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # spark.sparkContext permite acessar a API de RDD por meio de uma
    # SparkSession
    file = spark.sparkContext.textFile('/app/datasets/fakefriends.csv')

    # Mapeia o RDD usando uma função que retorne uma Row
    people = file.map(mapper)

    # <sparkSession>.createDataframe(...) registra um RDD como DataFrame,
    # inferindo e retornando o schema da "tabela"
    schema = spark.createDataFrame(people).cache()

    # createOrReplaceTempView registra uma "tabela"
    schema.createOrReplaceTempView('people')

    # <sparkSession>.sql(...) executa uma query
    teenagers = spark.sql('''SELECT name, age
                             FROM people
                             WHERE age >= 13
                             AND age <= 19''')

    for teen in teenagers.collect():
        print(teen)

    # Usa a DataframeAPI para exebir os resultados
    schema.groupBy('age') \
          .count() \
          .orderBy('age') \
          .show()

    # spark.stop para o SparkContext
    spark.stop()
