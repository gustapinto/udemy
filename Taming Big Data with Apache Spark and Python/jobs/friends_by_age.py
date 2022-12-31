'''
Analisa os dados de usuários a partir de um dataset de uma rede social
ficticia, montando uma listagem com a média de amigos por idade
'''
from os import getenv

from pyspark import SparkContext


def parse_line(line):
    fields = line.split(',')

    age = int(fields[2])
    number_of_friends = int(fields[3])

    return (age, number_of_friends)


def reduce_ages(entry, aggregator):
    number_of_friends = entry[0] + aggregator[0]
    ocurrences = entry[1] + aggregator[1]

    return (number_of_friends, ocurrences)


if __name__ == '__main__':
    sc = SparkContext(master=getenv('SPARK_MASTER_URL'),
                      appName='FriendsByAge')
    sc.setLogLevel('ERROR')

    file = sc.textFile('/app/datasets/fakefriends.csv')

    # Cria um RDD chave/valor mapeando um RDD usando um operador que
    # retorne dois valores
    rdd = file.map(parse_line)

    # <rdd>.mapValues() mapeia apenas os valores de um RDD chave/valor
    #
    # OBS: No caso desse exemplo esse mapeamento retorno um valor do tipo
    #      (idade, (número total de amigos, ocorrencias))
    mapped_rdd = rdd.mapValues(lambda x: (x, 1))

    # <rdd>.reduceByKey() combina valores para uma mesma chave, tendo
    # como operador uma função do tipo (valor, agregador -> ...)
    #
    # OBS: No caso abaixo temos um fluxo em que o valor é somado ao
    #      agregador a partir do par (número total de amigos, ocorrencias)
    totals_by_age = mapped_rdd.reduceByKey(reduce_ages)
    averages_by_age = totals_by_age.mapValues(lambda x: int(x[0] / x[1]))

    # <rdd>.sortBy() ordena os valores de um RDD a partir de um operador
    # do tipo (linha -> ...)
    sorted_averages_by_age = averages_by_age.sortBy(lambda x: x[0])

    # <rdd>.colelct() coleta o resultado de uma série de operações,
    # retornando uma lista
    results = sorted_averages_by_age.collect()

    print('\nIdade - Número médio de amigos')

    for result in results:
        print(f'{result[0]} - {result[1]}')
