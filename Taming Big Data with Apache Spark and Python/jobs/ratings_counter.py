'''
Analisa os dados de pontuações de um dataset de filmes, retornando
uma contagem do tipo (pontuação, total de avaliações)
'''
from collections import OrderedDict
from os import getenv

from pyspark import SparkContext


if __name__ == '__main__':
    # Conecta a uma instancia Spark master
    sc = SparkContext(master=getenv('SPARK_MASTER_URL'),
                      appName='RatingsHistogram')

    # Altera o nível de logs para ter uma output mais limpa no console
    sc.setLogLevel('ERROR')

    # Lê um arquivo de texto
    #
    # OBS: Esse arquivo precisa estar disponível para todos os workers
    #      do cluster, por isso é comum que usemos soluções de arquivos
    #      externos quando estamos processando dados com Spark
    file = sc.textFile('/app/datasets/ml-100k/u.data')

    # <rdd>.map() Mapeia o conteúdo do arquivo linha a linha, criando um
    # novo RDD com o resultado da operação, tomando como operador uma
    # função do tipo (linha -> ...)
    #
    # OBS: Spark não altera os dados inplace, ao invés disso ele sempre
    #      realiza a criação de um novo objeto ou RDD
    ratings = file.map(lambda line: line.split()[2])

    # Agrupa e conta os valores da coluna, retornando um defaultdict, com
    # <rdd>.countByValue() sendo uma action, ou seja, um método que gera
    # um resultado concreto, e não outro RDD
    result = ratings.countByValue()

    # Monta um dicionário ordenado para exibir os valores
    sorted_result = OrderedDict(sorted(result.items()))

    print('\nAvaliações:')

    for key, value in sorted_result.items():
        print(f'{key} - {value}')
