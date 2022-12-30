from collections import OrderedDict
from os import getenv

import pyspark


if __name__ == '__main__':
    # Conecta a uma instancia Spark master
    sc = pyspark.SparkContext(master=getenv('SPARK_MASTER_URL'),
                              appName='RatingsHistogram')

    # Altera o nível de logs para ter uma output mais limpa no console
    sc.setLogLevel('ERROR')

    # Lê um arquivo de texto
    #
    # OBS: Esse arquivo precisa estar disponível para todos os workers
    #      do cluster, por isso é comum que usemos soluções de arquivos
    #      externos quando estamos processando dados com Spark
    file = sc.textFile('/app/datasets/ml-100k/u.data')

    # Mapeia o arquivo, obtendo a terceira coluna (avaliações)
    ratings = file.map(lambda line: line.split()[2])

    # Agrupa e conta os valores da coluna, retornando um defaultdict
    result = ratings.countByValue()

    # Monta um dicionário ordenado para exibir os valores
    sorted_result = OrderedDict(sorted(result.items()))

    print('\nAvaliações:')

    for key, value in sorted_result.items():
        print(f'{key} - {value}')
