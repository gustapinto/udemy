'''
Conta a ocorrencia de palavras em um dataset usando flatmap
'''
import re
from os import getenv

from pyspark import SparkContext


def normalize_words(line):
    return re.compile(r'\W+', re.UNICODE).split(line.lower())


if __name__ == '__main__':
    sc = SparkContext(master=getenv('SPARK_MASTER_URL'),
                      appName='WordCount')
    sc.setLogLevel('ERROR')

    file = sc.textFile('/app/datasets/Book.txt')

    # <rdd>.flatMap() mapeia o RDD com um operador que pode retornar
    # um ou mais valores para cada operação, sendo assim uma versão
    # 1:N para o <rdd>.map() (que atua 1:1)
    words = file.flatMap(normalize_words)

    # Não usa <rdd>.countByValue() para poder ordenar os resultados
    # diretamente pelo RDD
    word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    sorted_word_counts = word_counts.sortBy(lambda x: x[1])
    result = sorted_word_counts.collect()

    print('\nPalavra - Ocorrências')

    for word, count in result:
        cleaned_word = str(word.encode('ascii', 'ignore'))

        if cleaned_word:
            print(f'{cleaned_word} - {count}')
