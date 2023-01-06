from os import getenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)

START_CHARACTER_ID = 5306  # Homem Aranha
TARGET_CHARACTER_ID = 14  # Adam 3031

counter = None


def convert_to_bfs(line):
    fields = line.split()
    character_id = int(fields[0])
    connections = [int(c) for c in fields[1:]]
    color = 'white'
    distance = 9999

    if character_id == START_CHARACTER_ID:
        color = 'gray'
        distance = 0

    return (character_id, (connections, distance, color))


def map_bfs(node):
    character_id, data = node
    connections, distance, color = data
    results = []

    if color == 'gray':
        for connection in connections:
            new_character_id = connection
            new_distance = distance + 1
            new_color = 'gray'

            if connection == TARGET_CHARACTER_ID:
                # <count>.add() incrementa o contador
                counter.add(1)

            results.append((new_character_id, ([], new_distance, new_color)))

        color = 'black'

    results.append((character_id, (connections, distance, color)))

    return results


def reduce_bfs(data_1, data_2):
    edges_1, distance_1, color_1 = data_1
    edges_2, distance_2, color_2 = data_2

    distance = 9999
    color = color_1
    edges = []

    edges.extend(edges_1)
    edges.extend(edges_2)

    if distance_1 < distance:
        distance = distance_1

    if distance_2 < distance:
        distance = distance_2

    if color_1 == 'white' and color_2 in ['gray', 'black']:
        color = color_2

    if color_1 == 'white' and color_2 == 'black':
        color = color_2

    if color_2 == 'white' and color_1 in ['gray', 'black']:
        color = color_1

    if color_2 == 'gray' and color_1 == 'black':
        color = color_1

    return (edges, distance, color)


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('MostPopularSuperhero') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # <spark context>.accumulator() inicia um acumulador que será
    # replicado para outros membros do cluster
    counter = spark.sparkContext.accumulator(0)

    rdd = spark.sparkContext.textFile('/app/datasets/Marvel_Graph.txt') \
                            .map(convert_to_bfs)

    for i in range(0, 10):
        mapped_rdd = rdd.flatMap(map_bfs)

        print(f'Processing {mapped_rdd.count()} values')

        if counter.value > 0:
            print(f'Character found after {counter.value} directions')
            break

        rdd = mapped_rdd.reduceByKey(reduce_bfs)

    spark.stop()
