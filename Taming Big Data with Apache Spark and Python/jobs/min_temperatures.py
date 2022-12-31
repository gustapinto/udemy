'''
Analisa a temperatura mínima a partir de um dataset de dados climáticos
de 1800
'''
from os import getenv

from pyspark import SparkContext


def parse_line(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    # Multiplica por 0.1 para normalizar os valores, visto que eles
    # estão sem virgulas no CSV
    temperature = float(fields[3]) * 0.1

    return (station_id, entry_type, temperature)


def get_temperature_by_station(rdd, operator):
    station_temperatures = rdd.map(lambda x: (x[0], x[2]))
    statio_temperature = station_temperatures.reduceByKey(lambda x, y: operator(x, y))
    results = statio_temperature.collect()

    return results


if __name__ == '__main__':
    sc = SparkContext(master=getenv('SPARK_MASTER_URL'),
                      appName='MinTemperatures')
    sc.setLogLevel('ERROR')

    file = sc.textFile('/app/datasets/1800.csv')
    rdd = file.map(parse_line)

    # <rdd>.filter() filtra um RDD, retornando apenas os valores que
    # passarem pelo predicado
    min_temps = rdd.filter(lambda x: x[1] == 'TMIN')
    max_temps = rdd.filter(lambda x: x[1] == 'TMAX')

    min_temps_results = get_temperature_by_station(min_temps, min)
    max_temps_results = get_temperature_by_station(max_temps, max)

    print('\nEstação - Temperatura mínima')

    for result in min_temps_results:
        print(f'{result[0]}: {result[1]:.2f} ºC')

    print('\nEstação - Temperatura máxima')

    for result in max_temps_results:
        print(f'{result[0]}: {result[1]:.2f} ºC')
