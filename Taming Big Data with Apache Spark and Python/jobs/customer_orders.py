'''
Processa o dataset de ordens de vendas e encontra o total gasto por
cada consumidor
'''
from os import getenv

from pyspark import SparkContext


def parse_line(line):
    fields = line.split(',')

    return int(fields[0]), float(fields[2])


if __name__ == '__main__':
    sc = SparkContext(master=getenv('SPARK_MASTER_URL'),
                      appName='CustomerOrders')
    sc.setLogLevel('ERROR')

    file = sc.textFile('/app/datasets/customer-orders.csv')
    orders = file.map(parse_line)
    amount_spent = orders.reduceByKey(lambda x, y: x + y)
    sorted_amount_spent = amount_spent.sortBy(lambda x: x[1])
    results = sorted_amount_spent.collect()

    print('\nCliente - Total')

    for customer, total in results:
        print(f'{customer:02d} - R$ {total:.2f}')
