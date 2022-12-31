'''
'''
from os import getenv

from pyspark import SparkContext


if __name__ == '__main__':
    sc = SparkContext(master=getenv('SPARK_MASTER_URL'),
                      appName='')
    sc.setLogLevel('ERROR')
