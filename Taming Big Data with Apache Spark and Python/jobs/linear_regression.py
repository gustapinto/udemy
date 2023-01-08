from os import getenv

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegression


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('LinearRegression') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # <rdd>.toDF() cria um dataframe a partir de um RDD
    df = spark.sparkContext.textFile('/app/datasets/regression.txt') \
                           .map(lambda l: l.split(',')) \
                           .map(lambda l: (float(l[0]), Vectors.dense(float(l[1])))) \
                           .toDF(['label', 'features'])

    # <dataframe>.randomSplit() divide o dataframe em partes com dados
    # amostrados aleatoriamente, retornando uma lista de dataframes com
    # os dados separados
    training_df, test_df = df.randomSplit([0.5, 0.5])

    regression = LinearRegression(maxIter=20, regParam=0.3,
                                  elasticNetParam=0.8)
    model = regression.fit(training_df)

    # Gera as predições com base no modelo montado
    all_predictions = model.transform(test_df).cache()

    predictions = all_predictions.select('prediction').rdd.map(lambda x: x[0])
    labels = all_predictions.select('label').rdd.map(lambda x: x[0])
    prediction_label = predictions.zip(labels).collect()

    for prediction, label in prediction_label:
        print(f'{label} - {prediction}')

    spark.stop()
