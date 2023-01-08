from os import getenv

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .master(getenv('SPARK_MASTER_URL')) \
                        .appName('RealState') \
                        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    df = spark.read.option('header', True) \
                   .option('inferSchema', True) \
                   .csv('/app/datasets/realestate.csv')
    assembler = VectorAssembler().setInputCols(['HouseAge',
                                                'DistanceToMRT',
                                                'NumberConvenienceStores']) \
                                 .setOutputCol('features')
    data_df = assembler.transform(df) \
                       .select('PriceOfUnitArea', 'features')
    train_df, test_df = data_df.randomSplit([0.5, 0.5])

    dtf = DecisionTreeRegressor().setFeaturesCol('features') \
                                 .setLabelCol('PriceOfUnitArea')
    all_predictions = dtf.fit(train_df) \
                         .transform(test_df) \
                         .cache()

    predictions = all_predictions.select('prediction').rdd.map(lambda x: x[0])
    labels = all_predictions.select('PriceOfUnitArea').rdd.map(lambda x: x[0])

    predictions_labels = predictions.zip(labels).collect()

    for prediction, label in predictions_labels:
        print(f'{label} - {prediction}')

    spark.stop()
