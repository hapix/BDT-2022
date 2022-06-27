from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
import findspark
import pyspark
import pandas as pd


class Model_maker:
    def __init__(self):
        self.data = None

    def set_model(self, data):
        self.data = data
        findspark.init()
        spark_path = findspark.find()
        print("Spark is located at: " + spark_path)
        spark = SparkSession.builder.appName('big').getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        print("Transfer the data to Spark.")
        print("Transform the Data to Spark Dataframe.")
        data = spark.createDataFrame(data)
        string_feature_list = []
        for col in data.columns:
            if col == 'label':
                continue
            elif pyspark.sql.types.StringType == type(data.schema[col].dataType):
                string_feature_list.append(col)
        print("Data Schema: ")
        data.printSchema()
        print("String Indexer (Transform) has been made ...")
        sex_indexer = StringIndexer(inputCol="sex", outputCol="sex_Index")
        working_status_indexer = StringIndexer(inputCol="working_status", outputCol="working_status_Index")
        own_home_indexer = StringIndexer(inputCol="own_home", outputCol="own_home_Index")
        marital_status_indexer = StringIndexer(inputCol="marital_status", outputCol="marital_status_Index")
        print("One Hot Coding (Transform) has been made ...")
        sex_ohc = OneHotEncoder(inputCol="sex_Index", outputCol="sex_vec")
        working_status_ohc = OneHotEncoder(inputCol="working_status_Index", outputCol="working_status_vec")
        own_home_ohc = OneHotEncoder(inputCol="own_home_Index", outputCol="own_home_vec")
        marital_status_ohc = OneHotEncoder(inputCol="marital_status_Index", outputCol="marital_status_vec")
        early_stages = [sex_indexer, working_status_indexer, own_home_indexer, marital_status_indexer,
                        sex_ohc, working_status_ohc, own_home_ohc, marital_status_ohc]
        print("Add the stages to the pipeline")
        pipeline = Pipeline(stages=early_stages)
        pipeline_model = pipeline.fit(data)
        print("Executing the transformation throughout the pipeline")
        data = pipeline_model.transform(data)
        data = data.drop(*('sex_Index', 'working_status_Index', 'own_home_Index', 'marital_status_Index',
                           'sex', 'working_status', 'own_home', 'marital_status'))
        feature_list = []
        for col in data.columns:
            if col == 'label':
                continue
            else:
                feature_list.append(col)
        print("Vector Assembler is working ...")
        assembler = VectorAssembler(inputCols=feature_list, outputCol="features")
        data = assembler.transform(data)
        data = data.select(['features', 'label'])
        print("Samples of feature vectors")
        data.show(5)
        lr = LinearRegression(featuresCol='features', labelCol='label', maxIter=10, regParam=0.3, elasticNetParam=0.8)
        splits = data.randomSplit([0.7, 0.3])
        train_df = splits[0]
        test_df = splits[1]
        lr_model = lr.fit(train_df)
        print("Coefficients: " + str(lr_model.coefficients))
        print("Intercept: " + str(lr_model.intercept))
    def save_model(self):
        pass

    def load_model(self):
        pass

    def use_model(self):
        pass
