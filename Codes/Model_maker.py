from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
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
        sc.setLogLevel("FATAL")
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
        string_indexer_dic = {}

        # Make the dictionary for saving the transform structure (Using StringIndexer)
        for str_column in string_feature_list:
            string_indexer_dic["%s_index" % str_column] = StringIndexer(inputCol=str_column,
                                                                        outputCol=str_column + "_index")
        # Convert string indexer dic to list cause the pipeline accept the list.
        string_indexer_list = [string_indexer_dic[indexer_item] for indexer_item in string_indexer_dic]

        # Make the dictionary for saving the transform structure (Using OneHotCoding)
        string_ohc_dic = {}
        for str_column_next in string_feature_list:
            string_ohc_dic["%s_ohc" % str_column_next] = OneHotEncoder(inputCol=str_column_next + "_index",
                                                                       outputCol=str_column_next + "_ohc")
        # Convert "one hot coding" dic to list cause the pipeline accept the list.
        indexed_ohc_list = [string_ohc_dic[indxer_item] for indxer_item in string_ohc_dic]

        # Add all the stages to the pipeline
        print("Add the stages to the pipeline")
        pipeline = Pipeline(stages=string_indexer_list + indexed_ohc_list)
        pipeline_model = pipeline.fit(data)
        print("Executing the transformation throughout the pipeline")
        data = pipeline_model.transform(data)

        # Remove the useless (primary columns. For example, we do not need "sex" colum
        # anymore because we change it through the StringIndexer and OneHotCoding stages
        # in previous steps).
        string_indexer_tuple = tuple(list(string_indexer_dic) + string_feature_list)
        data = data.drop(*string_indexer_tuple)

        # print("String Indexer (Transform) has been made ...")
        # sex_indexer = StringIndexer(inputCol="sex", outputCol="sex_Index")
        # working_status_indexer = StringIndexer(inputCol="working_status", outputCol="working_status_Index")
        # own_home_indexer = StringIndexer(inputCol="own_home", outputCol="own_home_Index")
        # marital_status_indexer = StringIndexer(inputCol="marital_status", outputCol="marital_status_Index")
        # print("One Hot Coding (Transform) has been made ...")
        # sex_ohc = OneHotEncoder(inputCol="sex_Index", outputCol="sex_vec")
        # working_status_ohc = OneHotEncoder(inputCol="working_status_Index", outputCol="working_status_vec")
        # own_home_ohc = OneHotEncoder(inputCol="own_home_Index", outputCol="own_home_vec")
        # marital_status_ohc = OneHotEncoder(inputCol="marital_status_Index", outputCol="marital_status_vec")
        # early_stages = [sex_indexer, working_status_indexer, own_home_indexer, marital_status_indexer,
        #                 sex_ohc, working_status_ohc, own_home_ohc, marital_status_ohc]

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
        test_result = lr_model.evaluate(test_df)
        training_summary = lr_model.summary
        print("========================================================")
        print('Model has been made')
        print("Root Mean Squared Error (RMSE)"
              " on test data = %g" % training_summary.rootMeanSquaredError)
        print("Root Mean Squared Error (RMSE)"
              " on test data = %g" % test_result.rootMeanSquaredError)
        print("========================================================")
        sc.stop()
        # return

    def save_model(self):
        pass

    def load_model(self):
        pass

    def use_model(self):
        pass
