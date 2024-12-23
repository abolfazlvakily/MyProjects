from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LinearSVC
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.master('spark://spark-master:7077').getOrCreate()
df = spark.read.format('csv').option('header', True).option('inferSchema', True).load('hdfs://namenode:9000/data_banknote_authentication.txt')
labelCol = "Class"
trainDF, testDF = df.randomSplit([.8, .2], seed=42)
print(f"""There are {trainDF.count()} rows in the training set, and {testDF.count()} in the test set""")
inputCols = trainDF.columns[0: len (trainDF.columns) - 1]
vecAssembler = VectorAssembler(inputCols = inputCols, outputCol = "features").setHandleInvalid("skip")
vecTrainDF = vecAssembler.transform(trainDF)
vecTrainDF.select ("features").show(5, False)
stdScaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
scalerModel = stdScaler.fit(vecTrainDF)
scaledDataDF = scalerModel.transform(vecTrainDF)
scaledDataDF.select ("scaledFeatures").show(5, False)
evaluator = MulticlassClassificationEvaluator(labelCol=labelCol, predictionCol="prediction", metricName="accuracy")

# MLP
layers = [4, 5, 4, 2]
mlp = MultilayerPerceptronClassifier(labelCol=labelCol, featuresCol="scaledFeatures", maxIter=100, layers=layers, blockSize=128, seed=1234)
pipeline_mlp = Pipeline(stages=[vecAssembler, stdScaler, mlp])
pipelineModel_mlp = pipeline_mlp.fit(trainDF)
predDF_mlp = pipelineModel_mlp.transform (testDF)
mlp_accuracy = evaluator.evaluate(predDF_mlp)
print("Accuracy of MLP Classifier is = %g"%(mlp_accuracy))
print("Error of MLP Classifier is = %g "%(1.0 - mlp_accuracy))

# Resource and data: http://archive.ics.uci.edu/ml/datasets/banknote+authentication, https://cprosenjit.medium.com/