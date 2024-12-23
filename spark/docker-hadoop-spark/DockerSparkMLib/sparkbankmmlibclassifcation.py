from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LinearSVC
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()
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

# LR
lr = LogisticRegression(maxIter=100, regParam=0.3, elasticNetParam=0.1, featuresCol="scaledFeatures", family = "binomial", labelCol=labelCol)
pipeline_lr = Pipeline(stages=[vecAssembler, stdScaler, lr])
pipelineModel_lr = pipeline_lr.fit(trainDF)
predDF_lr = pipelineModel_lr.transform(testDF)
evaluator = MulticlassClassificationEvaluator(labelCol=labelCol, predictionCol="prediction", metricName="accuracy")
lr_accuracy = evaluator.evaluate(predDF_lr)
print("Accuracy of LogisticRegression is = %g"%(lr_accuracy))
print("Test Error of LogisticRegression = %g "%(1.0 - lr_accuracy))

# Linear SVM
lsvc = LinearSVC(maxIter=10, regParam=0.1, featuresCol="scaledFeatures", labelCol=labelCol)
pipeline_lsvc = Pipeline(stages=[vecAssembler, stdScaler, lsvc])
pipelineModel_lsvc = pipeline_lsvc.fit(trainDF)
predDF_lsvc = pipelineModel_lsvc.transform (testDF)
lr_accuracy = evaluator.evaluate(predDF_lsvc)
print("Accuracy of LogisticRegression is = %g"%(lr_accuracy))
print("Test Error of LogisticRegression = %g "%(1.0 - lr_accuracy))

# Random Forest
rf = RandomForestClassifier(labelCol=labelCol, featuresCol="scaledFeatures", numTrees=50)
pipeline_rf = Pipeline(stages=[vecAssembler, stdScaler, rf])
pipelineModel_rf = pipeline_rf.fit(trainDF)
predDF_rf = pipelineModel_rf.transform (testDF)
rf_accuracy = evaluator.evaluate(predDF_rf)
print("Accuracy of Random Tree is = %g"%(rf_accuracy))
print("Error of Random Tree is = %g "%(1.0 - rf_accuracy))

# Resource and data: http://archive.ics.uci.edu/ml/datasets/banknote+authentication, https://cprosenjit.medium.com/