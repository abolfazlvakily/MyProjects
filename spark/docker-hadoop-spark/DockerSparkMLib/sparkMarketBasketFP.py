from pyspark.sql import SparkSession
spark = SparkSession.builder.master("spark://spark-master:7077").getOrCreate()
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth
sparkdata = spark.read.format('csv').option('header', True).option('inferSchema', True).load('hdfs://namenode:9000/dddd.csv')
basketdata = sparkdata.dropDuplicates(['SalesTransactionID', 'SalesItem']).sort('SalesTransactionID')
basketdata = basketdata.groupBy("SalesTransactionID").agg(F.collect_list("SalesItem")).sort('SalesTransactionID')
fpGrowth = FPGrowth(itemsCol="collect_list(SalesItem)", minSupport=0.006, minConfidence=0.006)
model = fpGrowth.fit(basketdata)
model.freqItemsets.show()
items = model.freqItemsets
model.associationRules.show()
rules = model.associationRules
model.transform(basketdata).show()
transformed = model.transform(basketdata)
result_pdf = items.select("*").toPandas()
result_pdf.head()

#resource and data: https://github.com/DAR-DatenanalyseRehberg/DDDD_Data-Driven-Dealings-Development/blob/main/DDDD.xlsx ,https://jesko-rehberg.medium.com