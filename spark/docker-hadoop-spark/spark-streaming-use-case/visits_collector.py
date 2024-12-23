import pyspark.sql.SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Column
from pyspark.sql.types import *
spark = SparkSession.builder().master("spark://spark-master:7077").appName("File Sink")
								 .getOrCreate()
				
schema = StructType([
  StructField("Date", StringType, True),
  StructField("Open", DoubleType, True),
  StructField("High", DoubleType, True),
  StructField("Low", DoubleType, True),
  StructField("Close", DoubleType, True),
  StructField("Adjusted Close", DoubleType, True),
  StructField("Volume", DoubleType, True)])
	

df = spark
  .readStream
  .option("maxFilesPerTrigger", 2) // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
  .option("header", true)
  .schema(schema)
  .csv("hdfs://namenode:9000/hospitalvisits")
  .withColumn("Name")
df.printSchema()
println("Streaming DataFrame : " + df.isStreaming)