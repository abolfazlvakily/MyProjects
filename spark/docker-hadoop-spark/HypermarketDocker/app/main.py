from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()
df = spark.read.option('header', True).csv('hdfs://namenode:9000/hypermarketdata/Supermarket_CustomerMembers.csv')
result = df.filter(df['Genre'] == 'Female').filter(df['Spending Score (1-100)'] > 50.0).select('CustomerID')
result.write.csv('/tmp/result')