import pymongo_spark
from pyspark.context import SparkContext
sc = SparkContext('local', 'test')
mongo_url = 'mongodb://mongo:27017/'
pymongo_spark.activate()
rdd = (sc.mongoRDD('{0}foo.bar'.format(mongo_url)).map(lambda doc: (doc.get('x'), doc.get('y'))))
rdd.collect()
