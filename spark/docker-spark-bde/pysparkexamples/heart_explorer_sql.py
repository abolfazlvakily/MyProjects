from pyspark.sql import SparkSession
from pyspark.sql.types import *

schema = StructType().add(
    'age', DoubleType(), True
).add(
    'sex', DoubleType(), True
).add(
    'chest_pain_type', DoubleType(), True
).add(
    'resting_blood_pressure', DoubleType(), True
).add(
    'serum_cholesterol_in_mg_dl', DoubleType(), True
).add(
    'fasting_blood_sugar_120_mg_dl', DoubleType(), True
).add(
    'resting_electrocardiographic_results', DoubleType(), True
).add(
    'maximum_heart_rate_achieved', DoubleType(), True
).add(
    'exercise_induced_angina', DoubleType(), True
).add(
    'oldpeak', DoubleType(), True
).add(
    'the_slope', DoubleType(), True
).add(
    'number_of_major_vessels', DoubleType(), True
).add(
    'target', DoubleType(), True
)
spark = SparkSession.builder.getOrCreate()

df = spark.read.option('delimiter', ' ').schema(schema).csv('/dataset/Heart/heart.dat')
print(df.show())

print(df.select(df['age'], df['sex'], df['target']).show())

filteredData = df.select(df['age'], df['sex'], df['target']).filter(df['age'] > 40)
print(filteredData.show())

print(filteredData.count())

df.createOrReplaceTempView('heart_dataset')

result = spark.sql("SELECT age, count(*) from heart_dataset GROUP BY age")
print(result.show())

result = spark.sql("SELECT age, count(*) cnt from heart_dataset WHERE target = '7' GROUP BY age ORDER BY cnt DESC")
print(result.show())

df_pandas = result.toPandas()
df_pandas.to_csv('/sample.csv')