from pyspark.sql import SparkSession
import time


spark = SparkSession \
    .builder \
    .appName('simple_pyspark') \
    .getOrCreate()

schema = 'ID INT, COUNTRY STRING, HIT LONG'
df = spark.createDataFrame(data=[(1,'Korea', 120),(2,'USA', 80), (3,'Japna', 40)], schema=schema)
df.show()
df.count()

# sleep 5 minute
time.sleep(6000)