from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time


spark = SparkSession \
    .builder \
    .appName('dataframe_cache') \
    .getOrCreate()

industry_path = "hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv"
employee_path = "hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv"

industry_df = spark.read.option("header", "true").csv(industry_path).persist()
employee_df = spark.read.option("header", "true").csv(employee_path).persist()

print("industry count:", industry_df.count())
print("employee count:", employee_df.count())

employee_dedup_df = employee_df.dropDuplicates(["company_id"])

result_df = (
    industry_df
    .filter(col("industry") == "ITServicesandITConsulting")
    .join(employee_dedup_df, "company_id")
    .filter(col("employee_count").cast("int") >= 1000)
    .orderBy(col("employee_count").cast("int").desc())
)

result_df.show()

# sleep 5 minute
time.sleep(300)