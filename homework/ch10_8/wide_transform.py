from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, count
import time

spark = SparkSession \
        .builder \
        .appName('wide_transform') \
        .config('spark.sql.adaptive.enabled', 'False') \
        .config('spark.executor.memory', '2g') \
        .config('spark.executor.instances', '3') \
        .config('spark.executor.cores', '2') \
        .getOrCreate()

print(f'spark application start')

jobskill_path = 'hdfs:///home/spark/sample/linkedin_jobs/jobs/job_skills.csv'
skill_path = 'hdfs:///home/spark/sample/linkedin_jobs/mappings/skills.csv'


jobskill_schema = 'job_id             LONG, ' \
                  'skill_abr          STRING'

skill_schema = 'skill_abr          STRING,' \
               'skill_name         STRING'
               

# postings Load
jobskill_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(jobskill_schema) \
                 .csv(jobskill_path)

skill_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(skill_schema) \
                 .csv(skill_path)

result_df = jobskill_df \
    .join(broadcast(skill_df), on='skill_abr', how='inner') \
    .groupBy('skill_name') \
    .agg(count('job_id').alias('job_count')) \
    .orderBy(col('job_count').desc())

result_df.persist()
result_count = result_df.count()

print(f'result count: {result_count}')

time.sleep(1200)