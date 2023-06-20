from pyspark.sql.functions import col, desc, sum, count
import datetime
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()


# read the newly arrived health grade data
health_df = spark.read.csv(
    "s3://sona-restaurant-project/input_values/health_grade/raw_data/health_grade.csv", header=True, inferSchema=True,)

health_history = "s3://sona-restaurant-project/input_values/health_grade/raw_data_history/health_grade-" + \
    str(datetime.datetime.now())+'.csv'
health_df.write.csv(health_history)

health_df = health_df.withColumn("grade_date", F.to_date(
    F.col("grade_date").cast("string"), 'yyyyMMdd'))

# Store daily data in parquet overwrite mode
health_df.write.partitionBy('grade_date').mode('overwrite').parquet(
    "s3://sona-restaurant-project/input_values/health_grade/input_parquet/")

healthDF = spark.read.parquet(
    "s3://sona-restaurant-project/input_values/health_grade/input_parquet/")


healthDF.createOrReplaceTempView("health")
health_df2 = spark.sql("select grade_date, restaurant_id, restaurant_name, cuisine, health_grade "
                       "from health ")
health_df2 = health_df2.withColumn(
    "restaurant_id", col("restaurant_id").cast(StringType()))

# adding dates between current health grade update update until the next
add_dates = health_df2. \
    groupBy('restaurant_id', 'restaurant_name', 'restaurant_name', 'cuisine'). \
    agg(F.min('grade_date').alias('min_dt'),
        F.max('grade_date').alias('max_dt')
        ). \
    withColumn('dt_arr', F.expr('sequence(min_dt, max_dt, interval 1 day)')). \
    withColumn('exploded_date', F.explode('dt_arr')). \
    select('restaurant_id', 'restaurant_name', 'cuisine',
           F.col('exploded_date').alias('grade_date'))

# filling the health grades for dates from current grade date until the next update
health_df_all_dates = add_dates. \
    join(health_df2, ['restaurant_id', 'grade_date', 'restaurant_name', 'cuisine'], 'left'). \
    withColumn("health_grade", F.last('health_grade', True).over(Window.partitionBy('restaurant_id').orderBy('grade_date').rowsBetween(-sys.maxsize, 0))).\
    select('restaurant_id', 'grade_date',
           'restaurant_name', 'cuisine', 'health_grade')

# writing to output file
health_df_all_dates.write.mode('append').format('parquet').save(
    "s3://sona-restaurant-project/output_values/health_grade/grade_values/")
