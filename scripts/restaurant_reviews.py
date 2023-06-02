import datetime
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, desc, sum, count
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

data_source = "yelp_reviews"  # @param ["google_reviews", "yelp_reviews"]

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# spark = SparkSession.builder\
#         .master("local")\
#         .appName("Colab")\
#         .config('spark.ui.port', '4050')\
#         .config("dfs.client.read.shortcircuit.skip.checksum", "true")\
#         .getOrCreate()
# spark


input_parquet_path = f"s3://sona-restaurant-project/input_values/{data_source}/input_parquet/"
# Displaying the current data in the input parquet folder
current_data_df = spark.read.parquet(input_parquet_path)
current_data_df.show()
current_data_df.count()

# Reading raw data
input_csv_path = f"s3://sona-restaurant-project/input_values/{data_source}/raw_data/reviews.csv"
review_df = spark.read.csv(input_csv_path, header=True, inferSchema=True)

# Creating a raw data history with timestamp
review_history = f's3://sona-restaurant-project/input_values/{data_source}/raw_data_history/reviews-' + str(
    datetime.datetime.now())+'.csv'
review_df.write.csv(review_history, header=True)

review_df = review_df.withColumn("review_date", F.to_date(
    F.col("review_date").cast("string"), 'yyyyMMdd'))

# Create dates_list
dates_list = review_df.select(
    'review_date').distinct().rdd.map(lambda x: x[0]).collect()

# Read data only for the dates in the dates_list
review_existing = spark.read.parquet(input_parquet_path).filter(
    col("review_date").isin(dates_list))

# Rearranging columns of review_df (raw_data) to match with the review_existing (input_parquet)
review_df = review_df.select('review_id',  'restaurant_id', 'restaurant_name',
                             'county', 'rating', 'reviewer_name', 'comments', 'operation', 'review_date')

# Merging raw data with the data from input parquet for the dates in the dates_list
review_df_merge = review_existing.union(review_df)
review_df_merge = review_df_merge.distinct()

# Writing merged dataframe to the input parquet file in dynamic overwrite mode
review_df_merge.write.partitionBy('review_date').mode('overwrite').format('parquet').option(
    "partitionOverwriteMode", "dynamic").save(input_parquet_path)

# Rating_sum Data calculation

review_df_merge.createOrReplaceTempView("reviews")
sum_rating_df = spark.sql("select review_date, restaurant_id, restaurant_name, county, sum(rating) rating_sum, count(rating) rating_count "
                          "from reviews group by restaurant_id, restaurant_name, county, review_date order by review_date")
sum_rating_df = sum_rating_df.withColumn(
    "restaurant_id", col("restaurant_id").cast(StringType()))

spark.sql('refresh table reviews')

# Adding dates for the missing dates in review_df2 and filling their values with 0
add_dates = sum_rating_df. \
    groupBy('restaurant_id', 'restaurant_name', 'restaurant_name', 'county'). \
    agg(F.min('review_date').alias('min_dt'),
        F.max('review_date').alias('max_dt')
        ). \
    withColumn('dt_arr', F.expr('sequence(min_dt, max_dt, interval 1 day)')). \
    withColumn('exploded_date', F.explode('dt_arr')). \
    select('restaurant_id', 'restaurant_name', 'county',
           F.col('exploded_date').alias('review_date'))

sum_rating_df_all_dates = add_dates. \
    join(sum_rating_df, ['restaurant_id', 'review_date', 'restaurant_name', 'county'], 'left'). \
    fillna(0, subset=['rating_sum', 'rating_count'])

# Writing Output file of rating_sum
rating_sum_path = f"s3://sona-restaurant-project/output-staging/{data_source}/rating_sum/"

sum_rating_df_all_dates.write.partitionBy('review_date').mode('overwrite').format('parquet').option(
    "partitionOverwriteMode", "dynamic").save(rating_sum_path)

# Getting dates 6 days before and after dates in our date_list
required_dates = []
for date in dates_list:
    for x in range(0, 6):
        required_dates.append(date + datetime.timedelta(days=x))
        required_dates.append(date - datetime.timedelta(days=x))

required_dates = list(set(required_dates))

# Reading rating_sum parquet for the dates in required_dates
final_rating_df = spark.read.parquet(rating_sum_path).filter(
    col("review_date").isin(required_dates))

# Creating rolling average window function and calculating rolling average
w = Window().partitionBy(['restaurant_id']).orderBy(
    'review_date').rowsBetween(-6, 0)
final_rating_df = final_rating_df.withColumn('rating_sum_rolling', F.sum("rating_sum").over(w)) \
    .withColumn('rating_count_rolling', F.sum("rating_count").over(w))
final_rating_df = final_rating_df.withColumn(
    'final_rating', final_rating_df['rating_sum_rolling']/final_rating_df['rating_count_rolling'])

final_rating_dates = []
for date in dates_list:
    for x in range(0, 6):
        final_rating_dates.append(date + datetime.timedelta(days=x))

final_rating_dates = list(set(final_rating_dates))

final_rating_df = final_rating_df.filter(
    col("review_date").isin(final_rating_dates))

# Calling existing final_rating data
final_rating_path = f"s3://sona-restaurant-project/output_values/{data_source}/final_rating/"
existing_final_rating_df = spark.read.parquet(final_rating_path)

final_rating_df = final_rating_df.select('restaurant_name', 'county', 'rating_sum', 'rating_count', 'review_date', 'final_rating', 'restaurant_id')

# Selecting the data from existing final_rating data for dates not in the final_rating_dates
existing_final_rating_DF = existing_final_rating_df.filter(~existing_final_rating_df.review_date.isin(final_rating_dates))

final_rating_df = existing_final_rating_DF.union(final_rating_df)

# Writing final final_rating data
final_rating_path = f"s3://sona-restaurant-project/output_values/{data_source}/final_rating/"

final_rating_df.write.partitionBy('restaurant_id').mode('overwrite').format('parquet').option(
    "partitionOverwriteMode", "dynamic").save(final_rating_path)
