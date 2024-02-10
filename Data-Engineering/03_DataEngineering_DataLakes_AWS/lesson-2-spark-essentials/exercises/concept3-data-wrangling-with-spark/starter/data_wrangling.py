# Take care of any imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum as Fsum

import pandas as pd
import matplotlib.pyplot as plt
import datetime

# Create the Spark Context
spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

print(
    spark.sparkContext.getConf().getAll()
    )
# Complete the script


path = "../../data/sparkify_log_small.json"
spark_log_df = spark.read.json(path)
spark_log_df.show(5)

# # Data Exploration 

# # Explore the data set.

# View 5 records 
spark_log_df.show(5)

# Print the schema
spark_log_df.printSchema()

# Describe the dataframe
print(spark_log_df.describe())

# Describe the statistics for the song length column
for col in spark_log_df.columns:
    spark_log_df.describe(col).show()

# Count the rows in the dataframe
print(spark_log_df.count())

# Select the page column, drop the duplicates, and sort by page
spark_log_df.select('page').dropDuplicates().sort('page').show()

# Select data for all pages where userId is 1046
spark_log_df.select(spark_log_df.columns).where(spark_log_df.userId==1046).show()

# # Calculate Statistics by Hour
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x/1000).hour)
spark_log_df = spark_log_df.withColumn('hour', get_hour(spark_log_df.ts))
spark_log_df.show(1)

# Select just the NextSong page
songs_in_hour_df = spark_log_df.filter(spark_log_df.page=='NextSong') \
    .groupby(spark_log_df.hour) \
    .count() \
    .orderBy(spark_log_df.hour.cast("float"))

songs_in_hour_df.show(2)

songs_in_hour_pd = songs_in_hour_df.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)
plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24)
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played")
plt.show()

# # Drop Rows with Missing Values
spark_log_df_notna = spark_log_df.dropna(how='any', subset=["userId", "sessionId"])

# How many are there now that we dropped rows with null userId or sessionId?
print(spark_log_df_notna.count())

# select all unique user ids into a dataframe
spark_log_df.select('userId').dropDuplicates().sort(spark_log_df.userId).show()

# Select only data for where the userId column isn't an empty string (different from null)
user_log_valid_df = spark_log_df.filter(spark_log_df.userId!='')
print(user_log_valid_df.count())

# # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then show those log entries. 
user_log_valid_df.filter("page = 'Submit Downgrade'").show()


# Create a user defined function to return a 1 if the record contains a downgrade
check_user_downgrade = udf(lambda x: 1 if x=='Submit Downgrade' else 0, IntegerType())
user_log_valid_df = user_log_valid_df.withColumn("downgraded", check_user_downgrade('page'))
print(user_log_valid_df.filter("page = 'Submit Downgrade'").count())
# Select data including the user defined function



# Partition by user id
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.
from pyspark.sql import Window

windowval = Window.partitionBy('userId').orderBy(desc('ts')).rangeBetween(Window.unboundedPreceding, 0)


# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have
# user_log_valid_df = user_log_valid_df.withColumn(
#     'phase', Fsum('downgraded').window(windowval)
# )
user_log_valid_df = user_log_valid_df \
    .withColumn("phase", Fsum("downgraded") \
    .over(windowval))

# Show the phases for user 1138 
user_log_valid_df.filter("'userId'==1138").show()
