import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

print(
    spark.sparkContext.getConf().getAll()
    )

input_path = '../../data/sparkify_log_small.json'

user_log_df = spark.read.json(input_path)

# user_log_df.printSchema()
# print(
#     user_log_df.describe()
# )

# user_log_df.show(n=1)
print(
    user_log_df.take(5)
)

output_path = '../../data/sparkify_log_small.csv'
user_log_df.write.mode('overwrite').save(output_path, format='csv', header=True)


user_log2_df = spark.read.csv(output_path, header=True)
user_log2_df.printSchema()

user_log2_df.show(n=1)

print(
    user_log2_df.take(5)
)

user_log2_df.select("userID").show()

print(
user_log2_df.take(1)
)