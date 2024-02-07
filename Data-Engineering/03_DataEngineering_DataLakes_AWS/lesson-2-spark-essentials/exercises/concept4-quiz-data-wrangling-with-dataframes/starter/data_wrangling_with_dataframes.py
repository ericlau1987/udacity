# # Data Wrangling with DataFrames Coding Quiz

from pyspark.sql import SparkSession

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()

path =  "../../data/sparkify_log_small.json"
sparkify_log_df = spark.read.json(path)
# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# TODO: write your code to answer question 1
empty_user_page = sparkify_log_df.select('page').filter(sparkify_log_df.userId=='').dropDuplicates()
all_user_page = sparkify_log_df.select('page').dropDuplicates()

for row in set(all_user_page.collect()) - set(empty_user_page.collect()):
    print(row)

# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 
sparkify_log_df.show()

# TODO: use this space to explore the behavior of the user with an empty string


# # Question 3
# 
# How many female users do we have in the data set?


# TODO: write your code to answer question 3


# # Question 4
# 
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# TODO: write your code to answer question 5

