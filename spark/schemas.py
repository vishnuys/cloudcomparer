from pyspark.sql.functions import *
from pyspark.sql.types import *

users_schema = StructType([
    StructField("userid", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("zipcode", IntegerType(), True)])

zipcodes_schema = StructType([
    StructField("zipcode", IntegerType(), True),
    StructField("zipcodetype", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)])

rating_schema = StructType([
    StructField("userid", IntegerType(), True),
    StructField("movieid", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", IntegerType(), True)])

movies_schema = StructType([
    StructField("movieid", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("releasedate", StringType(), True),
    StructField("unknown", IntegerType(), True),
    StructField("Action", IntegerType(), True),
    StructField("Adventure", IntegerType(), True),
    StructField("Animation", IntegerType(), True),
    StructField("Children", IntegerType(), True),
    StructField("Comedy", IntegerType(), True),
    StructField("Crime", IntegerType(), True),
    StructField("Documentary", IntegerType(), True),
    StructField("Drama", IntegerType(), True),
    StructField("Fantasy", IntegerType(), True),
    StructField("Film_Noir", IntegerType(), True),
    StructField("Horror", IntegerType(), True),
    StructField("Musical", IntegerType(), True),
    StructField("Mystery", IntegerType(), True),
    StructField("Romance", IntegerType(), True),
    StructField("Sci_Fi", IntegerType(), True),
    StructField("Thriller", IntegerType(), True),
    StructField("War", IntegerType(), True),
    StructField("Western", IntegerType(), True)])
