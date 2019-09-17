import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from .schemas import *


def execute_query1(t1, t1_alias, t2, t2_alias, c1, c2):
    sc = SparkContext('local', 'cloud')
    spark = SparkSession(sc)
    t1_alias_obt, attr1 = c1[0].split('.')
    t2_alias_obt, attr2 = c1[2].split('.')
    file_one = os.path.join('files', t1 + '.csv')
    file_two = os.path.join('files', t2 + '.csv')
    if t1 == 'users':
        df1 = spark.read.csv(file_one, header=False, schema=users_schema)
    elif t1 == 'zipcodes':
        df1 = spark.read.csv(file_one, header=False, schema=zipcodes_schema)
    elif t1 == 'movies':
        df1 = spark.read.csv(file_one, header=False, schema=movies_schema)
    elif t1 == 'rating':
        df1 = spark.read.csv(file_one, header=False, schema=rating_schema)

    if t2 == 'users':
        df2 = spark.read.csv(file_two, header=False, schema=users_schema)
    elif t2 == 'zipcodes':
        df2 = spark.read.csv(file_two, header=False, schema=zipcodes_schema)
    elif t2 == 'movies':
        df2 = spark.read.csv(file_two, header=False, schema=movies_schema)
    elif t2 == 'rating':
        df2 = spark.read.csv(file_two, header=False, schema=rating_schema)
    df1.show()
    df2.show()
