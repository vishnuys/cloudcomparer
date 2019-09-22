import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from .schemas import *
import json

sc = SparkContext('local', 'cloud')
spark = SparkSession(sc)


def execute_query1(t1, t1_alias, t2, t2_alias, attr1, attr2, c2_attr, parameter, operator):
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
    else:
        return 'No such table found. Please enter valid Table 1 name.'

    if t2 == 'users':
        df2 = spark.read.csv(file_two, header=False, schema=users_schema)
    elif t2 == 'zipcodes':
        df2 = spark.read.csv(file_two, header=False, schema=zipcodes_schema)
    elif t2 == 'movies':
        df2 = spark.read.csv(file_two, header=False, schema=movies_schema)
    elif t2 == 'rating':
        df2 = spark.read.csv(file_two, header=False, schema=rating_schema)
    else:
        return 'No such table found. Please enter valid Table 2 name.'
    joined = df1.join(df2, attr1)
    if operator == '=':
        filtered = joined.filter(col(c2_attr) == parameter)
    elif operator == '<':
        filtered = joined.filter(col(c2_attr) < parameter)
    elif operator == '>':
        filtered = joined.filter(col(c2_attr) > parameter)
    elif operator == '<=':
        filtered = joined.filter(col(c2_attr) <= parameter)
    elif operator == '>=':
        filtered = joined.filter(col(c2_attr) >= parameter)
    elif operator == '<>' or operator == '!=':
        filtered = joined.filter(col(c2_attr) != parameter)
    else:
        return 'Invalid comparision operator obtained. Please enter valid comparision operator.'

    return filtered.toJSON().map(lambda j: json.loads(j)).collect()


def execute_query2(table, groupParameters, selectList, functionType, functionParameter, havingFunction, havingOperator, havingParameter):
    csv_file = os.path.join('files', table + '.csv')
    if table == 'users':
        dataframe = spark.read.csv(csv_file, header=False, schema=users_schema)
    elif table == 'zipcodes':
        dataframe = spark.read.csv(csv_file, header=False, schema=zipcodes_schema)
    elif table == 'movies':
        dataframe = spark.read.csv(csv_file, header=False, schema=movies_schema)
    elif table == 'rating':
        dataframe = spark.read.csv(csv_file, header=False, schema=rating_schema)
    else:
        return 'Invalid table name. Please enter valid table name.'

    grouped = dataframe.groupBy(groupParameters)

    if functionType.lower() == 'count':
        result = grouped.count()
    elif functionType.lower() == 'min':
        result = grouped.min()
    elif functionType.lower() == 'max':
        result = grouped.max()
    elif functionType.lower() == 'sum':
        result = grouped.sum()
    else:
        return "Invalid Aggregation function. Please enter valid aggregation function"

    filtered = result

    if len(havingFunction) > 0:
        havingFunction = havingFunction.lower()
        col_name = 'count' if 'count' in havingFunction else havingFunction
        selectList.append(col_name)
        if havingOperator == '=':
            filtered = result.filter(col(col_name) == havingParameter)
        elif havingOperator == '<':
            filtered = result.filter(col(col_name) < havingParameter)
        elif havingOperator == '>':
            filtered = result.filter(col(col_name) > havingParameter)
        elif havingOperator == '<=':
            filtered = result.filter(col(col_name) <= havingParameter)
        elif havingOperator == '>=':
            filtered = result.filter(col(col_name) >= havingParameter)
        elif havingOperator == '<>' or havingOperator == '!=':
            filtered = result.filter(col(col_name) != havingParameter)
        else:
            return 'Invalid comparision operator obtained. Please enter valid comparision operator.'

    return filtered.toJSON().map(lambda j: json.loads(j)).collect()
