import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from .schemas import *
import json

sc = SparkContext('local', 'cloud')
spark = SparkSession(sc)


def execute_query1(t1, t1_alias, t2, t2_alias, attr1, attr2, c2_attr, parameter, operator):
    operations_performed = []
    file_one = os.path.join('files', t1 + '.csv')
    file_two = os.path.join('files', t2 + '.csv')

    if t1 == 'users':
        df1 = spark.read.csv(file_one, header=False, schema=users_schema)
        operations_performed.append('Read File 1 and Create RDD according to table schema: Users')
    elif t1 == 'zipcodes':
        df1 = spark.read.csv(file_one, header=False, schema=zipcodes_schema)
        operations_performed.append('Read File 1 and Create RDD according to table schema: Zipcodes')
    elif t1 == 'movies':
        df1 = spark.read.csv(file_one, header=False, schema=movies_schema)
        operations_performed.append('Read File 1 and Create RDD according to table schema: Movies')
    elif t1 == 'rating':
        df1 = spark.read.csv(file_one, header=False, schema=rating_schema)
        operations_performed.append('Read File 1 and Create RDD according to table schema: Rating')
    else:
        return 'No such table found. Please enter valid Table 1 name.', operations_performed

    if t2 == 'users':
        df2 = spark.read.csv(file_two, header=False, schema=users_schema)
        operations_performed.append('Read File 2 and Create RDD according to table schema: Users')
    elif t2 == 'zipcodes':
        df2 = spark.read.csv(file_two, header=False, schema=zipcodes_schema)
        operations_performed.append('Read File 2 and Create RDD according to table schema: Zipcodes')
    elif t2 == 'movies':
        df2 = spark.read.csv(file_two, header=False, schema=movies_schema)
        operations_performed.append('Read File 2 and Create RDD according to table schema: Movies')
    elif t2 == 'rating':
        df2 = spark.read.csv(file_two, header=False, schema=rating_schema)
        operations_performed.append('Read File 2 and Create RDD according to table schema: Rating')
    else:
        return 'No such table found. Please enter valid Table 2 name.', operations_performed

    joined = df1.join(df2, attr1)
    operations_performed.append('Perform Inner Join based on attribute: ' + attr1)

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
        return 'Invalid comparision operator obtained. Please enter valid comparision operator.', operations_performed
    operations_performed.append('Filter the result based on the condition: ' + " ".join([c2_attr, operator, str(parameter)]))

    operations_performed.append('Convert obtained DF to JSON')
    return filtered.toJSON().map(lambda j: json.loads(j)).collect(), operations_performed


def execute_query2(table, groupParameters, selectList, functionType, functionParameter, havingFunction, havingOperator, havingParameter):
    csv_file = os.path.join('files', table + '.csv')
    operations_performed = []
    if table == 'users':
        dataframe = spark.read.csv(csv_file, header=False, schema=users_schema)
        operations_performed.append('Read File and Create RDD according to table schema: Users')
    elif table == 'zipcodes':
        dataframe = spark.read.csv(csv_file, header=False, schema=zipcodes_schema)
        operations_performed.append('Read File and Create RDD according to table schema: Zipcodes')
    elif table == 'movies':
        dataframe = spark.read.csv(csv_file, header=False, schema=movies_schema)
        operations_performed.append('Read File and Create RDD according to table schema: Movies')
    elif table == 'rating':
        dataframe = spark.read.csv(csv_file, header=False, schema=rating_schema)
        operations_performed.append('Read File and Create RDD according to table schema: Rating')
    else:
        return 'Invalid table name. Please enter valid table name.', operations_performed

    grouped = dataframe.groupBy(groupParameters)
    operations_performed.append('Group RDD based on Parameter: ' + ', '.join(groupParameters))

    if functionType.lower() == 'count':
        result = grouped.count()
        operations_performed.append('Perform aggregation function: Count')
    elif functionType.lower() == 'min':
        result = grouped.min()
        operations_performed.append('Perform aggregation function: Min')
    elif functionType.lower() == 'max':
        result = grouped.max()
        operations_performed.append('Perform aggregation function: Max')
    elif functionType.lower() == 'sum':
        result = grouped.sum()
        operations_performed.append('Perform aggregation function: Sum')
    else:
        return "Invalid Aggregation function. Please enter valid aggregation function", operations_performed

    filtered = result

    if len(havingFunction) > 0:
        operations_performed.append('Apply having condition: ' + " ".join([havingFunction, havingOperator, str(havingParameter)]))
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
            return 'Invalid comparision operator obtained. Please enter valid comparision operator.', operations_performed

    operations_performed.append('Convert obtained DF to JSON')
    return filtered.toJSON().map(lambda j: json.loads(j)).collect(), operations_performed
