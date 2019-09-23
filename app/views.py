import os
import random
import string
from time import time
from traceback import print_exc
from django.shortcuts import render
from subprocess import call, Popen, PIPE
from django.views.generic import TemplateView
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from .helper import handle_condition, table_row_mapper
from spark.sparker import execute_query1, execute_query2
from django.http import HttpResponseBadRequest, JsonResponse
from cloudproject.settings import BASE_DIR, HADOOP_STREAMER_PATH, HDFS_CSV_PATH


# Create your views here.
class DefaultView(TemplateView):

    def get(self, request):
        return render(request, 'dummy.html')

    @method_decorator(csrf_exempt)
    def post(self, request):
        query = request.POST['query']

        # Initializations #
        query_type = ''
        spark_start = spark_end = 0
        mapreduce_start = mapreduce_end = 0
        spark_result = ''
        mapreduce_result = ''
        mapper_input = {}
        reducer_input = {}
        spark_operations = []
        spark_result_count = 0
        mapreduce_result_count = 0

        # Determine Query type #
        if 'group' in query.lower():
            query_type = 'two'
        elif 'inner' in query.lower():
            query_type = 'one'
        query_list = query.split()
        query_list = [q for q in query_list if query != '']

        if query_type == 'one':

            # Process Query one and extract data #
            try:
                fromIndex = query_list.index('FROM')
                innerIndex = query_list.index('INNER')
                joinIndex = query_list.index('JOIN')
                onIndex = query_list.index('ON')
                whereIndex = query_list.index('WHERE')
                table1 = query_list[fromIndex + 1:innerIndex]
                if len(table1) == 2:
                    t1, t1_alias = table1[0], table1[1]
                else:
                    t1 = table1[0]
                    t1_alias = ''
                table2 = query_list[joinIndex + 1:onIndex]
                if len(table2) == 2:
                    t2, t2_alias = table2[0], table2[1]
                else:
                    t2 = table2[0]
                    t2_alias = ''
                c1 = query_list[onIndex + 1:whereIndex]
                c2 = query_list[whereIndex + 1:]
                if len(c1) < 3:
                    c1 = handle_condition(c1)
                if len(c2) < 3:
                    c2 = handle_condition(c2)
                if '.' in c1[0]:
                    t1_alias_obt, attr1 = c1[0].split('.')
                else:
                    attr1 = c1[0]
                if '.' in c1[2]:
                    t2_alias_obt, attr2 = c1[2].split('.')
                else:
                    attr2 = c1[2]

                c2_alias_obt, c2_attr = c2[0].split('.')

                if c2_alias_obt == t1 or c2_alias_obt == t1_alias or c2_alias_obt == t1_alias_obt:
                    condition_table = t1
                    condition_row = table_row_mapper[t1][c2_attr]
                elif c2_alias_obt == t2 or c2_alias_obt == t2_alias or c2_alias_obt == t2_alias_obt:
                    condition_table = t2
                    condition_row = table_row_mapper[t2][c2_attr]
                else:
                    raise TypeError('Invalid table name/alias in where clause.')
                parameter = int(c2[2]) if c2[2].isnumeric() else c2[2].strip('"').strip("'")
                operator = c2[1]
            except TypeError:
                print_exc()
                spark_result = 'Invalid SQL syntax. Please check your SQL query and try again.'
            except SyntaxError:
                print_exc()
                spark_result = 'Invalid comparison operator. Please enter valid comparison operator.'
            except Exception:
                print_exc()
                spark_result = 'Error occured while processing query.'

            # Execute Mapreduce #
            try:
                output_folder = ''.join(random.choice(string.ascii_uppercase) for _ in range(8))
                output_path = os.path.join('output', output_folder)
                fileinput_one = os.path.join(HDFS_CSV_PATH, t1 + '.csv')
                fileinput_two = os.path.join(HDFS_CSV_PATH, t2 + '.csv')
                row_1 = table_row_mapper[t1][attr1]
                row_2 = table_row_mapper[t2][attr2]
                mapper_path = os.path.join(BASE_DIR, 'mapreduce/mapper_hdfs.py')
                reducer_path = os.path.join(BASE_DIR, 'mapreduce/reducer_hdfs.py')
                mapper_input = {
                    'input_file_one': fileinput_one,
                    'input_file_two': fileinput_two,
                    'table_1': t1,
                    'table_2': t2,
                    'table1_row': row_1,
                    'table2_row': row_2,
                }
                reducer_input = {
                    'mapper_output': 'Output obtained from map function',
                    'where_condition_table': condition_table,
                    'where_condition_row': condition_row,
                    'where_condition_operator': operator,
                    'where_condition_operand': parameter,
                }
                cmd = 'hadoop jar %s -input %s %s -mapper "%s %s %s %s %s" -reducer "%s %s %s %s %s" -output %s' % (HADOOP_STREAMER_PATH, fileinput_one, fileinput_two, mapper_path, t1, t2, row_1, row_2, reducer_path, condition_table, condition_row, operator, parameter, output_path)
                mapreduce_start = time()
                call(cmd, shell=True)
                output_file = os.path.join(output_path, 'part-00000')
                mapreduce_result = []
                map_output = Popen(["hdfs", "dfs", "-cat", output_file], stdout=PIPE)
                for line in map_output.stdout:
                    mapreduce_result_count += 1
                    newline = line.decode("utf-8")
                    mapreduce_result.append(eval(newline))
                mapreduce_end = time()
            except Exception:
                print_exc()
                mapreduce_result = 'Error occured while executing mapreduce.'

            # Execute Spark #
            try:
                spark_start = time()
                spark_result, spark_operations = execute_query1(t1, t1_alias, t2, t2_alias, attr1, attr2, c2_attr, parameter, operator)
                spark_end = time()
            except Exception:
                print_exc()
                spark_result = 'Error occured while executing spark.'
        elif query_type == 'two':

            # Process Query two and extract data #
            try:
                selectIndex = query_list.index('SELECT')
                fromIndex = query_list.index('FROM')
                groupIndex = query_list.index('GROUP')
                byIndex = query_list.index('BY')
                table = query_list[fromIndex + 1:groupIndex][0]
                selectColumns = query_list[selectIndex + 1:fromIndex]
                formattedSelects = []
                for i in selectColumns:
                    if ',' in i:
                        if i[-1] == ',':
                            i = i[:-1]
                        if ',' in i:
                            formattedSelects += i.split(',')
                        else:
                            formattedSelects.append(i)
                    else:
                        formattedSelects.append(i)
                func = [x for x in formattedSelects if '(' in x or ')' in x]
                selectList = [x for x in formattedSelects if x not in func]
                if len(func) == 1:
                    func = func[0]
                    obIndex = func.index('(')
                    cbIndex = func.index(')')
                    functionType = func[:obIndex]
                    functionParameter = func[obIndex + 1:cbIndex]
                elif len(func) == 2:
                    obpart = func[0] if '(' in func[0] else func[1]
                    cbpart = func[1] if ')' in func[1] else func[0]
                    func = obpart + cbpart
                    obIndex = func.index('(')
                    cbIndex = func.index(')')
                    functionType = func[:obIndex]
                    functionParameter = func[obIndex + 1:cbIndex]
                havingCondition = []
                havingFunction = ''
                havingParameter = ''
                havingOperator = ''
                if 'HAVING' in query_list:
                    havingIndex = query_list.index('HAVING')
                    groupParameters = query_list[byIndex + 1:havingIndex]
                    havingCondition = query_list[havingIndex + 1:]
                    if len(havingCondition) == 1:
                        symbols = [x for x in havingCondition[0] if x in ['<', '>', '=', '!']]
                        havingOperator = ''.join(symbols)
                        havingFunction, havingParameter = havingCondition.split(symbols)
                    elif len(havingCondition) == 2:
                        havingOperator = '>'
                        if '>' in havingCondition[0]:
                            havingCondition[0].remove('>')
                        elif '>' in havingCondition[1]:
                            havingCondition[1].remove('>')
                        havingFunction = havingCondition[0]
                        havingParameter = int(havingCondition[1]) if havingCondition[1].isnumeric() else havingCondition[1].strip('"').strip("'")
                    elif len(havingCondition) == 3:
                        havingFunction = havingCondition[0]
                        havingParameter = int(havingCondition[2]) if havingCondition[2].isnumeric() else havingCondition[2].strip('"').strip("'")
                        havingOperator = havingCondition[1]
                else:
                    groupParameters = query_list[byIndex + 1:]
                if len(groupParameters) == 1 and ',' in groupParameters:
                    groupParameters = groupParameters.split(',')
                if ',' in groupParameters:
                    groupParameters.remove(',')
                formattedGroups = []
                for i in groupParameters:
                    if ',' in i:
                        if i[-1] == ',':
                            i = i[:-1]
                        if ',' in i:
                            formattedGroups += i.split(',')
                        else:
                            formattedGroups.append(i)
                    else:
                        formattedGroups.append(i)
                groupParameters = formattedGroups
            except Exception:
                print_exc()
                spark_result = 'Error occured while processing query.'

            # Execute Mapreduce #
            try:
                output_folder = ''.join(random.choice(string.ascii_uppercase) for _ in range(8))
                output_path = os.path.join('output', output_folder)
                fileinput = os.path.join(HDFS_CSV_PATH, table + '.csv')
                rowlist = [table_row_mapper[table][x] for x in groupParameters]
                rowstring = " ".join(list(map(str, rowlist)))
                functionRow = table_row_mapper[table][functionParameter]
                mapper_path = os.path.join(BASE_DIR, 'mapreduce/q2_mapper.py')
                reducer_path = os.path.join(BASE_DIR, 'mapreduce/q2_reducer.py')
                cmd = ' hadoop jar %s -input %s -mapper "%s %s" -reducer "%s %s %s %s" -output %s' % (HADOOP_STREAMER_PATH, fileinput, mapper_path, rowstring, reducer_path, functionType, functionRow, havingParameter, output_path)
                mapper_input = {
                    'input_file': fileinput,
                    'groupby_rows_list': rowlist,
                }
                reducer_input = {
                    'mapper_output': 'Output obtained from map function',
                    'aggregation_function': functionType,
                    'column_row_number': functionRow,
                    'having_condition_operand': havingParameter
                }
                mapreduce_start = time()
                call(cmd, shell=True)
                output_file = os.path.join(output_path, 'part-00000')
                mapreduce_result = []
                map_output = Popen(["hdfs", "dfs", "-cat", output_file], stdout=PIPE)
                for line in map_output.stdout:
                    mapreduce_result_count += 1
                    newline = line.decode("utf-8")
                    mapreduce_result.append(eval(newline))
                mapreduce_end = time()
            except Exception:
                print_exc()
                mapreduce_result = 'Error occured while executing mapreduce.'

            # Execute Spark #
            try:
                spark_start = time()
                spark_result, spark_operations = execute_query2(table, groupParameters, selectList, functionType, functionParameter, havingFunction, havingOperator, havingParameter)
                spark_end = time()
            except Exception:
                print_exc()
                spark_result = 'Error occured while executing spark.'
        else:
            return HttpResponseBadRequest('Invalid SQL Query. Please input valid SQL query.')
        mapreduce_time = mapreduce_end - mapreduce_start  # Compute Mapreduce Runtime
        spark_time = spark_end - spark_start  # Compute Spark Runtime
        end_result = {
            'mapreduce': {
                'mapreduce_time': mapreduce_time,
                'mapper_input': mapper_input,
                'reducer_input': reducer_input,
                'mapreduce_result_count': mapreduce_result_count,
                'mapreduce_result': mapreduce_result,

            },
            'spark': {
                'spark_time': spark_time,
                'spark_result_count': len(spark_result),
                'spark_operations': spark_operations,
                'spark_result': spark_result
            }
        }
        return JsonResponse(end_result)
