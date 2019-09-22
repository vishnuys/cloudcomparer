import os
import random
import string
from time import time
from json import dumps
from subprocess import call
from traceback import print_exc
from django.shortcuts import render
from cloudproject.settings import BASE_DIR
from django.views.generic import TemplateView
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from .helper import handle_condition, table_row_mapper
from spark.sparker import execute_query1, execute_query2
from django.http import HttpResponse, HttpResponseBadRequest
# from IPython import embed


# Create your views here.
class DefaultView(TemplateView):

    def get(self, request):
        return render(request, 'dummy.html')

    @method_decorator(csrf_exempt)
    def post(self, request):
        query = request.POST['query']
        query_type = ''
        spark_start = spark_end = 0
        mapreduce_start = mapreduce_end = 0
        spark_result = ''
        mapreduce_result = ''
        if 'group' in query.lower():
            query_type = 'two'
        elif 'inner' in query.lower():
            query_type = 'one'
        query_list = query.split()
        query_list = [q for q in query_list if query != '']
        if query_type == 'one':
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
            try:
                output_folder = ''.join(random.choice(string.ascii_uppercase) for _ in range(8))
                output_path = os.path.join(BASE_DIR, 'output', output_folder)
                fileinput_one = os.path.join('files', t1 + '.csv')
                fileinput_two = os.path.join('files', t2 + '.csv')
                row_1 = table_row_mapper[t1][attr1]
                row_2 = table_row_mapper[t2][attr2]
                cmd = 'hadoop jar /home/ysv/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -input %s %s -mapper "/home/ysv/cloud/cloudproject/mapreduce/mapper_hdfs.py %s %s %s %s" -reducer "/home/ysv/cloud/cloudproject/mapreduce/reducer_hdfs.py %s %s %s %s" -output %s' % (fileinput_one, fileinput_two, t1, t2, row_1, row_2, condition_table, condition_row, operator, parameter, output_path)
                mapreduce_start = time()
                call(cmd, shell=True)
                mapreduce_end = time()
            except Exception:
                print_exc()
                mapreduce_result = 'Error occured while executing mapreduce.'
            try:
                spark_start = time()
                spark_result = execute_query1(t1, t1_alias, t2, t2_alias, attr1, attr2, c2_attr, parameter, operator)
                spark_end = time()
            except Exception:
                print_exc()
                spark_result = 'Error occured while executing spark.'
        elif query_type == 'two':
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
                if 'HAVING' in query_list:
                    havingIndex = query_list.index('HAVING')
                    groupParameters = query_list[byIndex + 1:havingIndex]
                    havingCondition = query_list[havingIndex + 1:]
                else:
                    groupParameters = query_list[byIndex + 1:]
                if len(groupParameters) == 1 and ',' in groupParameters:
                    groupParameters = groupParameters.split(',')
                if ',' in groupParameters:
                    groupParameters.remove(',')
            except Exception:
                print_exc()
                spark_result = 'Error occured while processing query.'
            try:
                spark_result = execute_query2(table, groupParameters, havingCondition, selectList, functionType, functionParameter)
            except Exception:
                print_exc()
                spark_result = 'Error occured while executing spark.'
        else:
            return HttpResponseBadRequest('Invalid SQL Query. Please input valid SQL query.')
        spark_time = spark_end - spark_start
        mapreduce_time = mapreduce_end - mapreduce_start
        end_result = {
            'mapreduce_result': mapreduce_result,
            'mapreduce_time': mapreduce_time,
            'spark_time': spark_time,
            'spark_result': spark_result
        }
        return HttpResponse(dumps(end_result))
