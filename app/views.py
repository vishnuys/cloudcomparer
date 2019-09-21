from django.shortcuts import render
from django.views.generic import TemplateView
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.http import HttpResponse, HttpResponseBadRequest
from spark.sparker import execute_query1, execute_query2
from time import time
from json import dumps
from .helper import handle_condition
from traceback import print_exc
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
        spark_result = ''
        if 'group' in query.lower():
            query_type = 'two'
        elif 'inner' in query.lower():
            query_type = 'one'
        query_list = query.split()
        query_list = [q for q in query_list if query != '']
        spark_start = time()
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
            except TypeError:
                print_exc()
                spark_result = 'Invalid comparison operator. Please enter valid comparison operator.'
            except Exception:
                print_exc()
                spark_result = 'Error occured while processing query.'
            try:
                spark_result = execute_query1(t1, t1_alias, t2, t2_alias, c1, c2)
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
        spark_end = time()
        spark_time = spark_end - spark_start
        end_result = {
            'spark_time': spark_time,
            'spark_result': spark_result
        }
        return HttpResponse(dumps(end_result))
