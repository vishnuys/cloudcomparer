from django.shortcuts import render
from django.views.generic import TemplateView
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.http import HttpResponse
from spark.sparker import execute_query1
from time import time
from json import dumps
# from IPython import embed


# Create your views here.
class DefaultView(TemplateView):

    def get(self, request):
        return render(request, 'dummy.html')

    @method_decorator(csrf_exempt)
    def post(self, request):
        query = request.POST['query']
        query_type = ''
        if 'group' in query.lower():
            query_type = 'two'
        elif 'inner' in query.lower():
            query_type = 'one'
        queryList = query.split()
        queryList = [q for q in queryList if query != '']
        if query_type == 'one':
            fromIndex = queryList.index('FROM')
            innerIndex = queryList.index('INNER')
            joinIndex = queryList.index('JOIN')
            onIndex = queryList.index('ON')
            whereIndex = queryList.index('WHERE')
            t1, t1_alias = queryList[fromIndex + 1:innerIndex]
            t2, t2_alias = queryList[joinIndex + 1:onIndex]
            c1 = queryList[onIndex + 1:whereIndex]
            c2 = queryList[whereIndex + 1:]
            spark_start = time()
            spark_result = execute_query1(t1, t1_alias, t2, t2_alias, c1, c2)
            spark_end = time()
        spark_time = spark_end - spark_start
        end_result = {
            'spark_time': spark_time,
            'spark_result': spark_result
        }
        return HttpResponse(dumps(end_result))
