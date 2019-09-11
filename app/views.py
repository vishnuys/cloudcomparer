from django.shortcuts import render
from django.views.generic import TemplateView
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.http import HttpResponse


# Create your views here.

class DefaultView(TemplateView):

	def  get(self, request):
		return render(request, 'dummy.html')

	@method_decorator(csrf_exempt)
	def post(self, request):
		print(request.POST)
		return HttpResponse('')
