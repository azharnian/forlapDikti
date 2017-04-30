from django.shortcuts import render
from django.http import HttpResponse

def forlapdikti(request):
	#return HttpResponse('Hi!')
	
	return render(request, 'web/index.html', {
		'siteName' : 'Geographical Information System of Forlap Dikti',
		'name' : 'ForlapDikti',
		'content' : 'Maps',
		})

def about(request):
	#return HttpResponse('Hi!')
	
	return render(request, 'web/about.html', {
		'siteName' : 'About Us',
		'name' : 'ForlapDikti',
		'content' : 'Maps',
		})