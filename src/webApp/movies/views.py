from django.shortcuts import render
from django.http import HttpResponse
from .models import MovieInfo, Recommended


def index(request):
    movies = MovieInfo.objects.all()
    return render(request, 'index.html', {'movies': movies})


def users(request):
    request.encoding = 'utf-8'
    message = request.GET['id']
    index = []
    movies = []
    if message != '':
        if int(message) in range(10001, 20001):
            rec = Recommended.objects.filter(userId=message)
            for item in rec:
                index.append(item.index)
            for i in range(0, len(index)):
                movies.append(MovieInfo.objects.get(index=index[i]))
            return render(request, 'search.html', {'movies': movies})
        else:
            message = '不存在的 user id'
            return HttpResponse(message)
    else:
        message = '请输入user id'
        return HttpResponse(message)
