from django.db import models


class MovieInfo(models.Model):
    index = models.CharField(max_length=5, primary_key=True)
    image = models.CharField(max_length=200)
    title = models.CharField(max_length=50)
    actor = models.CharField(max_length=100)
    time = models.CharField(max_length=50)
    score = models.CharField(max_length=10)

    def __str__(self):
        return str(self.index) + "_" + self.title


class Recommended(models.Model):
    userId = models.CharField(max_length=10)
    index = models.CharField(max_length=5)
    recommended = models.CharField(max_length=50)

    def __str__(self):
        return str(self.userId) + "_" + self.index
