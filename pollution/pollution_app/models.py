from django.db import models


class Country(models.Model):
    name = models.CharField(max_length=48)
    code = models.CharField(max_length=2)


class City(models.Model):
    name = models.CharField(max_length=128)
    country = models.ForeignKey(Country, related_name='cities')


class Location(models.Model):
    name = models.CharField(max_length=128)
    latitude = models.DecimalField(max_digits=11, decimal_places=7, null=True)
    longitude = models.DecimalField(max_digits=11, decimal_places=7, null=True)

    city = models.ForeignKey(City, related_name='locations')


class Measurement(models.Model):
    parameter = models.CharField(max_length=6)
    value = models.FloatField()
    unit = models.CharField(max_length=10)
    utc = models.DateTimeField()


    location = models.ForeignKey(Location, related_name='measurements')
