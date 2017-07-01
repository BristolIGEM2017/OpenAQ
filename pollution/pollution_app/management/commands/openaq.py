from concurrent import futures
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from pollution_app.models import Country, City, Location, Measurement
from math import floor
from datetime import datetime
from .openaq_api import API

api = API()
ENTRIES_PER_PAGE = 1000


def update_countries(ex):
    for country in api.countries(page=1, limit=ENTRIES_PER_PAGE)['results']:
        obj, _ = Country.objects.get_or_create(
            code=country['code'],
            defaults={
                'name': country['name']
            }
        )
    return "Countries", [
        ex.submit(update_cities, ex, country)
        for country in Country.objects.all()
    ]


def update_cities(ex, country):
    page = floor((1 + country.cities.count()) / ENTRIES_PER_PAGE) + 1
    stop = False
    while not stop:
        result = api.cities(page=page, limit=ENTRIES_PER_PAGE,
                            country=country.code)
        stop = page * result['meta']['limit'] > result['meta']['found']
        for city in result['results']:
            obj, _ = City.objects.get_or_create(
                name=city['city'],
                country=country,
            )
        page += 1
    return "Country {}".format(country.name), [
        ex.submit(update_locations, ex, city)
        for city in City.objects.filter(country=country)
    ]


def update_locations(ex, city):
    page = floor((1 + city.locations.count()) / ENTRIES_PER_PAGE) + 1
    stop = False
    while not stop:
        result = api.locations(page=page, limit=ENTRIES_PER_PAGE,
                               city=city.name)
        stop = page * result['meta']['limit'] > result['meta']['found']
        for location in result['results']:
            coordinates = location.get('coordinates', {})

            obj, _ = Location.objects.get_or_create(
                name=location['location'],
                city=city,
                defaults={
                    'name': location['location'],
                    'latitude': coordinates.get('latitude', None),
                    'longitude': coordinates.get('longitude', None),
                    'city': city
                }
            )
        page += 1
    return "City {}".format(city.name), [
        ex.submit(update_measurements, ex, location)
        for location in Location.objects.filter(city=city)
    ]


def update_measurements(ex, location):
    page = floor((1 + location.measurements.count()) / ENTRIES_PER_PAGE) + 1
    result = api.measurements(page=page, limit=ENTRIES_PER_PAGE,
                              location=location.name, sort="asc",
                              order_by="date")
    stop = page * result['meta']['limit'] > result['meta']['found']
    for measures in result['results']:
        date = datetime.strptime(measures['date']['utc'],
                                 '%Y-%m-%dT%H:%M:%S.%fZ')
        date = date.replace(tzinfo=timezone.utc)

        Measurement.objects.get_or_create(
            location=location,
            utc=date,
            parameter=measures['parameter'],
            defaults={
                'parameter': measures['parameter'],
                'value': measures['value'],
                'unit': measures['unit'],
                'utc': date,
                'location': location
            }
        )

    page += 1
    return (
        "Location page {} {}".format(page, location.name),
        [] if stop else [ex.submit(update_measurements, ex, location)]
    )


class Command(BaseCommand):
    def handle(self, *args, **options):
        with futures.ThreadPoolExecutor(max_workers=10) as ex:
            tasks = set([ex.submit(update_countries, ex)])
            while tasks:
                done, tasks = futures.wait(
                    tasks, return_when=futures.FIRST_COMPLETED)
                for f in done:
                    output, futs = f.result()
                    tasks |= set(futs)
                    print("{} {}".format(len(tasks), output))
