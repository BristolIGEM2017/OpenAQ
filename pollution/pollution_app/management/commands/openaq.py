from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from pollution_app.models import Country, City, Location, Measurement
from math import floor
from datetime import datetime
from .openaq_api import API

api = API()
ENTRIES_PER_PAGE = 1000


def update_countries():
    for country in api.countries(page=1, limit=ENTRIES_PER_PAGE)['results']:

        obj, _ = Country.objects.get_or_create(
            code=country['code'],
            defaults={
                'name': country['name']
            }
        )
        print("{}".format(obj.name))
        update_cities(obj)
        print(obj.name)


def update_cities(country):
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
            print("-{}\x1b[0K".format(obj.name))
            update_locations(obj)
        page += 1
    print("\r\x1b[0K\r\x1b[1A", end="")


def update_locations(city):
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

            print("|-{}\x1b[0K".format(obj.name))
            update_measurements(obj)
        page += 1
    print("\r\x1b[0K\r\x1b[1A", end="")


def update_measurements(location):
    page = floor((1 + location.measurements.count()) / ENTRIES_PER_PAGE) + 1
    stop = False
    while not stop:
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

            print("\r||-{}\x1b[0K".format(measures['date']['utc']), end="")
        page += 1
    print("\r\x1b[0K\r\x1b[1A", end="")


class Command(BaseCommand):
    def handle(self, *args, **options):
        update_countries()
