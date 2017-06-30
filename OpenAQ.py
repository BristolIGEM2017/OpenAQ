from datetime import datetime
from math import floor
from OpenAQModels import Country, City, Location, Measurement, db
from OpenAQAPI import API
from pony.orm import db_session, commit
from settings import DB_ARGS, DB_KWARGS

api = API()
ENTRIES_PER_PAGE = 1000

@db_session
def update_countries():
    for country in api.countries(page=1, limit=ENTRIES_PER_PAGE)['results']:
        c = Country.get(code=country['code'])
        if c is None:
            c = Country(name=country['name'], code=country['code'])
        print("{}".format(c.name))
        update_cities(c)
        print(c.name)


@db_session
def update_cities(country):
    page = floor(len(country.cities) / ENTRIES_PER_PAGE) + 1
    stop = False
    while not stop:
        result = api.cities(page=page, limit=ENTRIES_PER_PAGE,
                            country=country.code)
        stop = page * result['meta']['limit'] > result['meta']['found']
        for city in result['results']:
            c = City.get(city_name=city['city'],
                         country=country)
            if c is None:
                c = City(city_name=city['city'],
                         country=country)
            print("-{}\x1b[0K".format(c.city_name))
            update_locations(c)
        page += 1

    print("\r\x1b[0K\r\x1b[1A", end="")


@db_session
def update_locations(city):
    page = floor(len(city.locations) / ENTRIES_PER_PAGE) + 1
    stop = False
    while not stop:
        result = api.locations(page=page, limit=ENTRIES_PER_PAGE,
                               city=city.city_name)
        stop = page * result['meta']['limit'] > result['meta']['found']
        for location in result['results']:
            l = Location.get(location_name=location['location'], city=city)
            if l is None:
                coordinates = location.get('coordinates', {})
                l = Location(location_name=location['location'],
                             latitude=coordinates.get('latitude', None),
                             longitude=coordinates.get('longitude', None),
                             city=city)
            print("|-{}\x1b[0K".format(l.location_name))
            update_measurements(l)
        page += 1
    print("\r\x1b[0K\r\x1b[1A", end="")


@db_session
def update_measurements(location):
    page = floor(len(location.measurements) / ENTRIES_PER_PAGE) + 1
    stop = False
    while not stop:
        result = api.measurements(page=page, limit=ENTRIES_PER_PAGE,
                                  location=location.location_name, sort="asc",
                                  order_by="date")
        stop = page * result['meta']['limit'] > result['meta']['found']
        for measures in result['results']:
            date = datetime.strptime(measures['date']['utc'],
                                     '%Y-%m-%dT%H:%M:%S.%fZ')
            m = Measurement.get(location=location,
                                utc=date,
                                parameter=measures['parameter'])
            if m is None:
                Measurement(parameter=measures['parameter'],
                            value=measures['value'],
                            unit=measures['unit'],
                            utc=date,
                            location=location)
                print("\r||-{}\x1b[0K".format(measures['date']['utc']), end="")
        page += 1
        commit()
    print("\r\x1b[0K\r\x1b[1A", end="")


if __name__ == "__main__":
    db.bind(*DB_ARGS, **DB_KWARGS)
    db.generate_mapping(create_tables=True)

    update_countries()
