from datetime import datetime
from pony.orm import Required, Database, Set, Optional

db = Database()
epoch = datetime.fromtimestamp(0)


class Country(db.Entity):
    name = Required(str, 48)
    code = Required(str, 2)

    cities = Set('City')


class City(db.Entity):
    city_name = Required(str, 128)
    country = Required(Country)

    locations = Set('Location')


class Location(db.Entity):
    location_name = Required(str, 128)
    city = Required(City)
    latitude = Optional(float, nullable=True)
    longitude = Optional(float, nullable=True)

    measurements = Set('Measurement')


class Measurement(db.Entity):
    location = Required(Location)
    parameter = Required(str, 6)
    value = Required(float)
    unit = Required(str, 10)
    utc = Required(datetime)
