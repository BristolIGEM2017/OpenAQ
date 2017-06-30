import time
import requests


class API(object):
    def __init__(self):
        self.url_base = 'https://api.openaq.org/v1/'

    def __getattr__(self, method_name):
        if method_name in ['cities', 'countries', 'fetches', 'latest',
                           'locations', 'measurements', 'parameters',
                           'sources']:
            def get(**kwargs):
                try:
                    r = requests.get(self.url_base + method_name, params=kwargs)
                except requests.exceptions.RequestException:
                    time.sleep(20)
                    r = requests.get(self.url_base + method_name, params=kwargs)
                return r.json()
            return get
        else:
            raise AttributeError()
