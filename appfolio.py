
from importio import importio
from importio import latch

from geopy.geocoders import GoogleV3
from geopy.location import Location

import logging, json
import re
from datetime import datetime

from properties.models import Address

import logging
log = logging.getLogger(__name__)

class GoogleV3Custom(GoogleV3):
    def _parse_json(self, page, exactly_one=True):
        '''Returns location, (latitude, longitude) from json feed.'''

        places = page.get('results', [])
        if not len(places):
            self._check_status(page.get('status'))
            return None

        def _get_component(place, component, type='long_name'):
            val = [x[type] for x in place['address_components'] if component in x['types']]
            if val:
                return val.pop()
            else:
                return None

        def parse_place(place):
            '''Get the location, lat, lng from a single json place.'''
            location = {}
            location['num'] = _get_component(place, 'street_number')
            location['unit'] = _get_component(place, 'subpremise')
            location['street'] = _get_component(place, 'route')
            location['city'] = _get_component(place, 'locality')
            location['state'] = _get_component(place, 'administrative_area_level_1', 'short_name')
            location['country'] = _get_component(place, 'country', 'short_name')
            location['postal_code'] = _get_component(place, 'postal_code')
            location['formatted'] = place['formatted_address']

            print location

            latitude = place['geometry']['location']['lat']
            longitude = place['geometry']['location']['lng']
            return Location(location, (latitude, longitude), place)

        if exactly_one:
            return parse_place(places[0])
        else:
            return [parse_place(place) for place in places]

class ScrapperClient:
    def __init__(self):
        self.dataRows = []
        self.client = importio.importio(user_id="b12f3a23-b267-45a4-99f2-b8a0d2e9b491", api_key="YqcifZoCcEPdmXwAL8g855gQ2ZSmtGZwiwaBpj71TMNKsvAXhpvhLiz9mpy5DlC7KIZX62sC+TnaSxhNLfJNXg==", host="https://query.import.io")
        self.geolocator = GoogleV3Custom()

    def _callback(self, query, message):
        if message["type"] == "DISCONNECT":
            log.error("Query in progress when library disconnected: " + json.dumps(message["data"], indent = 4))

        if message["type"] == "MESSAGE":
            if "errorType" in message["data"]:
                log.error("There was an  error during the query: " + json.dumps(message["data"], indent = 4))
            else:
                self.dataRows.extend(message["data"]["results"])
          
        if query.finished(): self.queryLatch.countdown()


    def sync(self, company):
        self.client.connect()

        print "Property List = " + company.url
        self.queryLatch = latch.latch()

        self.client.query({
            "connectorGuids": [
                "903de60e-6edc-49a3-aa6e-671cdb0d8ac5"
            ],
            "input": {
                "webpage/url": company.url,
            }
        }, self._callback)

        self.queryLatch.await()

        links = [x['details_link'] for x in self.dataRows]

        self.dataRows = []

        self.queryLatch = latch.latch(len(links))
        #self.queryLatch = latch.latch(len)
        for link in links:
            self.client.query({
                "connectorGuids": [
                    "897bdd91-24c0-409d-9e29-dffee6f1d64c"
                ],
                "input": {
                    "webpage/url": link,
                }
            }, self._callback)

        print "Waiting...\n"
        self.queryLatch.await()
        print "Finished!\n"

        self.client.disconnect()

        properties = self.dataRows

        print "JSON = " + str(properties)
        active_property_ids = []
        existing_property_ids = map(lambda x: x.pk, company.address_set.all())
        for property in properties:
            published_address = property['address']
            if company.address_set.filter(published_address=published_address).exists():
                existing_property = company.address_set.get(published_address=published_address)

                if 'bed' in property:
                    existing_property.bedrooms = int(property['bed'])

                if 'bath' in property:
                    existing_property.baths = float(property['bath'])

                if 'rent' in property:
                    existing_property.price = int(property['rent'])

                if 'available_on' in property:
                    #existing_property.date_available = datetime.strptime(property['available_on'], "%m/%d/%y").date()
                    existing_property.date_available = datetime.now()

                if 'description' in property:
                    existing_property.description = property['description']

                if 'sqft' in property:
                    existing_property.sqft = int(re.sub(',', '', property['sqft']))

                existing_property.active = True
                existing_property.save()
                active_property_ids.append(existing_property.pk)
            else:
                # Create a new property
                address, (latitude, longitude) = self.geolocator.geocode(published_address)

                new_property = Address(
                    num = int(address['num']),
                    unit = address['unit'],
                    city = address['city'],
                    state = address['state'],
                    country = address['country'],
                    postal_code = int(address['postal_code']),
                    formatted = address['formatted'],
                    published_address = published_address,
                    active = True,
                    company = company, 
                )

                if 'bed' in property:
                    new_property.bedrooms = int(property['bed'])

                if 'bath' in property:
                    new_property.baths = float(property['bath'])

                if 'rent' in property:
                    new_property.price = int(property['rent'])

                if 'available_on' in property:
                    #new_property.date_available = datetime.strptime(property['available_on'], "%m/%d/%y").date()
                    new_property.date_available = datetime.now()

                if 'description' in property:
                    new_property.description = property['description']

                if 'sqft' in property:
                    new_property.sqft = int(re.sub(',', '', property['sqft']))

                new_property.save()
                active_property_ids.append(new_property.pk)

            # see what was removed
            for id in existing_property_ids:
                if id not in active_property_ids:
                    inactive_property = Address.objects.get(pk=id)
                    inactive_property.active = False
                    inactive_property.save()
