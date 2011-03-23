#!/usr/bin/env python
#
# Copyright 2011 Martin J Chorley & Matthew J Williams
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from database_wrapper import DBWrapper
from venues_api import VenueAPIGateway
from urllib2 import HTTPError
from api import *
from exceptions import Exception
from shapely.geometry import Point, Polygon
import math

"""
Now uses the Shapely package for doing testing of whether a point is within a given search location. Shapely can be obtained
by the usual python methods (easy_install etc) but does rely on the GEOS framework. OS X users can obtain a port of GEOS here:
http://www.kyngchaos.com/software:frameworks
"""

def get_venue_details( id ):
	response = []
	while not response:
		try :
			response = venue_api.query_resource( "venues", id )
		# not particularly proud of this :-)
		except Exception as e:
			print e
			pass
	return response

def point_inside_polygon(point,poly):
	return poly.contains(point)


if __name__ == "__main__":
	
	f=open('monitor_checkins.log', 'w')
	f.write( 'running\n' )
	f.flush()

	import _credentials
	
	# load credentials
	client_id = _credentials.client_id
	client_secret = _credentials.client_secret
	access_tokens = _credentials.access_tokens
	# use venue gateway not normal gateway so can do more than 500 calls an hour
	venue_gateway = VenueAPIGateway( client_id=client_id, client_secret=client_secret, token_hourly_query_quota=1000 )
	gateway = APIGateway( access_tokens=access_tokens, token_hourly_query_quota=500 )

	dbw = DBWrapper()

	api = APIWrapper( gateway )
	venue_api = APIWrapper( venue_gateway )
	cardiff_polygon = Polygon([(51.4846,-3.2314),(51.4970,-3.2162),(51.5043,-3.1970),(51.5010,-3.1575),(51.4831,-3.1411),(51.4660,-3.1356),(51.4514,-3.1562),(51.4260,-3.1692),(51.4320,-3.1878)])
	while True:
		count_venues = 0
		count_checkins = 0
		count_venues_with_checkins = 0
		for venue in dbw.get_all_venues():
			stats = venue.statistics
			users = 0
			for stat in stats:
				users = max( users, stat.users )
			if users >= 2:
				location = venue.location
				lat = location.latitude
				lng = location.longitude
				point = Point(lat,lng)
				if point_inside_polygon(point, cardiff_polygon):
					f.write( 'Retrieve details for venue: %s\n' % ( venue.name.encode('utf-8') ) )
					f.flush()
					response = get_venue_details( venue.foursq_id )
					count_venues = count_venues + 1
					v = response.get( 'response' )
					v = v.get( 'venue' )
					stats = v.get( 'stats' )
					dbw.add_statistics_to_database(venue,stats)
					hereNow = v.get( 'hereNow' )
					count = hereNow.get( 'count' )
					f.write( 'checkins found: %d\n' % ( count ) )
					f.flush()
					if count > 0:
						count_venues_with_checkins = count_venues_with_checkins + 1
						response = api.query_resource( "venues", venue.foursq_id, "herenow" )
						hereNow = response['response']
						hereNow = hereNow['hereNow']
						items = hereNow['items']
						for item in items:
							count_checkins = count_checkins + 1
							f.write( 'Adding checkin\n' )
							f.flush()
							dbw.add_checkin_to_database(item, venue)
		f.write( 'venues checked: %d\n' % ( count_venues ) )
		f.write( 'venues with checkins: %d\n' % ( count_venues_with_checkins ) )
		f.write( 'checkins: %d\n' % ( count_checkins ) )
		f.write( 'total checkins in database: %d' dbw.count_checkins_in_database() )
		f.flush()

	

