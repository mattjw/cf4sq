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
import logging

"""
Uses the Shapely package for doing testing of whether a point is within a given search location. Shapely can be obtained
by the usual python methods (easy_install etc) but does rely on the GEOS framework. OS X users can obtain a port of GEOS here:
http://www.kyngchaos.com/software:frameworks
"""

def get_venue_details( id ):
	while True:
		try :
			response = venue_api.query_resource( "venues", id )
			return response
		except Exception, e:
			logging.debug(e)

def point_inside_polygon(point,poly):
	return poly.contains(point)


if __name__ == "__main__":
	
	#
	# Logging
	logging.basicConfig( filename="4sq.log", level=logging.DEBUG, 
		datefmt='%d/%m/%y|%H:%M:%S', format='|%(asctime)s|%(levelname)s| %(message)s'  )
	logging.info( 'checkin monitor initiated' )

	import _credentials
	dbw = DBWrapper()
	# load credentials
	client_id = _credentials.client_id
	client_secret = _credentials.client_secret
	access_tokens = _credentials.access_tokens
	# use venue gateway not normal gateway so can do more than 500 calls an hour
	venues = dbw.get_all_venues()#_with_checkins()
	if len(venues)*3 < 5000:
		calls = len(venues)*3
	else:
		calls = 5000
	venue_gateway = VenueAPIGateway( client_id=client_id, client_secret=client_secret, token_hourly_query_quota=calls )
	gateway = APIGateway( access_tokens=access_tokens, token_hourly_query_quota=450 )

	api = APIWrapper( gateway )
	venue_api = APIWrapper( venue_gateway )
	cardiff_polygon = Polygon(_credentials.cardiff_polygon)
	while True:
		count_venues = 0
		count_checkins = 0
		count_venues_with_checkins = 0
		for venue in venues:
			location = venue.location
			lat = location.latitude
			lng = location.longitude
			point = Point(lat,lng)
			if point_inside_polygon(point, cardiff_polygon):
				logging.info( 'retrieve details for venue: %s\n' % ( venue.name.encode('utf-8') ) )
				response = get_venue_details( venue.foursq_id )
				count_venues = count_venues + 1
				v = response.get( 'response' )
				v = v.get( 'venue' )
				stats = v.get( 'stats' )
				dbw.add_statistics_to_database(venue,stats)
				hereNow = v.get( 'hereNow' )
				count = hereNow.get( 'count' )
				logging.info( 'checkins found: %d\n' % ( count ) )
				if count > 0:
					count_venues_with_checkins = count_venues_with_checkins + 1
					response = api.query_resource( "venues", venue.foursq_id, "herenow" )
					hereNow = response['response']
					hereNow = hereNow['hereNow']
					items = hereNow['items']
					for item in items:
						count_checkins = count_checkins + 1
						logging.info( 'Adding checkin\n' )
						dbw.add_checkin_to_database(item, venue)
		logging.info( 'venues checked: %d\n' % ( count_venues ) )
		logging.info( 'venues with checkins: %d\n' % ( count_venues_with_checkins ) )
		logging.info( 'checkins: %d\n' % ( count_checkins ) )
		logging.info( 'total checkins in database: %d\n' % dbw.count_checkins_in_database() )

	

