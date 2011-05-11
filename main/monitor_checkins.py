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
from datetime import datetime as now
import math
import logging
import sys

"""
Uses the Shapely package for doing testing of whether a point is within a given search location. 

Shapely can be obtained by the usual python methods (easy_install etc) but does rely on the GEOS framework. 
OS X users can obtain a port of GEOS here: http://www.kyngchaos.com/software:frameworks
"""

def get_venue_details( id ):
	delay = 1
	while True:
		try :
			response = venue_api.query_resource( "venues", id )
			return response, True
		except HTTPError as e:
			if e.code in [500,501,502,503,504]:
				logging.debug('%s error, sleeping for %d seconds' % (e.code, delay))
				time.sleep(delay)
				if delay < (60 * 15):
					delay = delay * 2
				else:
					return response, False
			if e.code in [400,401,403,404,405]:
				return response, False
				logging.debug('%s error, moving on' % e.code)	
			logging.debug(e)
		except Exception as e:
			logging.debug('General Error, retrying')
			logging.debug(e)

def point_inside_polygon(point,poly):
	return poly.contains(point)

if __name__ == "__main__":

	import _credentials
	dbw = DBWrapper()
	#
	# Input & args
	args = sys.argv

	if len(args) is not 2:
		print "Incorrect number of arguments"
		print "Argument pattern: city_code"
		exit(1)
	else:
		city_code = args[1]

	logging.info('Restarted monitor_checkins.py')
	logging.info('Running with city_code: %s' % city_code)

	# load credentials
	client_id = _credentials.client_id[city_code]
	client_secret = _credentials.client_secret[city_code]
	access_tokens = _credentials.access_tokens[city_code]

	logging.info('%s client_id: %s' % (city_code, client_id))
	logging.info('%s client_secret: %s' % (city_code, client_secret))
	logging.info('%s access_tokens: %s' % (city_code, access_tokens[0]))

	centres = {'CDF' : Point(51.476251, -3.17509), 'BRS' : Point(51.450477, -2.59466), 'CAM' : Point(52.207870, 0.12712)}
	centre = centres[city_code]

	logging.info('%s centre: %s' % (city_code, centre))

	# use venue gateway not normal gateway so can do more than 500 calls an hour
	venues = dbw.get_all_venues()
	venue_gateway = VenueAPIGateway( client_id=client_id, client_secret=client_secret, token_hourly_query_quota=5000 )
	gateway = APIGateway( access_tokens=access_tokens, token_hourly_query_quota=500 )

	api = APIWrapper( gateway )
	venue_api = APIWrapper( venue_gateway )

	logging.info('%s api gateways initialised' % city_code)

	polygon = centre.buffer(0.25, resolution=20)

	logging.info('%s bounding polygon: %s' % (city_code, polygon))

	while True:
		count_venues = 0
		count_checkins = 0
		count_venues_with_checkins = 0
		logging.info('start running checkin crawl in %s' % city_code)
		crawl_string = 'MONITOR_CHECKINS_' + city_code
		dbw.add_crawl_to_database(crawl_string, 'START', now.now())
		for venue in venues:
			if venue.city_code == city_code:
				if dbw.is_active(venue):
					location = venue.location
					lat = location.latitude
					lng = location.longitude
					point = Point(lat,lng)
					if point_inside_polygon(point, polygon):
						logging.info( '%s: retrieve details for venue: %s' % ( city_code, venue.name.encode('utf-8') ) )
						response = get_venue_details( venue.foursq_id )
						count_venues = count_venues + 1
						v = response.get( 'response' )
						v = v.get( 'venue' )
						hereNow = v.get( 'hereNow' )
						count = hereNow.get( 'count' )
						logging.info( '%s: checkins found: %d' % ( city_code, count ) )
						if count > 0:
							count_venues_with_checkins = count_venues_with_checkins + 1
							response = api.query_resource( "venues", venue.foursq_id, "herenow" )
							hereNow = response['response']
							hereNow = hereNow['hereNow']
							items = hereNow['items']
							for item in items:
								count_checkins = count_checkins + 1
								logging.info( '%s: Adding checkin' % city_code )
								dbw.add_checkin_to_database(item, venue)
		dbw.add_crawl_to_database(crawl_string, 'FINISH', now.now( ))
		logging.info( '%s venues checked: %d' % ( city_code, count_venues ) )
		logging.info( '%s venues with checkins: %d' % ( city_code, count_venues_with_checkins ) )
		logging.info( '%s checkins: %d' % ( city_code, count_checkins ) )

	

