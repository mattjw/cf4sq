#!/usr/bin/env python
#
# Copyright 2011 Matthew J Williams & Martin J Chorley
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
from api import *
from venues_api import VenueAPIGateway
from decimal import *
from urllib2 import HTTPError
from random import randint
from datetime import datetime as now
import datetime
import time
import logging

def search_venues( lat, lng, delta, city_code, num_venues=6000 ):
	"""
	Searches for venues in foursquare. Starts at a given point ('lat,'lng') moving outwards, until the 
	given 'num_venues' are found. 'delta' is used to tune the distance that searches are made away from a central point.
	"""
	import _credentials
	dbw = DBWrapper()

	# load credentials
	client_id = _credentials.sc_client_id
	client_secret = _credentials.sc_client_secret
	# use venue gateway not normal gateway so can do 5000 calls an hour
	gateway = VenueAPIGateway( client_id=client_id, client_secret=client_secret )

	logging.info('start venue search crawl %s' % city_code)
	crawl_string = 'Venue Search' + city_code
	dbw.add_crawl_to_database(crawl_string, 'START', now.now())

	# coordinates are stored as Decimal objects to prevent stupid rounding/storage errors
	getcontext().prec = 7

	api = APIWrapper( gateway )
	delta = Decimal(delta)
	start_points = []
	checked_points = []

	start_points.append({'lat':Decimal(lat),'lng':Decimal(lng)})
	logging.info('add start point: (%s, %s)' % (lat, lng))

	# start checking venues
	check_points( start_points, checked_points, api, delta, dbw, num_venues, city_code )

	dbw.add_crawl_to_database(crawl_string, 'FINISH', now.now())

def get_points_surrounding( point, delta ):
	"""
	Get the points surrounding the given central 'point' 'delta' distance away 
	"""
	points = []
	points.append( point )

	lat = point['lat']
	lng = point['lng']

	points.append( {'lat':lat, 'lng':lng+delta} )
	points.append( {'lat':lat+delta, 'lng':lng+delta} )
	points.append( {'lat':lat+delta, 'lng':lng} )
	points.append( {'lat':lat+delta, 'lng':lng-delta} )
	points.append( {'lat':lat, 'lng':lng-delta} )
	points.append( {'lat':lat-delta, 'lng':lng-delta} )
	points.append( {'lat':lat-delta, 'lng':lng} )
	points.append( {'lat':lat-delta, 'lng':lng-delta} )

	return points


def check_points( start_points, checked_points, api, initial_delta, dbw, num_venues, city_code ):
	"""
	Find foursquare venues and add them to the database. 
	"""

	delta = initial_delta
        venues = dbw.get_all_venues(city_code)
	# do we have an area to check or have we found enough venues?
	while start_points and len(dbw.get_all_venues(city_code)) < num_venues:
		# take a new start point
		start_point = start_points[randint(0,len(start_points)-1)]

		lat = start_point['lat']
		lng = start_point['lng']

		logging.info( ' %s: start point: %.7f,%.7f' % (city_code, float(lat), float(lng) ) )
		# how many venues do we have already?
		num_venues_pre = dbw.count_venues_in_database()
		
		points = get_points_surrounding( start_point, delta )

		# for each point and the surrounding points
		for point in points:
			# make sure we haven't already checked it
			if not point in checked_points:
				
				# get the venues in the area
				venues, success = get_venues_near( point['lat'], point['lng'], api )

				# add the venues to the database
				if success:
					for venue in venues:
						dbw.add_venue_to_database( venue, city_code )
				
				# mark this central point as checked
				checked_points.append( start_point )
		
		# how many venues do we have now?
		num_venues_post = dbw.count_venues_in_database()

		# no new venues, so move to a new start point
		if num_venues_pre == num_venues_post:
			logging.info( '%s: no new venues, moving to new start point' % (city_code) )
			# don't check this point again!
			point = start_points.pop(0)
			# reset delta
			delta = initial_delta
			logging.info( '%s: point removed: %.7f %.7f' % ( city_code, float( point['lat'] ), float( point['lng'] ) ) )
			# get some new start points away from this point
			points = get_points_surrounding( point, delta )
			for point in points:
				if not point in start_points and not point in checked_points:
					start_points.append( point )
					logging.info( '%s: new start point: %.7f, %.7f' % ( city_code, float( point['lat'] ), float( point['lng'] ) ) )
		# found new venues, so search this area more
		else:
			logging.info( '%s: new venues, keeping start point and decreasing radius' % city_code )
			delta = delta / 2

		# let us know how we're doing
		logging.info( '%s: delta: %.6f\n' % (city_code, delta ) )
		logging.info( '%s: venues: %d\n' % (city_code, len(dbw.get_all_venues(city_code))))
		logging.info( '%s: start_points: %d\n' % (city_code, len(start_points) ) )

def get_venues_near( lat, lng, api ):
	# try and get some venues, and try and deal with any errors that occur.
	delay = 1
	while True:
		try:
			venues = api.find_venues_near( lat, lng )
			return venues, True
		except HTTPError as e:
			if e.code in [500,501,502,503,504]:
				logging.debug('%s error, sleeping for %d seconds' % (e.code, delay))
				time.sleep(delay)
				if delay < (60 * 15):
					delay = delay * 2
				else:
					return venues, False
			if e.code in [400,401,403,404,405]:
				return venues, False
				logging.debug('%s error, moving on' % e.code)	
			logging.debug(e)
		except Exception as e:
			logging.debug('General Error, retrying')
			logging.debug(e)

if __name__ == "__main__":

	CDF = ['51.476251', '-3.17509']
	BRS = ['51.450477', '-2.59466']
	CAM = ['52.207870', '0.12712']

	search_venues(CDF[0], CDF[1],'0.0050000', 'CDF')
	search_venues(BRS[0], BRS[1],'0.0050000', 'BRS')
	search_venues(CAM[0], CAM[1],'0.0050000', 'CAM')
