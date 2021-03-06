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
import datetime
import time
import logging

def search_venues( lat, lng, delta, num_venues=5000 ):
	"""
	Searches for venues in foursquare. Starts at a given point ('lat,'lng') moving outwards, until the 
	given 'num_venues' are found. 'delta' is used to tune the distance that searches are made away from a central point.
	"""
	import _credentials

	# load credentials
	client_id = _credentials.client_id
	client_secret = _credentials.client_secret
	# use venue gateway not normal gateway so can do 5000 calls an hour
	gateway = VenueAPIGateway( client_id=client_id, client_secret=client_secret )

	# coordinates are stored as Decimal objects to prevent stupid rounding/storage errors
	getcontext().prec = 7

	api = APIWrapper( gateway )
	delta = Decimal(delta)
	start_points = []
	checked_points = []

	start_points.append({'lat':Decimal(lat),'lng':Decimal(lng)})

	dbw = DBWrapper()
	# start checking venues
	check_points( start_points, checked_points, api, delta, dbw, num_venues )

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


def check_points( start_points, checked_points, api, initial_delta, dbw, num_venues ):
	"""
	Find foursquare venues and add them to the database. 
	"""
	logging.basicConfig( filename="4sq.log", level=logging.DEBUG, 
		datefmt='%d/%m/%y|%H:%M:%S', format='|%(asctime)s|%(levelname)s| %(message)s'  )
	logging.info( 'venue search initiated' )

	delta = initial_delta
	# do we have an area to check or have we found enough venues?
	while start_points and dbw.count_venues_in_database() < num_venues:
		# take a new start point
		start_point = start_points[randint(0,len(start_points)-1)]

		lat = start_point['lat']
		lng = start_point['lng']

		logging.info( 'start point: %.6f,%.6f\n' % ( float(lat), float(lng) ) )
		# how many venues do we have already?
		num_venues_pre = dbw.count_venues_in_database()
		
		points = get_points_surrounding( start_point, delta )

		# for each point and the surrounding points
		for point in points:
			# make sure we haven't already checked it
			if not point in checked_points:
				
				# get the venues in the area
				venues = get_venues_near( point['lat'], point['lng'], api )

				# add the venues to the database
				for venue in venues:
					dbw.add_venue_to_database( venue )
				
				# mark this central point as checked
				checked_points.append( start_point )
				# record the search
				dbw.add_search_to_database( datetime.datetime.now(), lat, lng )
		
		# how many venues do we have now?
		num_venues_post = dbw.count_venues_in_database()

		# no new venues, so move to a new start point
		if num_venues_pre == num_venues_post:
			logging.info( 'no new venues, moving to new start point\n' )
			# don't check this point again!
			point = start_points.pop(0)
			# reset delta
			delta = initial_delta
			logging.info( 'point removed: %.7f %.7f\n' % ( float( point['lat'] ), float( point['lng'] ) ) )
			# get some new start points away from this point
			points = get_points_surrounding( point, delta )
			for point in points:
				if not point in start_points and not point in checked_points:
					start_points.append( point )
					logging.info( 'new start point: %.7f, %.7f\n' % ( float( point['lat'] ), float( point['lng'] ) ) )
		# found new venues, so search this area more
		else:
			logging.info( 'new venues, keeping start point and decreasing radius' )
			delta = delta / 2

		# let us know how we're doing
		logging.info( 'delta: %.6f\n' % ( delta ) )
		logging.info( 'venues: %d\n' % (dbw.count_venues_in_database()) )
		logging.info( 'start_points: %d\n' % len(start_points) )

def get_venues_near( lat, lng, api ):
	# try and get some venues, and try and deal with any errors that occur.
	while True:
		try:
			venues = api.find_venues_near( lat, lng )
			return venues
		except HTTPError as e:
			logging.debug(e)
			if e.code == 403:
				logging.debug('got an error, sleeping')
				time.sleep(60*60*15)

if __name__ == "__main__":
	search_venues('51.453470', '-2.591060','0.0050000' )	
