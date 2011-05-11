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

import logging
import _credentials
from database_wrapper import DBWrapper
from venues_api import VenueAPIGateway
from urllib2 import HTTPError, URLError
from api import *
from exceptions import Exception

def get_venue_details( id ):
	delay = 1
	while True:
		response = ''
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


if __name__ == "__main__":

	dbw = DBWrapper()
	# load credentials
	client_id = _credentials.sc_client_id
	client_secret = _credentials.sc_client_secret
	venue_gateway = VenueAPIGateway( client_id=client_id, client_secret=client_secret, token_hourly_query_quota=5000 )

	venues = dbw.get_all_venues()
	
	venue_api = APIWrapper( venue_gateway )

	crawl_string = 'CHECK_STATS'
	dbw.add_crawl_to_database(crawl_string, 'START', now.now())

	count_venues = 0
	for venue in venues:
		logging.info( '%s: retrieve details for venue: %s' % ( venue.city_code, venue.name.encode('utf-8') ) )
		response, success = get_venue_details( venue.foursq_id )
		if success:
			count_venues = count_venues + 1
			v = response.get( 'response' )
			v = v.get( 'venue' )
			stats = v.get( 'stats' )
			dbw.add_statistics_to_database(venue,stats)	
	logging.info( 'venues checked: %d' % ( count_venues ) )

	dbw.add_crawl_to_database(crawl_string, 'FINISH', now.now())