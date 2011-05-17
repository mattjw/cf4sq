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
from urllib2 import HTTPError, URLError
from api import *
from exceptions import Exception
from datetime import datetime as now
from setproctitle import setproctitle

def get_venue_details( id ):
    while True:
        response = ''
        try :
            response = api.query_resource( "venues", id, userless=True, tenacious=True )
            return response, True
        except Exception as e:
            logging.debug( u'STAT_CHK Error (Venue deletion/Foursquare down?), moving on. ' )
            logging.debug( e )
            return response, False


if __name__ == "__main__":

    dbw = DBWrapper()
    setproctitle('STAT_CHK')
    # load credentials
    client_id = _credentials.sc_client_id
    client_secret = _credentials.sc_client_secret
    client_tuples = [(client_id, client_secret)]
    access_tokens = _credentials.sc_access_token

    gateway = APIGateway( access_tokens, 500, client_tuples, 5000 )
    api = APIWrapper( gateway )


    venues = dbw.get_all_venues( )

    crawl_string = 'CHECK_STATS'
    dbw.add_crawl_to_database( crawl_string, 'START', now.now( ) )
    logging.info( u'STAT_CHK started crawl for statistics check' )
    count_venues = 0
    for venue in venues:
        logging.info( u'STAT_CHK %s: retrieve details for venue: %s' % ( venue.city_code, venue.name ) )
        response, success = get_venue_details( venue.foursq_id )
        if success:
            count_venues = count_venues + 1
            v = response.get( 'response' )
            v = v.get( 'venue' )
            stats = v.get( 'stats' )
            dbw.add_statistics_to_database( venue,stats )
            logging.info( u'STAT_CHK %s: checkins found: %d' % ( venue.city_code, stats['checkinsCount'] ) )
        else:
            logging.info( u'STAT_CHK %s: Error for venue: %s, id: %s' % ( venue.city_code, venue.name, venue.foursq_id ) )
    logging.info( u'STAT_CHK venues checked: %d' % ( count_venues ) )

    dbw.add_crawl_to_database( crawl_string, 'FINISH', now.now( ) )
