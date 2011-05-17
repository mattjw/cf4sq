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
from urllib2 import HTTPError
from api import *
from exceptions import Exception
from shapely.geometry import Point, Polygon
from datetime import datetime as now
from setproctitle import setproctitle
import logging
import sys

"""
Monitor Checkin script.

This will loop through all the venues for a city and look for checkins. The city to be checked
is determined by the 'city_code' command line argument, which should match a city code that can
be found in the database.

Checks the venue is active before checking the venue, and checks the location falls within a 
circular area of a given size around a central point of the city. This uses the Shapely package 
for doing testing of whether a point is within a given polygon. Shapely can be obtained by the 
usual python methods (easy_install etc) but relies on the GEOS  framework. OS X users can obtain 
a port of GEOS here: http://www.kyngchaos.com/software:frameworks
"""

def get_venue_details( id, aspect=None, userless=False ):
    """
    wrapper routine to call the venues API. 
    """
    while True:
        response = ''
        try :
            response = api.query_resource( "venues", id, aspect=aspect, userless=userless, tenacious=True )
            return response, True
        # anything else, record and try again
        except Exception as e:
            logging.debug( u'CHK_MON Error (Venue deletion/Foursquare down?), moving on. ' )
            return response, False

if __name__ == "__main__":

    import _credentials
    dbw = DBWrapper( )

    #
    # Input & args
    args = sys.argv

    if len(args) is not 2:
        print "Incorrect number of arguments - please supply city_code"
        exit(1)
    else:
        city_code = args[1]

    logging.info( u'CHK_MON Restarted monitor_checkins.py' )
    logging.info( u'CHK_MON Running with city_code: %s' % city_code )
    setproctitle( u'CHK_MON %s' % city_code )

    # load credentials
    client_id = _credentials.client_id[city_code]
    client_secret = _credentials.client_secret[city_code]
    client_tuples = [(client_id, client_secret)]
    access_tokens = _credentials.access_tokens[city_code]

    gateway = APIGateway( access_tokens, 500, client_tuples, 5000 )
    api = APIWrapper( gateway )

    logging.info( u'CHK_MON %s client_id: %s' % ( city_code, client_id ) )
    logging.info( u'CHK_MON %s client_secret: %s' % ( city_code, client_secret ) )
    logging.info( u'CHK_MON %s access_tokens: %s' % ( city_code, access_tokens[0] ) )
    logging.info( u'CHK_MON %s api gateways initialised' % city_code )

    # get the centre point for the city and construct a bounding area
    centre = _credentials.centres[city_code]
    centre = Point(centre[0], centre[1])
    logging.info( u'CHK_MON %s centre: %s' % ( city_code, centre ) )

    polygon = centre.buffer( 0.25, resolution=20 )
    logging.info( u'CHK_MON %s bounding polygon: %s' % ( city_code, polygon ) )

    # retrieve the list of venues from the database
    venues = dbw.get_venues_in_city( city_code )
    logging.info( u'CHK_MON retrieved %d venues from database for %s' % ( len(venues), city_code ) )

    # loop forever checking the venues for checkins
    while True:
        count_venues = 0
        count_checkins = 0
        count_venues_with_checkins = 0
        logging.info( u'CHK_MON start running checkin crawl in %s' % city_code )
        # log the start of a crawl
        crawl_string = 'MONITOR_CHECKINS_' + city_code
        dbw.add_crawl_to_database( crawl_string, 'START', now.now( ) )
        for venue in venues:
            if dbw.is_active( venue ):
                location = venue.location
                lat = location.latitude
                lng = location.longitude
                point = Point(lat,lng)
                if polygon.contains(point):
                    logging.info( u'CHK_MON %s: retrieve details for venue: %s' % ( city_code, venue.name ) )
                    response, success = get_venue_details( venue.foursq_id, userless=True )
                    if success:
                        count_venues = count_venues + 1
                        v = response.get( 'response' )
                        v = v.get( 'venue' )
                        hereNow = v.get( 'hereNow' )
                        count = hereNow.get( 'count' )
                        logging.info( u'CHK_MON %s: checkins found: %d' % ( city_code, count ) )
                        if count > 0:
                            count_venues_with_checkins = count_venues_with_checkins + 1
                            response, success = get_venue_details( venue.foursq_id, aspect="herenow", userless=False )
                            if success:
                                hereNow = response['response']
                                hereNow = hereNow['hereNow']
                                items = hereNow['items']
                                for item in items:
                                    count_checkins = count_checkins + 1
                                    logging.info( u'CHK_MON %s: Adding checkin' % city_code )
                                    dbw.add_checkin_to_database(item, venue )
                    else:
                        logging.info( u'STAT_CHK %s: Error for venue: %s, id: %s' % ( venue.city_code, venue.name, venue.foursq_id ) )
        # log the end of the crawl
        dbw.add_crawl_to_database(crawl_string, 'FINISH', now.now( ) )
        logging.info( u'CHK_MON %s venues checked: %d' % ( city_code, count_venues ) )
        logging.info( u'CHK_MON %s venues with checkins: %d' % ( city_code, count_venues_with_checkins ) )
        logging.info( u'CHK_MON %s checkins: %d' % ( city_code, count_checkins ) )
