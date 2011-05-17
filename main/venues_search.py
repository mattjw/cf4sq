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
from api import *
from urllib2 import HTTPError
from random import randint
from datetime import datetime as now
from shapely.geometry import Point, Polygon
from exceptions import Exception
from setproctitle import setproctitle
import time
import logging
import _credentials

class Location:
    def __init__( self, x, y ):
        self.x = x
        self.y = y
        self.venues = []
        self.checked = False

    def get_venues_near( self, api ):
        lat = self.x
        lng = self.y
        # try and get some venues, and try and deal with any errors that occur.
        ll_str = "%f,%f" % ( float(lat), float(lng) )
        get_qry = { 'll': ll_str, 'intent': 'checkin', 'limit': limit }
        try :
            response = api.query_routine( "venues", "search", get_params=get_qry, userless=True, tenacious=True )
            response = response['response']  # a dict
            groups = response['groups']  # a list
            trending = None
            nearby = None
            for group in groups:
                if group['type'] == 'trending':
                    trending = group  # a three-field dict specifying a collection
                if group['type'] == 'nearby':
                    nearby = group  # a three-field dict specifying a collection
            nearby = nearby['items']  # a list of nearby venues
            self.venues = nearby
            self.checked = True
            return self.venues, self.checked
        # anything else, record and try again
        except Exception as e:
            print e
            logging.debug( u'VEN_SRCH general error, moving on' )
            return self.venues, self.checked

    def get_venues( self, api ):
        if self.checked:
            return self.venues
        else:
            while not self.checked:
                self.get_venues_near( api )
            return self.venues



class Cell:
    """
    An object that represents a geographic cell that may contain venues, specified by a lower
    left (x, y) coordinate and a width and height. 

    Each cell stores the top left, top right, bottom left and bottom right corners of the cell 
    as (x, y) coordinates and will store a list of venues found at each of these points. Comparing 
    the contents of the two lists will reveal if there are points between the corners at which 
    new venues may be found.
    """
    def __init__( self, x, y, width, height ):

        self.bl = Location( x, y )
        self.tl = Location( x,  y + height )
        self.br = Location( x + width, y )
        self.tr = Location( x + width, y + height )

    def get_locations( self ):
        return self.bl, self.tl, self.br, self.tr

    def get_children( self ):
        """
        Checks the list of venues stored for each corner of the cell. If a venue is found in one
        corner and not another, there may be a point between the two at which more venues can be 
        found.

        This method will construct and return child cells between the corners that can be checked 
        later for new venues
        """
        # assume no new venues
        bottom = False
        top = False
        left = False 
        right = False

        # construct lists of venue names
        self.bl_venue_ids = []
        self.tl_venue_ids = []
        self.tr_venue_ids = []
        self.br_venue_ids = []

        for venue in self.bl.venues:
            self.bl_venue_ids.append(venue['id'])
        for venue in self.tl.venues:
            self.tl_venue_ids.append(venue['id'])
        for venue in self.tr.venues:
            self.tr_venue_ids.append(venue['id'])
        for venue in self.br.venues:
            self.br_venue_ids.append(venue['id'])

        # look for any venues in one list that are not found in another
        for venue in self.bl.venues:
            if not venue['id'] in self.br_venue_ids:
                logging.info( u'VEN_SRCH %s venue not found' % venue['name'] )
                bottom = True
                break
        for venue in self.bl.venues:
            if not venue['id'] in self.tl_venue_ids:
                logging.info( u'VEN_SRCH %s venue not found' % venue['name'] )
                left = True
                break
        for venue in self.tr.venues:
            if not venue['id'] in self.br_venue_ids:
                logging.info( u'VEN_SRCH %s venue not found' % venue['name'] )
                right = True
                break
        for venue in self.tr.venues:
            if not venue['id'] in self.tl_venue_ids:
                logging.info( u'VEN_SRCH %s venue not found' % venue['name'] )
                top = True
                break
        children=[]

        # if we found missing venues, create the child cell
        if bottom:
            bcell = Cell( self.x, self.y, self.width/2, self.height/2 )
            logging.info( u'VEN_SRCH bl child cell added' )
            logging.info( u'VEN_SRCH coords: %.7f, %.7f' % ( bcell.x, bcell.y ) )
            children.append(bcell)
        if left:
            lcell = Cell( self.x, self.y + self.height/2, self.width/2, self.height/2 )
            logging.info( u'VEN_SRCH tl child cell added' )
            logging.info( u'VEN_SRCH coords: %.7f, %.7f' % ( lcell.x, lcell.y ) )
            children.append(lcell)
        if right:
            rcell = Cell( self.x + self.width/2, self.y, self.width/2, self.height/2, )
            logging.info( u'VEN_SRCH br child cell added' )
            logging.info( u'VEN_SRCH coords: %.7f, %.7f' % ( rcell.x, rcell.y ) )
            children.append(rcell)
        if top:
            tcell = Cell( self.x + self.width/2, self.y + self.height/2, self.width/2, self.height/2 )
            logging.info( u'VEN_SRCH tr child cell added' )
            logging.info( u'VEN_SRCH coords: %.7f, %.7f' % ( tcell.x, tcell.y ) )
            children.append(tcell)
        return children

def search_venues( city_code ):

    # load the central point
    centre = _credentials.centres[city_code]
    centre = Point(centre[0], centre[1])
    delta = 0.05

    # find the lower left corner of a bounding box
    x = centre.x - delta
    y = centre.y - delta

    # create 4 cells split over the area
    bl_cell = Cell( x, y, delta, delta )
    tl_cell = Cell( x, y + delta, delta, delta )
    br_cell = Cell( x + delta, y, delta, delta )
    tr_cell = Cell( x + delta, y + delta, delta, delta )

    cells = [bl_cell, tl_cell, br_cell, tr_cell]

    # loop over all the cells and search for new venues
    for cell in cells:
        logging.info( u'VEN_SRCH cells length: %d' % len( cells ) )
        children = check_venues(cell, city_code)
        logging.info( u'VEN_SRCH adding %d child cells' % len( children ) )
        for c in children:
            cells.append(c)
        logging.info( u'VEN_SRCH removing parent cell' )
        cells.remove(cell)
        logging.info( u'VEN_SRCH cells length: %d' % len( cells ) )
        

def check_venues( cell, city_code ):
    """
    Check the corners of the cell and search for venues around them
    """
    bl = cell.bl
    tl = cell.tl
    br = cell.br
    tr = cell.tr                 

    cells = [bl,tl,br,tr]

    for corner in cells:



    cell.bl_venues, success = get_venues_near( bl.x, bl.y, api )
    logging.info( u'VEN_SRCH bl_venues:' )
    if success:
        for venue in cell.bl_venues:
            dbw.add_venue_to_database( venue, city_code )
    cell.tl_venues, success = get_venues_near( tl.x, tl.y, api )
    logging.info( u'VEN_SRCH tl_venues:' )
    if success:
        for venue in cell.tl_venues:
            dbw.add_venue_to_database( venue, city_code )
    cell.tr_venues, success = get_venues_near( tr.x, tr.y, api )
    logging.info( u'VEN_SRCH tr_venues:' )
    if success:
        for venue in cell.tr_venues:
            dbw.add_venue_to_database( venue, city_code )
    cell.br_venues, success = get_venues_near( br.x, br.y, api )
    logging.info( u'VEN_SRCH br_venues:' )
    if success:
        for venue in cell.br_venues:
            dbw.add_venue_to_database( venue, city_code )

    return cell.get_children()

if __name__ == "__main__":

    setproctitle('VEN_SRCH')
    dbw = DBWrapper( )

    # load credentials
    client_id = _credentials.vs_client_id
    client_secret = _credentials.vs_client_secret
    client_tuples = [(client_id, client_secret)]
    access_tokens = _credentials.vs_access_token

    gateway = APIGateway( access_tokens, 500, client_tuples, 5000 )
    api = APIWrapper( gateway )

    logging.info( u'VEN_SRCH start venue search crawl' )

    crawl_string = 'VENUE_SEARCH_CDF'
    dbw.add_crawl_to_database( crawl_string, 'START', now.now( ) )
    search_venues( 'CDF' )
    dbw.add_crawl_to_database( crawl_string, 'FINISH', now.now( ) )
    crawl_string = 'VENUE_SEARCH_BRS'
    dbw.add_crawl_to_database( crawl_string, 'START', now.now( ) )
    search_venues( 'BRS' )
    dbw.add_crawl_to_database( crawl_string, 'FINISH', now.now( ) )
    crawl_string = 'VENUE_SEARCH_CAM'
    dbw.add_crawl_to_database( crawl_string, 'START', now.now( ) )
    search_venues( 'CAM' )
    dbw.add_crawl_to_database( crawl_string, 'FINISH', now.now( ) )
