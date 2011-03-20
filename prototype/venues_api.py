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


import time
import json
import urllib
import urllib2
import copy
from api import FoursquareRequestError, RateLimitExceededError


class VenueAPIGateway:
    """
    An object that interfaces with the foursquare API. All HTTP queries to the
    API should be carried out through a single gateway.
    
    Deals with unauthenticated queries to the VenueAPI, when only client_id and
    client_secret need to be provided rather than an access_token
    
    Provides local query rate limiting. If specified, the gateway will delay
    issuing API queries to prevent exceeding the hourly request quota.
    """
    
    def __init__( self, client_id, client_secret, token_hourly_query_quota=5000 ):
        """
        `token_hourly_query_quota` is the maximum number of queries per hour.
        """
        
        self.client_id = client_id
        self.client_secret = client_secret
        
        #
        # Query delaying...
        if token_hourly_query_quota is not None:
            query_interval = ( 60 * 60 ) / float( token_hourly_query_quota )   # in seconds
            
            self.earliest_query_time = time.time()   # as secs since unix epoch
            self.query_interval = query_interval
                # The time to wait between issuing queries
        else:
            self.query_interval = None
            self.earliest_query_time = None 
        
        #
        # URL...
        scheme = 'https://'
        netloc = 'api.foursquare.com'
        path_prefix = '/v2'
        self.api_base_url = scheme + netloc + path_prefix 
        
    def query( self, path_suffix, get_params ):
        """
        Issue a query to the foursquare web service.
        
        This method will handle inserting client_id and client_secret.
        
        `get_params` is the GET parameters; a dictionary.
        
        `path_suffix` is appended to the API's base path. The left-most
        '/' is inserted if absent.s
        
        If query is successful the method returns JSON data encoded as
        python objects via `json.loads()`.
        This method interprets any errors returned by the query and raises
        errors accordingly.
        Other than the conversion to python objects, the structure and values
        of the data are unaltered. All three foursquare top-level attributes
        are included; i.e., meta, notifications, response.
        """
        #
        # Do sleep for delay query if necessary...
        if self.query_interval is not None:
            while time.time() < self.earliest_query_time:
                sleep_dur = self.earliest_query_time - time.time()
                time.sleep( sleep_dur )
                #~ Potential for scheduler thrashing if time difference
                #  is tiny? Near-zero millis rounded down => repeated looping?
        
        #
        # Build & issue request...
        params = copy.copy( get_params )
        params['client_id'] = self.client_id
        params['client_secret'] = self.client_secret
        
        path_suffix = path_suffix.lstrip( '/' )
        
        url = self.api_base_url + '/' + path_suffix + "?" + urllib.urlencode( params )
        
        try:
            response = urllib2.urlopen( url )
        except urllib2.HTTPError, e:
            raise e
        except urllib2.URLError, e:
            raise e
        
        raw_data = response.read()
        py_data = json.loads( raw_data )
        
        # Request error handling...
        response_code = int( py_data['meta']['code'] )
        if response_code != 200:
            error_type = py_data['meta']['errorType'][0]
            error_detail = py_data['meta']['errorDetail'][0]
            if error_type == 'rate_limit_exceeded':
                raise RateLimitExceededError( response_code, error_type, 
                    error_detail )
            
            raise FoursquareRequestError( response_code, error_type, 
                error_detail )
        
        if self.query_interval is not None:
            self.earliest_query_time = time.time() + self.query_interval
        
        #
        # Fin
        return py_data 

if __name__ == "__main__":
    import _credentials
    from api import APIWrapper
    client_id = _credentials.client_id
    client_secret = _credentials.client_secret
    gateway = VenueAPIGateway( client_id=client_id, client_secret=client_secret )
    api = APIWrapper( gateway )
    
    print "Grab info about a given venue..."
    reply = api.query_resource( "venues", "591313" )
    data = reply['response']
    print data
    print 
    
    print "Search for venues near a given location..."
    venues = api.find_venues_near( 51.4777, -3.1844 )
    for v in venues:
        print v['name']
