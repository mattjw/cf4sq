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


class APIGateway:
    """
    An object that interfaces with the foursquare API. All HTTP queries to the
    API should be carried out through a single gateway.
    
    Supports multiple access tokens. API calls are distributed evenly
    across all available access tokens. This is done simply cycling though
    the access tokens.
    
    Provides local query rate limiting. If specified, the gateway will delay
    issuing API queries to prevent exceeding the hourly request quota of a
    token.
    
    The rate limiting may be improved with the knowledge that the foursquare
    rate limit is a limit per endpoint, rather than a limit per access token.
    For now, a limit per token is assumed.
    """
    
    def __init__( self, access_tokens, token_hourly_query_quota=None ):
        """
        `access_tokens` may be a sequence of access tokens or a single
        access token (i.e., string).
        
        `token_hourly_query_quota` is the maximum number of queries per hour
        for a single access token. Thus, the max number of queries per hour
        is given by
            len( access_tokens ) * token_hourly_query_quota .
        If `None` then the gateway will not enforce any limiting.
        """
        #
        # Access tokens...
        if not getattr( access_tokens, '__iter__', False ):
            # If it's doesn't have an iterator, then we assume it's a string, 
            # and therefore only one access token has been given.
            access_tokens = [ access_tokens ]
        
        self.access_tokens = access_tokens
        self.next_access_token_index = 0
        
        #
        # Query delaying...
        if token_hourly_query_quota is not None:
            max_per_hour = token_hourly_query_quota * len( access_tokens )
            query_interval = ( 60 * 60 ) / float( max_per_hour )   # in seconds
            
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
        
        This method will handle inserting an access_token; thus, a token should
        not be included in the inputted GET parameters. Such a parameter will
        be overwritten.
        
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
        token = self.access_tokens[self.next_access_token_index]
        params['oauth_token'] = token
        
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
        
        #
        # Prep for next call...
        self.next_access_token_index = \
            ( self.next_access_token_index + 1 ) % len( self.access_tokens )
        
        if self.query_interval is not None:
            self.earliest_query_time = time.time() + self.query_interval
        
        #
        # Fin
        return py_data 

class FoursquareRequestError( RuntimeError ):
    def __init__( self, response_code, error_type, error_detail ):
        self.response_code = response_code
        self.error_type = error_type
        self.error_detail = error_detail 
        
    def __str__( self ):
        return "%s:%s" % ( self.response_code, self.error_type )


class RateLimitExceededError( FoursquareRequestError ):
    pass


class APIWrapper( object ):
    """
    A simple wrapper providing some higher level API functionality.
    """
    
    def __init__( self, gateway ):
        """
        `gateway` should be an `APIGateway` object, through which all
        API queries will be issued.
        """
        self.gateway = gateway
        
    def query_resource( self, resource_type, id, aspect=None, get_params={} ):
        """
        Issue a query regarding a resource with a specific ID.
        
        Not all queries require a resource ID. For issuing queries that do not
        take an ID see `query_method`.
        
        See http://developer.foursquare.com/docs/index_docs.html for more
        information.
        
        This method does not handle queries involving POST parameters.
        
        Parmameters as follows...
        `resource_type`
            The type of the resource. E.g., users, venues,
            checkins, etc.
        `id`
            The identifier for an instance of the resource.
        `aspect`
            Aspects are data items connected to an instance of a resource. If
            an aspect is not specified then the resource itself is returned.
        `get_params`
            The GET parameters for the query. A dictionary.
        
        The JSON stream returned by the foursquare API is decoded into Python
        data structures (lists, dictionaries, etc.) and returned by this
        method.
        (More info at: http://docs.python.org/library/json.html#encoders-and-decoders)
        """
        path_suffix = "/%s" % resource_type
        path_suffix += "/%s" % id
        if aspect:
            path_suffix += "/%s" % aspect
        
        return self.gateway.query( path_suffix, get_params )
        
    def query_routine( self, resource_type, routine, get_params={} ):
        """
        Some resources also offer 'routine', which do not require any ID.
        This helps with issuing routine queries to the API.
        (N.B. 'routine' is my own term. It is not the same as foursquare
        actions.)
        
        See http://developer.foursquare.com/docs/index_docs.html for more
        information.
        
        This method does not handle queries involving POST parameters.
        
        `resource_type`:
            The type of the resource. E.g., users, venues,
            checkins, etc.
        `routine`:
            The routine associated with the resource type.
        `get_params`:
            The GET parameters for the query. A dictionary.
        
        The JSON stream returned by the foursquare API is decoded into Python
        data structures (lists, dictionaries, etc.) and returned by this
        method.
        (More info at: http://docs.python.org/library/json.html#encoders-and-decoders)
        """
        path_suffix = "/%s" % resource_type
        path_suffix += "/%s" % routine
        
        return self.gateway.query( path_suffix, get_params )
        
    def find_venues_near( self, lat, long, limit=50 ):
        """
        Call to the venue search method to find venues near a given latitude
        and longitude.
        
        `lat` and `long` can be numbers or strings containing numbers.
        
        The venue search method also returns 'trending' places. These are
        discarded.
        
        Returns a ~?~.
        """
        #
        # Issue query... 
        ll_str = "%f,%f" % ( float(lat), float(long) )
        get_qry = { 'll': ll_str, 'intent': 'checkin', 'limit': limit }
        data = self.query_routine( "venues", "search", get_qry )
        
        #
        # Parse hierarchy to grab list of venues...
        response = data['response']  # a dict
        groups = response['groups']  # a list
        
        trending = None
        nearby = None
        for group in groups:
            if group['type'] == 'trending':
                trending = group  # a three-field dict specifying a collection
            if group['type'] == 'nearby':
                nearby = group  # a three-field dict specifying a collection
        
        nearby = nearby['items']  # a list of nearby venues
        venues = nearby 
        return venues
    
    def get_friends_of( self, user_id ):
        """
        Get the friends of a particular user.
        
        This is coded to return *all* of the friends of the user. This may
        require multiple access requests.
        
        Returns a list of users. Each user is a dictionary containing
        a terse subset of user attributes.
        """
        offset = 0
        limit = 500
        
        data = self.query_resource( 'users', user_id, 'friends', 
                   {'limit':limit, 'offset':offset} )
        target_num_friends = long( data['response']['friends']['count'] )

        friends_list = data['response']['friends']['items']
        while len(friends_list) < target_num_friends:
            offset += limit
            data = self.query_resource( 'users', user_id, 'friends', 
                       {'limit':limit, 'offset':offset} )
            friends_list += data['response']['friends']['items']
            
        assert len( friends_list ) == target_num_friends
        return friends_list
    
    def get_user_by_id( self, user_id ):
        data = self.query_resource( 'users', user_id )
        response = data['response']  # a dict
        user = response['user'] # a dict
        return user

if __name__ == "__main__":
    import _credentials
    tokens = _credentials.access_tokens
    gateway = APIGateway( tokens )
    api = APIWrapper( gateway )
    
    print "Grab all the friends of a very popular user..."
    friends = api.get_friends_of( 1235468 )
    print len( friends )
    print len( frozenset( [ elm['id'] for elm in friends] ) )
    
    print "Grab all the friends of a less popular user..."
    friends = api.get_friends_of( 5082497 )
    print len( friends )
    print len( frozenset( [ elm['id'] for elm in friends] ) )
    exit()
    
    print "Grab info about a given venue..."
    reply = api.query_resource( "venues", "591313" )
    data = reply['response']
    print data
    print 
    
    print "Grab a list of friends of a given user..."
    friends = api.get_friends_of( 5082497 )
    for f in friends:
        print "\t", f
    print
    
    print "Search for venues near a given location..."
    venues = api.find_venues_near( 51.4777, -3.1844 )
    for v in venues:
        print v['name']
    
    print "Try out some API limiting..."
    gateway = APIGateway( tokens, 60 )   
    gateway = APIGateway( tokens, 28800 )
    gateway = APIGateway( tokens[0], 500 )
    # 60 reqs per hour per token for two tokens => 1 req per 30 secs   
    # 28,800 reqs per hour per token for two tokens => 1 req per 0.25 secs
    # 500 reqs per hour per token for one token => 1 req per 7.2 secs
    
    api = APIWrapper( gateway )

    print "Start time: %s" % time.time()
    print "(About to issue 5 queries...)"
    for _ in range(5):
        api.query_resource( "venues", "591313" )
    print "Finish time: %s" % time.time()
