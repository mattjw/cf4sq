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
import threading 
import logging


class APIGateway:
    """
    An object that interfaces with the foursquare API. All HTTP queries to the
    API should be carried out through a single gateway.
    
    Supports multiple access tokens. API calls are distributed evenly
    across all available access tokens. This is done simply cycling though
    the access tokens.
    
    The gateway discriminates between userlesss and authenticated access. 
    Userless access required a client ID and client secret to issue
    a query. Authenticated access requires an access token. 
    Userless and authenticated access use independent rates. The gateway
    handles these appropriately. 
    Userless access permits only a subset of the foursquare API functions. 
    Authenticated access allows access to all API functions. 
    
    Provides local query rate limiting. If specified, the gateway will delay
    issuing API queries to prevent exceeding the hourly request quota of a
    token.
    [Note: For the moment, query limiting is REQUIRED! ~MJW]
    The rate limiting may be improved with the knowledge that the foursquare
    rate limit is a limit per endpoint, rather than a limit per access token.
    For now, a limit per token is assumed.
    """
    
    def __init__( self, auth_access_tokens, auth_hourly_quota,
                        client_credentials, userless_hourly_quota ):
        """
        ## Authenticated Access Args ##
        `auth_access_tokens` may be a sequence of access tokens or a single
        access token (i.e., string).
        
        `auth_hourly_quota` is the maximum number of queries per hour
        for a single access token. Thus, the max number of authenticated 
        queries per hour is given by
            len( auth_access_tokens ) * auth_hourly_quota .
        
        ## Userless Access Args ##
        `client_credentials` a sequences of 2-tuples, or a single 2-tuple. 
        Each 2-tuple as (client_id, client_secret).
        
        `userless_hourly_quota` is the maximum number of USERLESS queries per
        hour for a single client. Thus, the max number of authenticated 
        queries per hour is given by
            len( client_credentials ) * userless_hourly_quota .
        """
        #
        # (Authenticated) access tokens...
        if not getattr( auth_access_tokens, '__iter__', False ):
            # If it doesn't have an iterator, then we assume it's a string, 
            # and therefore only one access token has been given.
            auth_access_tokens = [ auth_access_tokens ]
        
        self.auth_access_tokens = auth_access_tokens
        self.next_auth_access_token_index = 0
        
        #
        # Userless access...
        if len( client_credentials ) == 2:
            # The inputted `client_credentials` is a sequence of two:
            # it may be a 2-tuple for a single client, or it may be
            # a sequence of two clients.
            # We'll check the first element in this sequence to see if it's
            # a client ID or a 2-tuple.
            
            first_elm = client_credentials[0]
            if not getattr( first_elm, '__iter__', False ):
                # If it doesn't have an iterator, then we assume it's a string, 
                # and therefore the argument is a credentials tuple.
                client_credentials = [ client_credentials ]
        
        self.client_credentials = client_credentials
        self.next_client_index = 0
        
        #
        # URL...
        scheme = 'https://'
        netloc = 'api.foursquare.com'
        path_prefix = '/v2'
        self.api_base_url = scheme + netloc + path_prefix 
        
        #
        # Query limiting -- authenticated access...
        max_per_hour = auth_hourly_quota * len( auth_access_tokens )
        query_interval = ( 60 * 60 ) / float( max_per_hour )   # in seconds
            # The time to wait between issuing queries
        
        self.__auth_monitor = {'wait':query_interval,
                               'earliest':None,
                               'timer':None}
        
        #
        # Query limiting -- userless access...
        max_per_hour = userless_hourly_quota * len( client_credentials )
        query_interval = ( 60 * 60 ) / float( max_per_hour )   # in seconds
            # The time to wait between issuing queries
        
        self.__userless_monitor = {'wait':query_interval,
                                   'earliest':None,
                                   'timer':None}
    
    def __rate_controller( self, monitor_dict ):
        """
        Internal function to delay time as necessary. 
        This is general: depending on the `monitor_dict`, either the
        userless queries or the authenticated queries can be delayed.
        
        The methods functions as follows:
        1. join the timer that has been elapsing in the background.
        2. if the timer still hasn't finished yet, then we wait for it
          (causing the main thread to pause here too).
        3. update the monitor for the next delay. start the background timer.
        
        Fields of the monitor dictionary will be updated by this method.
        Dictionary fields understood as follows:
         * wait: the amount of time to wait between queries.
         * earliest: the earliest time that the next query should be issued.
         * timer: a backround thread that monitors the time for this particular
                  delay.
        """
        #
        # Cause main thread to wait until the desired time has elapsed.
        # If this is the first query for this monitor, then the timer is None
        # and we don't do any waiting.
        if monitor_dict['timer'] is not None:
            monitor_dict['timer'].join()   # causes main thread to sit and wait
                                           # for this monitor to elapse
            
            # Waste time in the (unlikely) case that the timer thread finished
            # early.
            while time.time() < monitor_dict['earliest']:
                time.sleep( monitor_dict['earliest'] - time.time() )
            
            
        #
        # Prepare for next call and start timer...
        earliest = time.time() + monitor_dict['wait']
        timer = threading.Timer( earliest-time.time(), lambda: None )
        monitor_dict['earliest'] = earliest
        monitor_dict['timer'] = timer
        monitor_dict['timer'].start()
        
    def query( self, path_suffix, get_params, userless=False ):
        """
        Issue a query to the foursquare web service.
        
        This method will handle inserting an access_token or client credentials;
        thus, a token should not be included in the inputted GET parameters. 
        Such a parameter will be overwritten.
        
        `get_params` is the GET parameters; a dictionary.
        
        `path_suffix` is appended to the API's base path. The left-most
        '/' is inserted if absent.s
        
        `userless` is a boolean specifying whether the access will be
        userless. If `userless` is False then authenticated access will be used.
        The method defaults to using authenticated access.
        
        If query is successful the method returns JSON data encoded as
        python objects via `json.loads()`.
        This method interprets any errors returned by the query and raises
        errors accordingly.
        Other than the conversion to python objects, the structure and values
        of the data are unaltered. All three foursquare top-level attributes
        are included; i.e., meta, notifications, response.
        """
        #
        # Cause rate delay...
        if userless:
            self.__rate_controller( self.__userless_monitor )
        else:
            self.__rate_controller( self.__auth_monitor )
        
        #
        # Params sanitising -- erase any tokens and client creds...
        params = copy.copy( get_params )
        for fld in ['oauth_token','client_id','client_secret']:
            if fld in params:
                del params[fld]
        
        #
        # Build & issue request...
        if userless:
            (client_id,client_secret) = self.client_credentials[self.next_client_index]
            params['client_id'] = client_id
            params['client_secret'] = client_secret
            self.next_client_index = \
                ( self.next_client_index + 1 ) % len( self.client_credentials )
        else:
            token = self.auth_access_tokens[self.next_auth_access_token_index]
            params['oauth_token'] = token
            self.next_auth_access_token_index = \
                ( self.next_auth_access_token_index + 1 ) % len( self.auth_access_tokens )
        
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
    
    def query_resource( self, resource_type, id, aspect=None, get_params={}, userless=False, tenacious=False ):
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
        `userless`
            Issue as a userless query rather than an authenticated query.
        `tenacious`:
            If True, will query in 'tenacious mode'. See method 
            `__query_tenaciously`.
        
        The JSON stream returned by the foursquare API is decoded into Python
        data structures (lists, dictionaries, etc.) and returned by this
        method.
        (More info at: http://docs.python.org/library/json.html#encoders-and-decoders)
        """
        path_suffix = "/%s" % resource_type
        path_suffix += "/%s" % id
        if aspect:
            path_suffix += "/%s" % aspect
        
        if not tenacious:
            return self.gateway.query( path_suffix, get_params, userless=userless )
        else:
            return self.__query_tenaciously( path_suffix, get_params, userless=userless )
        
    def query_routine( self, resource_type, routine, get_params={}, userless=False, tenacious=False ):
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
        `userless`:
            Issue as a userless query rather than an authenticated query.
        `tenacious`:
            If True, will query in 'tenacious mode'. See method 
            `__query_tenaciously`.
        
        The JSON stream returned by the foursquare API is decoded into Python
        data structures (lists, dictionaries, etc.) and returned by this
        method.
        (More info at: http://docs.python.org/library/json.html#encoders-and-decoders)
        """
        path_suffix = "/%s" % resource_type
        path_suffix += "/%s" % routine
        
        if not tenacious:
            return self.gateway.query( path_suffix, get_params, userless=userless )
        else:
            return self.__query_tenaciously( path_suffix, get_params, userless=userless )
    
    def __query_tenaciously( self, path_suffix, get_params, userless=False ):
        """
        Intermediary helper method to handle tenaciously issuing of queries.
        
        If errors occur while querying, the method will keep retrying until it 
        succeeds.
        Retries will be preceeded by an incremental backoff period. 
        The following HTTP errors will trigger a retry:
            500: ('Internal Server Error', 'Server got itself in trouble')
            501: ('Not Implemented',
                  'Server does not support this operation')
            502: ('Bad Gateway', 'Invalid responses from another server/proxy.')
            503: ('Service Unavailable',
                  'The server cannot process the request due to a high load')
            504: ('Gateway Timeout',
                  'The gateway server did not receive a timely response')
        Other HTTP errors will *not* trigger a retry; any other
        general URL errors *will* trigger a retry.
        All errors that do not trigger a retry are reraised.
        """
        backoff = 6.0  # seconds
        max_backoff = 5.0*60 
        
        while True:
            try:
                result = self.gateway.query( path_suffix, get_params, userless=userless )
                return result
            except urllib2.URLError, e:
                do_retry = False
                if hasattr( e, 'reason' ):
                    # This corresponds to a non-HTTP error. These are
                    # typically communication issues (e.g., no route to
                    # host, IP could not be resolved, server not 
                    # responding, etc.).
                    do_retry = True
                if hasattr( e, 'code' ) and e.code in [500,501,502,503,504]:
                    # This corresponds to a HTTP error whose error code
                    # is in a select subset of errors.
                    do_retry = True 
                
                if do_retry:
                    logging.debug('query error due to "%s", sleeping for %d seconds' % (e, backoff))
                    time.sleep( backoff )
                    backoff *= 2
                    backoff = min( [backoff,max_backoff] )
                else:
			        raise e
    
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
    city_code = 'CDF'
    
    client_id = _credentials.client_id[city_code]
    client_secret = _credentials.client_secret[city_code]
    client_tuples = [(client_id, client_secret)]
    
    access_tokens = _credentials.access_tokens[city_code]
    
    gateway = APIGateway( access_tokens, 163, client_tuples, 600 )
    api = APIWrapper( gateway )
    
    # acces_tokens: 163 per hour = 1 every 22s
    # client_tups: 600 per hour = 1 every 6s
    
    logging.basicConfig( filename="_testing.log", level=logging.DEBUG, 
        datefmt='%d/%m/%y|%H:%M:%S', format='|%(asctime)s|%(levelname)s| %(message)s'  )
    
    print 
    print '## Test tenacious querying'
    
    try:
        data = api.query_resource( "VLARGENvenues", "591313", userless=True, tenacious=True )
        print "\n",data
    except Exception,e:
        print e

    data = api.query_resource( "venues", "591313", userless=True, tenacious=True )
    print "\n",data
    
    data = api.query_routine( "venues", "search", {'ll': '44.3,37.2', 'intent': 'checkin', 'limit': '5'}, tenacious=True )
    print "\n",data
    
    data = api.query_resource( "venues", "591313", userless=True, tenacious=True )
    print "\n",data
    
    print "\n[[ done ]]"
    exit()
    
    print 
    print "## Compare userless vs. authed results"
    print "checksum userless:"
    print len( repr( api.query_resource( "venues", "591313", userless=True ) ) )
    print "checksum authed:"
    print len( repr( api.query_resource( "venues", "591313", userless=False ) ) )
    print "checksum userless:"
    print len( repr( api.query_resource( "venues", "591313", userless=True ) ) )
    print "checksum authed:"
    print len( repr( api.query_resource( "venues", "591313", userless=False ) ) )
    
    
    print 
    print "## Testing timing..."
    
    # First two are never delayed, so don't test their calls...
    api.query_resource( "venues", "591313", userless=False )    
    api.query_resource( "venues", "591313", userless=True )    
    
    print "\n\n<<authed [%s]>>" % [time.gmtime()[4:6]]
    rt = api.query_resource( "venues", "591313", userless=False )    
    #print rt
    
    print "\n\n<<authed [%s]>>" % [time.gmtime()[4:6]]
    rt = api.query_resource( "venues", "591313", userless=False )    
    #print rt
    
    print "\n\n<<userless [%s]>>" % [time.gmtime()[4:6]]
    rt = api.query_resource( "venues", "591313", userless=True )
    #print rt

    print "\n\n<<userless [%s]>>" % [time.gmtime()[4:6]]
    rt = api.query_resource( "venues", "591313", userless=True )
    #print rt

    print "\n\n<<userless [%s]>>" % [time.gmtime()[4:6]]
    rt = api.query_resource( "venues", "591313", userless=True )
    #print rt
    
    print "\n\n<<userless [%s]>>" % [time.gmtime()[4:6]]
    rt = api.query_resource( "venues", "591313", userless=True )
    #print rt
    
    print "\n\n<<userless [%s]>>" % [time.gmtime()[4:6]]
    rt = api.query_resource( "venues", "591313", userless=True )
    #print rt
    
    print "\n\n<<authed [%s]>>" % [time.gmtime()[4:6]]
    rt = api.query_resource( "venues", "591313", userless=False )    
    #print rt

    print "\n\n<<fin... [%s]>>" % [time.gmtime()[4:6]]
    
    
    print "Grab friends from a non-existent user..."
    try:
        friends = api.get_friends_of( 23432190333 )
        print len( friends )
    except Exception, e:
        print e
    
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
    
