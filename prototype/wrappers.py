

import json
import urllib, urllib2

import credentials


class APIQuerier( object ):
    """
    A simple wrapper for issuing queries to the foursquare API.
    """
    
    api_base_url = "https://api.foursquare.com/v2"
    
    def __init__( self, oauth_access_token ):
        self.oath_token = oauth_access_token
        
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
        #
        # Build request...
        url = self.api_base_url
        
        url += "/%s" % resource_type
        url += "/%s" % id
        if aspect:
            url += "/%s" % aspect
        
        params = { 'oauth_token':self.oath_token }
        params.update( get_params )
        url += "?" + urllib.urlencode( params )
        
        #
        # Issue request...
        try:
            response = urllib2.urlopen( url )
        except URLError, e:
            print e.reason
            raise Exception()
        
        #
        # Handle response...
        content = response.read()
        decoded = json.loads( content )
        return decoded
        
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
        #
        # Build request...
        url = self.api_base_url
        
        url += "/%s" % resource_type
        url += "/%s" % routine
        
        params = { 'oauth_token':self.oath_token }
        params.update( get_params )
        url += "?" + urllib.urlencode( params )
        
        #
        # Issue request...
        try:
            response = urllib2.urlopen( url )
        except URLError, e:
            print e.reason
            raise Exception()
        
        #
        # Handle response...
        content = response.read()
        decoded = json.loads( content )
        return decoded


if __name__ == "__main__":
    import credentials
    api = APIQuerier( credentials.access_token2 )
    
    print "Grab info about a given venue..."
    reply = api.query_resource( "venues", "591313" )
    data = reply['response']
    print data
    print 
    
    print "Search for venues near a given location..."
    reply = api.query_routine( "venues", "search", {'ll':'51.4777,-3.1844'} )
    data = reply['response']
    venues = data['groups'][0]['items']
    for v in venues:
        print v['name']
    
    
    
    