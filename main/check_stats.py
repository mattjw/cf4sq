import logging
import _credentials
from database_wrapper import DBWrapper
from venues_api import VenueAPIGateway
from urllib2 import HTTPError, URLError
from api import *
from exceptions import Exception

def get_venue_details( id ):
	while True:
		response = ''
		try :
			response = venue_api.query_resource( "venues", id )
			return response, True
		except HTTPError, e:
			logging.debug('HTTPError code %d for venue %s' % (e.code, id))
			if e.code == 400:
				return response, False
			logging.debug(e)
		except Exception, e:
			logging.debug('General Error, retrying')


if __name__ == "__main__":
	#
	# Logging
	logging.basicConfig( filename="4sq.log", level=logging.DEBUG, 
		datefmt='%d/%m/%y|%H:%M:%S', format='|%(asctime)s|%(levelname)s| %(message)s'  )
	logging.info( 'checkin monitor initiated' )

	dbw = DBWrapper()
	# load credentials
	client_id = _credentials.sc_client_id
	client_secret = _credentials.sc_client_secret
	venue_gateway = VenueAPIGateway( client_id=client_id, client_secret=client_secret, token_hourly_query_quota=4500 )

	venues = dbw.get_all_venues()
	
	venue_api = APIWrapper( venue_gateway )

	count_venues = 0
	for venue in venues:
		logging.info( 'retrieve details for venue: %s' % ( venue.name.encode('utf-8') ) )
		response, success = get_venue_details( venue.foursq_id )
		if success:
			count_venues = count_venues + 1
			v = response.get( 'response' )
			v = v.get( 'venue' )
			stats = v.get( 'stats' )
			dbw.add_statistics_to_database(venue,stats)	
	logging.info( 'venues checked: %d' % ( count_venues ) )