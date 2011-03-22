from database_wrapper import DBWrapper
import database_wrapper_new
from venues_api import *
from api import *

def print_locations():
	dbw = DBWrapper()
	venues = dbw.get_all_venues()
	
	for venue in venues:
		statistics = venue.statistics
		location = venue.location
		users = 0
		for statistic in statistics:
			users = max(users, statistic.users)
		print '%s;%.6f;%.6f;%s' % (venue.name,location.latitude,location.longitude,users)

def print_kml():

	f=open('locations.kml', 'w')

	dbw = DBWrapper()
	venues = dbw.get_all_venues()

	f.write( '<?xml version="1.0" encoding="UTF-8"?>' )
	f.write( '<kml xmlns="http://www.opengis.net/kml/2.2">' )
	f.write( '<Folder>' )
	count = 0
	total = dbw.count_venues_in_database()/2
	for venue in venues:
		count = count + 1
		f.write( '<Placemark>' )
		f.write( '<description>"%s"</description>' % venue.name.replace('&','and').replace('<','').encode('utf-8') )
		f.write( '<Point>' )
		f.write( '<coordinates>%.8f,%.8f</coordinates>' % (venue.location.longitude, venue.location.latitude) )
		f.write( '</Point>' )
		f.write( '</Placemark>' )
	f.write( '</Folder>' )
	f.write( '</kml>' )
	print count

if __name__ == "__main__":
	print_kml()