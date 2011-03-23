from database_wrapper import DBWrapper
from venues_api import *
from api import *
from shapely.geometry import Point, Polygon

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

def point_inside_polygon(point,poly):
	return poly.contains(point)

def print_kml():

	f=open('locations.kml', 'w')
	g=open('locations_restricted.kml','w')

	dbw = DBWrapper()
	venues = dbw.get_all_venues()

	cardiff_polygon = Polygon([(51.4846,-3.2314),(51.4970,-3.2162),(51.5043,-3.1970),(51.5010,-3.1575),(51.4831,-3.1411),(51.4660,-3.1356),(51.4514,-3.1562),(51.4260,-3.1692),(51.4320,-3.1878)])


	f.write( '<?xml version="1.0" encoding="UTF-8"?>' )
	f.write( '<kml xmlns="http://www.opengis.net/kml/2.2">' )
	f.write( '<Folder>' )
	g.write( '<?xml version="1.0" encoding="UTF-8"?>' )
	g.write( '<kml xmlns="http://www.opengis.net/kml/2.2">' )
	g.write( '<Folder>' )
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
		location = venue.location
		point = Point(location.latitude, location.longitude)
		if point_inside_polygon(point,cardiff_polygon):
			g.write( '<Placemark>' )
			g.write( '<description>"%s"</description>' % venue.name.replace('&','and').replace('<','').encode('utf-8') )
			g.write( '<Point>' )
			g.write( '<coordinates>%.8f,%.8f</coordinates>' % (venue.location.longitude, venue.location.latitude) )
			g.write( '</Point>' )
			g.write( '</Placemark>' )			

	f.write( '</Folder>' )
	f.write( '</kml>' )
	g.write( '</Folder>' )
	g.write( '</kml>' )
	print count

if __name__ == "__main__":
	print_kml()