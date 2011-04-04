from database_wrapper import DBWrapper
from venues_api import *
from api import *
from shapely.geometry import Point, Polygon
import datetime

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

def print_checkin_stats():

	dbw = DBWrapper()
	checkins = dbw.get_all_checkins()

	delay = 60*2
	next_check_time = time.time()
	while True:
		while time.time() < next_check_time:
			sleep_dur = next_check_time - time.time()
			time.sleep( sleep_dur )

		print 'unique checkins captured: %d' % len(checkins)
		diff = datetime.timedelta(hours=1)
		checked = []
		count = 0
		for checkin1 in checkins:
			for checkin2 in checkins:
				if not checkin2 in checked or checkin1 in checked:
					if checkin1.venue_id == checkin2.venue_id:
						if not checkin1.user_id == checkin2.user_id:
							time1 = datetime.datetime.fromtimestamp(checkin1.created_at)
							time2 = datetime.datetime.fromtimestamp(checkin2.created_at)
							if time1 - time2 < diff:
								count = count + 1
		print 'matching checkins: %d' % count
		next_check_time = time.time() + delay


def point_inside_polygon(point,poly):
	return poly.contains(point)

def count_venues_in_polygon():
	dbw = DBWrapper()
	venues = dbw.get_all_venues()

	polygon=Polygon([(51.4648,-2.6107),(51.4707,-2.5924),(51.4738,-2.5619),(51.4683,-2.5463),(51.4602,-2.5365),(51.4496,-2.5350),(51.4319,-2.5457),(51.4304,-2.5887),(51.4334,-2.6089),(51.4471,-2.6194),(51.4560,-2.6195)])

	count = 0
	for venue in venues:
		location = venue.location
		point = Point(location.latitude, location.longitude)
		if point_inside_polygon(point,polygon):
			count = count + 1
		
	print count
				
def print_kml():

	f=open('locations.kml', 'w')
	g=open('locations_restricted.kml','w')

	dbw = DBWrapper()
	venues = dbw.get_all_venues()
 
	cardiff_polygon = Polygon([(51.4648,-2.6107),(51.4707,-2.5924),(51.4738,-2.5619),(51.4683,-2.5463),(51.4602,-2.5365),(51.4496,-2.5350),(51.4319,-2.5457),(51.4304,-2.5887),(51.4334,-2.6089),(51.4471,-2.6194),(51.4560,-2.6195)])


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
	count_venues_in_polygon()
	print_kml()