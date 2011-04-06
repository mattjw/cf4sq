from database_wrapper import DBWrapper
from venues_api import *
from api import *
from shapely.geometry import Point, Polygon
import datetime
import logging
import sys

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
			
def print_kml(buffer_size):

	f=open('locations.kml', 'w')
	g=open('locations_restricted.kml','w')

	dbw = DBWrapper()
	venues = dbw.get_all_venues()

	centres = {'CDF' : Point(51.476251, -3.17509), 'BRS' : Point(51.450477, -2.59466), 'CAM' : Point(52.207870, 0.12712)}
	polygons = {}
	count  = {}
	count_restricted = {}
	for centre, point in centres.iteritems():
		polygons[centre] = point.buffer(buffer_size, resolution=20)
		count[centre] = 0
		count_restricted[centre] = 0

	f.write( '<?xml version="1.0" encoding="UTF-8"?>' )
	f.write( '<kml xmlns="http://www.opengis.net/kml/2.2">' )
	f.write( '<Folder>' )
	g.write( '<?xml version="1.0" encoding="UTF-8"?>' )
	g.write( '<kml xmlns="http://www.opengis.net/kml/2.2">' )
	g.write( '<Folder>' )

	total_count = 0
	
	for venue in venues:
		logging.info('writing venue %s' % venue.name.encode('utf-8'))
		f.write( '<Placemark>' )
		f.write( '<description>"%s"</description>' % venue.name.replace('&','and').replace('<','').encode('utf-8') )
		f.write( '<Point>' )
		f.write( '<coordinates>%.8f,%.8f</coordinates>' % (venue.location.longitude, venue.location.latitude) )
		f.write( '</Point>' )
		f.write( '</Placemark>' )
		location = venue.location
		city_code = venue.city_code
		polygon = polygons[city_code]
		point = Point(location.latitude, location.longitude)
		count[city_code] += 1
		if point_inside_polygon(point,polygon):
			count_restricted[city_code] += 1
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
	
	for centre in centres:
		print 'Total Venues: %d. Restricted Venues: %d' % (count[centre], count_restricted[centre])

if __name__ == "__main__":

    #
    # Input & args
    args = sys.argv
    
    if len(args) == 1:
        # No argument specified, use default distance
        buffer_size = 0.1
    else:
        buffer_size = float(args[1])
	print_kml(buffer_size)