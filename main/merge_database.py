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

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlite3 import dbapi2 as sqlite
from datetime import datetime as now
import database
import new_database

BRS_DB='sqlite:///bristol4sq.db'
CDF_DB='sqlite:///cardiff4sq.db'
CAM_DB='sqlite:///cambridge4sq.db'
JNT_DB='sqlite:///4sq.db'
MODULE=sqlite
DEBUG=False

jnt_engine = create_engine( JNT_DB, module=MODULE, echo=DEBUG )
jnt_session = sessionmaker(bind=jnt_engine)()

def get_category(category):
	return jnt_session.query( new_database.Category ).filter( new_database.Category.foursq_id==category.foursq_id ).first()

def get_location(location):
	return jnt_session.query( new_database.Location ).filter( new_database.Location.latitude==location.latitude and new_database.Location.longitude==location.longitude).first()

def get_venue(venue):
	return jnt_session.query( new_database.Venue ).filter( new_database.Venue.foursq_id==venue.foursq_id ).first()

def add_category(category):
	c = get_category(category)
	if c == None:
		c = new_database.Category(name=category.name, foursq_id=category.foursq_id)
		jnt_session.add(c)
		jnt_session.commit()
	return c

def add_location(location):
	l = get_location(location)
	if l == None:
		l = new_database.Location(location.latitude, location.longitude)
		jnt_session.add(l)
		jnt_session.commit()
	return l

def add_venue(venue, city_code):
	l = add_location(venue.location)
	v = get_venue(venue)
	if v == None:
		v = new_database.Venue( foursq_id=venue.foursq_id, name=venue.name, verified=venue.verified, location_id=l.id, city_code=city_code)
		if not venue.category == None:
			c = add_category(venue.category)
			v.category = c
		jnt_session.add(v)
		jnt_session.commit()
	return v

if __name__ == "__main__":

	cdf_engine = create_engine( CDF_DB, module=MODULE, echo=DEBUG )
	cam_engine = create_engine( CAM_DB, module=MODULE, echo=DEBUG )
	brs_engine = create_engine( BRS_DB, module=MODULE, echo=DEBUG )
	
	cdf_session = sessionmaker(bind=cdf_engine)()
	cam_session = sessionmaker(bind=cam_engine)()
	brs_session = sessionmaker(bind=brs_engine)()

	cdf_venues = cdf_session.query( database.Venue ).all( )
	cam_venues = cam_session.query( database.Venue ).all( )
	brs_venues = brs_session.query( database.Venue ).all( )

	print 'Cardiff Venues: %d' % len(cdf_venues)
	print 'Cambridge Venues: %d' % len(cam_venues)
	print 'Bristol Venues: %d' % len(brs_venues)

	cdf_locations = cdf_session.query( database.Location ).all( )
	cam_locations = cam_session.query( database.Location ).all( )
	brs_locations = brs_session.query( database.Location ).all( )

	print 'Cardiff Locations: %d' % len(cdf_locations)
	print 'Cambridge Locations: %d' % len(cam_locations)
	print 'Bristol Locations: %d' % len(brs_locations)

	cdf_categories = cdf_session.query( database.Category ).all( )
	cam_categories = cam_session.query( database.Category ).all( )
	brs_categories = brs_session.query( database.Category ).all( )

	print 'Cardiff Categories: %d' % len(cdf_categories)
	print 'Cambridge Categories: %d' % len(cam_categories)
	print 'Bristol Categories: %d' % len(brs_categories) 

	for venue in cdf_venues:
		add_venue(venue, 'CDF')
	for venue in cam_venues:
		add_venue(venue, 'CAM')
	for venue in brs_venues:
		add_venue(venue, 'BRS')

	jnt_venues = jnt_session.query( new_database.Venue ).all()
	jnt_locations = jnt_session.query( new_database.Location ).all()
	jnt_categories = jnt_session.query( new_database.Category ).all()
	print 'Joint Venues: %d' % len(jnt_venues)
	print 'Joint Locations: %d' % len(jnt_locations)
	print 'Joint Categories: %d' % len(jnt_categories)