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


from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlite3 import dbapi2 as sqlite
from datetime import datetime as now

from database import *

DATABASE = 'sqlite:///cs4sq.db'
MODULE=sqlite
DEBUG=False

class DBWrapper( object ):
	"""
	A simple wrapper providing some higher level methods to make adding to and querying the database easier.
	Database settings can be adjusted above, and *should* allow changes of database.

	Note: there are two methods included that aren't needed for day-to-day running of the code:
	__drop_tables__ and __create_tables__. Pretty self explanatory what they do. Shouldn't have to 
	say this, but for god's sake don't run __drop_tables__ EVER. It's just a convenience until the
	schema is correct. Then it'll be removed.
	"""
	def __init__( self ):
		Session = sessionmaker(bind=self._get_engine())
		self.session = Session()

	def get_session( self ):
		return self.session

	def add_category_to_database( self, cat ):
		"""
		Add a category to the database. Current schema does not relate categories to parents/children.
		"""
		c = self.get_category_from_database(cat)
		if c == None:
			c = Category(name=cat['name'], foursq_id=cat['id'])
			self.session.add(c)
			self.session.commit()
		return c

	def count_venues_in_database( self ):
		"""
		Count all the venues in the database
		"""
		return len(self.session.query(Venue).all())

	def get_category_from_database( self, category ):
		"""
		Input:		'category': dict containing category information. 'id' key used to match against foursq_id

		Retrieve the given category from the database.

		Output:		Category object
		"""
		return self.session.query(Category).filter(Category.foursq_id==category.get('id')).first()

	def add_location_to_database( self, loc ):
		"""
		Input: 		'loc': dict containing location with 'lat' and 'lng' keys.

		Checks to see if the location is already in the database, matching to both latitude and longitude.
		 If it isn't, the location is added to the database.
		
		Output: 	Location object
		"""
		l = self.session.query(Location).filter(Location.latitude==loc.get('lat')).filter(Location.longitude==loc.get('lng')).first()
		if l == None:
			print 'Location not found in database: %.5f, %.5f' % (loc.get('lat'), loc.get('lng'))
			l = Location(latitude=loc.get('lat'), longitude=loc.get('lng'))
			self.session.add(l)
			self.session.commit()
		else:
			print 'Location already in database: %.5f, %.5f' % (l.latitude, l.longitude)
		return l

	def add_checkin_to_database(self, checkin, venue):
		"""
		Input: 		'checkin': dict containing checkin information with 'id' and 'createdAt' keys and a dict with user information.
		Input: 		'venue': Venue object from database (e.g. returned from get_venue_by_name())

		Checks to see if the checkin already exists in the database, matching to foursquare provided 'id'. If it doesn't,
		the information is extracted and added to the database.
		Note: venue is supplied as a Venue object and is assumed to already exist in the database.

		Output: 	Checkin object
		"""
		c = self.get_checkin_from_database(checkin)
		if c == None:
			print 'Checkin not found in database'
			c = Checkin(foursq_id=checkin.get('id'), created_at=checkin.get('createdAt'))
			user = checkin.get('user')
			
			u = self.get_user_from_database(user)
			if u == None:
				print 'User not found in database: %s %s' % (user.get('firstName'), user.get('lastName'))
				u = self.add_user_to_database(user)
			else:
				print 'User found in database'
			c.user = u
			c.user_id = u.id
			c.venue = venue
			c.checkin_id = venue.id
		else:
			print 'Checkin found in database'
		self.session.add(c)
		self.session.commit()


	def add_venue_to_database( self, venue ):
		"""
		Input 		'venue': dict containing venue information with 'id', 'name' and 'verified' keys and dicts with location, 
					statistic and category information

		Checks if the venue already exists in the database, matching to foursquare 'id'. If it doesn't, information is extracted and
		the venue is added. Primary Category, Location and current Statistics for the venue are added at the same time.

		Output		Venue object
		"""
		v = self.get_venue_from_database(venue)
		if v == None:
			print 'Venue not in database: %s' % (venue.get('name'))

			loc = venue['location']
			l = self.add_location_to_database(loc)
			
			v = Venue(foursq_id=venue['id'], name=venue['name'], verified=venue['verified'], location_id=l.id)
			
			categories = venue['categories']
			for category in categories:
				if category.get('primary'):
					c = self.add_category_to_database(category)
					v.category_id = c.id
					v.category = c
			self.session.add(v)
			self.session.commit()

			stat = venue['stats']
			self.add_statistics_to_database(v, stat)
			self.session.commit()
		else:
			print 'Venue already in database: %s' % (v.name)
		return v

	def update_mayor( self, venue, mayor ):
		"""
		Input	'venue': dict containing venue information, foursquare 'id' is used to match venues
				'mayor': dict containing user information with 'id', 'firstName', 'lastName', 'gender' and 'homeCity' keys

		Retrieves a venue from the database and sets or updates the current mayor.

		Output  None
		"""
		v = self.get_venue_from_database(venue)
		if v == None:
			self.add_venue_to_database(venue)
		else:
			u = self.add_user_to_database(mayor)
			v.mayor_id = u.id
			v.mayor = u
			self.session.add(v)
			self.session.commit()

	def get_venue_from_database( self, venue ):
		"""
		Input	'venue': dict containing venue information, foursquare 'id' is used to match venues
		
		Retrieves the specified venue from the database.
		
		Output	Venue object. Will be 'None' if venue does not exist in the database. 
		"""
		return self.session.query(Venue).filter(Venue.foursq_id==venue['id']).first()

	def get_user_from_database( self, user):
		"""
		Input	'user': dict containing user information, foursquare 'id' is used to match users
		
		Retrieves the specified user from the database.
		
		Output	User object. Will be 'None' if user does not exist in the database. 
		"""
		return self.session.query(User).filter(User.foursq_id==user.get('id')).first()
	
	def get_checkin_from_database( self, checkin ):
		"""
		Input	'checkin': dict containing checkin information, foursquare 'id' is used to match venues
		
		Retrieves the specified checkin from the database.
		
		Output	Checkin object. Will be 'None' if checkin does not exist in the database. 
		"""
		return self.session.query(Checkin).filter(Checkin.foursq_id==checkin.get('id')).first()

	def get_venue_by_name( self, name ):
		"""
		Input	'name': string of name venue to match.
		
		Retrieves the specified venue from the database.
		
		Output	Venue object. Will be 'None' if venue does not exist in the database. 
		"""
		return self.session.query(Venue).filter(Venue.name==name).first()

	def add_user_to_database( self, user):
		"""
		Input	'user': dict containing user information with 'id', 'firstName', 'lastName', 'gender' and 'homeCity' keys

		Checks to see if the user is in the database, if not, adds the user.

		Output	User object
		"""
		u = self.get_user_from_database(user)
		if u == None:
			print 'User not found in database: %s %s' % (user.get('firstName'), user.get('lastName'))
			u = User(foursq_id=user.get('id'), first_name=user.get('firstName'), last_name=user.get('lastName'), gender=user.get('gender'), home_city=user.get('homeCity'))
			self.session.add(u)
			self.session.commit()
		else:
			print 'User found in database: %s %s' % (u.first_name, u.last_name)
		return u
	
	def add_search_to_database( self, date, latitude, longitude ):
		s = Search( latitude=latitude, longitude=longitude, date=date )
		self.session.add(s)
		self.session.commit()
		return s

	def add_statistics_to_database( self, venue, stats ):
		"""
		Input	'venue': Venue object, the venue to which the statistics relate.
				'stats': dict containing statistic information with 'checkinsCount' and 'usersCount' keys.

		Adds a new set of statistics to the database for a venue. Adds the current date and time to the statistics.

		Output	Statistics object
		"""
		s = Statistic(venue.id, now.now(), stats['checkinsCount'], stats['usersCount'])
		self.session.add(s)
		self.session.commit()
		return s

	def get_all_venues( self ):
		"""
		Retrieves all venues from the database.

		Output	list of Venue objects
		"""
		return self.session.query(Venue).all()

	def _get_engine( self ):
		return create_engine(DATABASE, module=MODULE, echo=DEBUG)

	def __create_tables__( self ):
		"""
		Sets up database tables.
		"""
		engine = self._get_engine()
		if not engine.has_table('venues'):
			engine.create(Venue.__table__)
		if not engine.has_table('categories'):
			engine.create(Category.__table__)
		if not engine.has_table('locations'):
			engine.create(Location.__table__)
		if not engine.has_table('statistics'):
			engine.create(Statistic.__table__)
		if not engine.has_table('users'):
			engine.create(User.__table__)
		if not engine.has_table('checkins'):
			engine.create(Checkin.__table__)
		if not engine.has_table('lookups'):
			engine.create(Lookup.__table__)
		if not engine.has_table('searches'):
			engine.create(Search.__table__)

	def __drop_tables__( self ):
		"""
		OH GOD NEVER CALL THIS.
		"""
		engine = self._get_engine()
		if engine.has_table('venues'):
			engine.drop(Venue.__table__)
		if engine.has_table('categories'):
			engine.drop(Category.__table__)
		if engine.has_table('locations'):
			engine.drop(Location.__table__)
		if engine.has_table('statistics'):
			engine.drop(Statistic.__table__)
		if engine.has_table('users'):
			engine.drop(User.__table__)
		if engine.has_table('checkins'):
			engine.drop(Checkin.__table__)
		if engine.has_table('lookups'):
			engine.drop(Lookup.__table__)
		if engine.has_table('searches'):
			engine.drop(Search.__table__)