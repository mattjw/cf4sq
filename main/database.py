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


"""
This is a basic database schema for the cs4sq project. Not very sophisticated, but it'll do.
"""
from sqlalchemy import Table, Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import BIGINT
from sqlalchemy.orm import relationship, backref

Base = declarative_base()

class Category( Base ):
    __tablename__ = 'categories'

    id = Column( Integer, primary_key=True )
    foursq_id = Column( String )
    name = Column( String )

    def __init__( self, name, foursq_id ):
        self.name = name
        self.foursq_id = foursq_id

class Location( Base ):
    __tablename__ = 'locations'

    id = Column( Integer, primary_key=True )
    latitude = Column( Float )
    longitude = Column( Float )

    def __init__( self, latitude, longitude ):
        self.latitude = latitude
        self.longitude = longitude

    def __repr__( self ):
        return "<Location('%f', '%f')>" % (self.latitude, self.longitude)

class Statistic( Base ):
    __tablename__ = 'statistics'

    id = Column( Integer, primary_key=True ) 
    venue_id = Column( Integer, ForeignKey( 'venues.id' ) )
    venue = relationship("Venue", backref=backref('statistics', order_by='Statistic.date'), cascade="all, save-update")
    date = Column( DateTime )
    checkins = Column( Integer )
    users = Column( Integer )

    def __init__( self, venue_id, date, checkins, users ):
        self.date = date
        self.venue_id = venue_id
        self.checkins = checkins
        self.users = users

    def __repr__( self ):
        return "<Statistics('%s', '%d', '%d')>" % (self.date, self.checkins, self.users)

class Venue( Base ):
    __tablename__ = 'venues'

    id = Column( Integer, primary_key=True )
    foursq_id = Column( String )
    name = Column( String )
    verified = Column( Boolean )
    city_code = Column ( String )
    location_id = Column( Integer, ForeignKey( 'locations.id' ) )
    location = relationship("Location", backref=backref('venues'), cascade="all, save-update")
    category_id = Column( Integer, ForeignKey('categories.id') )
    category = relationship("Category", backref=backref('venues'), cascade="all, save-update")
    mayor_id = Column( Integer, ForeignKey( 'users.id' ) )
    mayor = relationship("User", backref=backref('mayorships'), cascade="all, save-update")

    def __init__( self, foursq_id, name, verified, location_id, city_code ):
        self.foursq_id = foursq_id
        self.name = name
        self.verified = verified
        self.location_id = location_id
        self.city_code = city_code

    def __repr__( self ):
        return "<Venue('%s', '%s', '%s', '%s', '%s')>" % (self.name, self.foursq_id, self.location, self.statistics, self.checkins)

    def get_latest_statistic():
        return statistics[0]
    
class User( Base ):
    __tablename__ = 'users'

    id = Column( Integer, primary_key=True )
    foursq_id = Column( String )
    first_name = Column( String )
    last_name = Column( String )
    gender = Column( String )
    home_city = Column( String )
    num_mayorships = Column( Integer )

    def __init__( self, foursq_id, first_name, last_name, gender, home_city):
        self.foursq_id = foursq_id
        self.first_name = first_name
        self.last_name = last_name
        self.gender = gender
        self.home_city = home_city

    def __repr__( self ):
        return "<User('%s', '%s', '%s', '%s', '%s')" % (self.first_name, self.last_name, self.foursq_id, self.mayorships, self.checkins)

class Checkin( Base ):
    __tablename__ = 'checkins'

    id = Column( Integer, primary_key=True )
    foursq_id = Column( String )
    user_id = Column( Integer, ForeignKey( 'users.id') )
    user = relationship("User", backref=backref('checkins'), cascade="all, save-update")
    venue_id = Column( Integer, ForeignKey( 'venues.id' ) )
    venue = relationship("Venue", backref=backref('checkins'), cascade="all, save-update")
    created_at = Column( BIGINT )

    def __init__( self, foursq_id, created_at ):
        self.created_at = created_at
        self.foursq_id = foursq_id

    def __repr__( self ):
        return "<Checkin('%d')>" % (self.created_at)

class Friendship( Base ):
    """
    Although 4sq friendships are symmetric, we choose to store both 
    directions of the friendship as separate rows. Thus, the table will 
    have two row pers 4sq friendship. This simplifies querying.
    
    Friendship creation dates are not included in the 4sq API.
    
    A discovered friendship is stored along with the date of the run in which
    the friendship was found. 
    
    The date `date_crawled` specifies when the friendship was discovered. 
    
    The `crawl_id` is an optional value to be used where the friendships
    are being crawled in a one-off run. The same id is used for all friendships
    collected in the same run. This allows a the snapshot captured by a 
    particular run to be resconstructed easily.
    `crawl_id` is an integer. A new run identifier can be generated
    by choosing max(crawl_id)+1.
    """
    
    __tablename__ = 'friendships'
    
    id = Column( Integer, primary_key=True )
    
    userA_id = Column( Integer, ForeignKey( 'users.id') )
    #userA = relationship("User", backref=backref('friendships'), cascade="all, save-update")
    
    userB_id = Column( Integer, ForeignKey( 'users.id') )
    #userB = relationship("User", backref=backref('friendships'), cascade="all, save-update")
    
    date_crawled = Column( DateTime )
    crawl_id = Column( Integer, nullable=True )
    
    def __init__( self, userA, userB, date_crawled, crawl_id=None ):
        """
        `userA`,`userB`: expected to be User objects.
        """
        #self.userA = userA        
        self.userA_id = userA.id

        #self.userB = userB
        self.userB_id = userB.id
        
        self.date_crawled = date_crawled
        self.crawl_id = crawl_id

    def __repr__( self ):
        return "<Friendship('%d, '%d', '%s','%s')>" % ( self.userA_id, self.userB_id, self.date_crawled, self.crawl_id )
        
    