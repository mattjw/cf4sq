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


"""
This will mine all friendships from those users in the database that have
checked in at least once.

Some friends may have not been seen by the checking monitor before. These
are added to the users table.
"""


from database_wrapper import DBWrapper
from urllib2 import HTTPError
from api import *


if __name__ == "__main__":
    
    #
    # Log
    import logging
    logging.basicConfig( filename="crawl_friends.log", level=logging.DEBUG, 
        datefmt='%d/%m/%y|%H:%M:%S', format='|%(asctime)s|%(levelname)s| %(message)s'  )
    logging.info( 'initiating a friend crawl.' )

    #
    # Prep
    import _credentials
    access_tokens = _credentials.access_tokens
    
    gateway = APIGateway( access_tokens=access_tokens, token_hourly_query_quota=500 )
    api = APIWrapper( gateway )
    dbw = DBWrapper()
    
    if False:
        #~ For debugging
        from database import Friendship
        dbw._get_engine().drop(Friendship.__table__)
        dbw._get_engine().create(Friendship.__table__)
    
    #
    # Begin mining...
    max_crawl_id = dbw.get_friendships_max_crawl_id()
    
    if max_crawl_id is None:
        logging.debug( 'no previous crawls found.' )
        crawl_id = 1
    else:
        crawl_id = max_crawl_id + 1
    
    logging.info( 'crawl id = %s', crawl_id )
    count_users = 0
    sum_degree = 0
    friend_rows_added = 0
    
    all_users = dbw.get_all_users_with_checkins() 
    for indx, user_obj in enumerate( all_users ):
        user_4sq_id = user_obj.foursq_id
        friends = api.get_friends_of( user_4sq_id )
        
        logging.info( 'crawling user %s (%s of %s). found %s friends.', user_4sq_id, indx+1, len(all_users), len(friends) )
        count_users += 1
        sum_degree += len( friends )
        
        for friend_dict in friends:
            friend_4sq_id = friend_dict['id']
            
            # Check that the friend user is a 'user' and not a brand (etc.)
            friend_api_dict = api.get_user_by_id( friend_4sq_id )
            if friend_api_dict['type'].lower() != 'user':
                logging.info( "friend user %s was not of type 'user'. skipping.", friend_4sq_id )
            
            # Add the friend user if necessary
            friend_obj = dbw.get_user_from_database( friend_dict )
            if friend_obj is None:
                friend_obj = dbw.add_user_to_database( friend_dict )
                logging.debug( 'added new user to database: %s', str(friend_obj) )
            
            # Add friendship in each direction (iff not already added in this run)
            friendship = dbw.get_friendship_from_database( user_obj, friend_obj, crawl_id )
            if len(friendship) == 0:
                fship_obj = dbw.add_friendship_to_database( user_obj, friend_obj, crawl_id )
                logging.debug( 'added new row to friendships: %s', str(fship_obj) )
                friend_rows_added += 1
                
            friendship = dbw.get_friendship_from_database( friend_obj, user_obj, crawl_id )
            if len(friendship) == 0:
                fship_obj = dbw.add_friendship_to_database( friend_obj, user_obj, crawl_id )
                logging.debug( 'added new row to friendships: %s', str(fship_obj) )
                friend_rows_added += 1
        
    logging.info( 'finishing run' )
    logging.info( 'crawl id: %s', crawl_id )
    logging.info( 'users checked: %s', count_users )
    logging.info( 'sum of node degrees: %s', sum_degree )
    logging.info( 'friendhip rows added: %s', friend_rows_added )
    logging.info( 'run finished' )
    
    f.write( 'venues checked: %d\n' % ( count_venues ) )
    f.write( 'venues with checkins: %d\n' % ( count_venues_with_checkins ) )
    f.write( 'checkins: %d\n' % ( count_checkins ) )
    f.write( 'total checkins in database: %d\n' % dbw.count_checkins_in_database() )
    f.flush()

    

