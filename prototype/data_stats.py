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
Script to grab some statistics about the data collected so far.
"""

from sqlalchemy import create_engine
from datetime import datetime, timedelta
import sys
import os.path

def num_checkins_since( conn, since_date ):
    """
    `since_date` is a Python datetime object.
    """
    # SQLite datetime format:           2011-03-23 14:02:15
    # Python datetime.isoformat(' '):   2011-03-23 14:02:15
    date_str = since_date.isoformat( ' ' )
    qry_str = """select count(*) 
                 from checkins
                 where datetime(created_at,'unixepoch') >= '%s' """ % (date_str)
    result_set = connection.execute(qry_str)
    return int( result_set.first()[0] )


def num_visited_venues_since( conn, since_date ):
    """
    `since_date` is a Python datetime object.
    """
    date_str = since_date.isoformat( ' ' )
    qry_str = """select count( distinct(venue_id) ) 
                 from checkins
                 where datetime(created_at,'unixepoch') >= '%s' """ % (date_str)
    result_set = connection.execute(qry_str)
    return int( result_set.first()[0] )


if __name__ == "__main__":
    #
    # Input & args
    args = sys.argv

    if len(args) not in [2,3]:
        print "Incorrect number of arguments"
        print "Argument pattern: dbfile [hours]"
        exit(1)
    
    db_filename = sys.argv[1]
    if not os.path.isfile( db_filename ):
        print "Invalid or nonexistent file: %s" % db_filename
        exit(1)

    db_URL = 'sqlite:///' + sys.argv[1]    
    
    if len(args) == 2:
        # No history length specified; take everything
        since_date = datetime.min
    else:
        history_length = timedelta( hours=float(args[2]) )
        since_date = datetime.now() - history_length
    
    #
    # Setup
    engine = create_engine( db_URL )
    connection = engine.connect()

    #
    # Info
    num_checkins = num_checkins_since( connection, since_date )    
    num_venues = num_visited_venues_since( connection, since_date )
    checkins_per_venue = (float(num_checkins)/float(num_venues)) if num_venues else float('NaN')
    
    since_text = 'unrestricted' if (since_date is datetime.min) else since_date.isoformat(' ')
    
    print '----'
    print "Checking database:          %s" % db_filename
    print "Looking at checkins since:  %s" % since_text
    print '----'
    print "Number of checkins:         %d" % num_checkins
    print "Number of visited venues:   %d" % num_venues
    print "Checkins per venue:         %f" % checkins_per_venue
    print '----'
    
    #
    # Finish
    connection.close()

