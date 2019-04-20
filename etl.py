### File implements the etl pipeline to populate the Sparkify music history data pipeline,
### loading the sparkifydb Cassandra database tables.

## Python module imports
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

## SQL query imports
from sql_queries import(
    create_keyspace,
    drop_keyspace,
    create_length_table,
    create_session_table,
    create_listeners_table,
    song_lengths_insert,
    session_table_insert,
    listeners_table_insert,
)

# Import utility functions
from utils import(
    get_data
)


## Create Cassandra cluster and session object
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

## Drop keyspace if exists
session.execute(drop_keyspace)

## Create and set keyspace if not yet existing
session.execute(create_keyspace)
session.set_keyspace('sparkifydb')

## Create song_lengths, session_table, and song_listeners tables in keyspace
session.execute(create_length_table)
session.execute(create_session_table)
session.execute(create_listeners_table)

## Get dataset to load
event_data = get_data()

## Insert data into tables
# Insert data into song_lengths table
for line in event_data:
    session.execute(song_lengths_insert, (line[8], line[0], line[3], line[9], line[5]))

# Insert into session_table
for line in event_data:
    session.execute(session_table_insert, (line[10], line[0], line[9], line[1], line[4], line[3], line[8]))

# Insert into listeners_table
for line in event_data:
    session.execute(listeners_table_query, (line[1], line[4], line[9]))
    
## Close database and cluster connections
session.shutdown()
cluster.shutdown()