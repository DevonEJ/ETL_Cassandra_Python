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
from cql_queries import(
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

# Remove ipython checkpoints (can distort data processing)
os.system('rm -rf event_data/.ipynb_checkpoints')

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
get_data()

## Insert data into tables
# CSV file containing all of the event data rows which have artist names
file = 'event_datafile_new.csv'

# Insert data into song_lengths table
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        session.execute(song_lengths_insert, (int(line[8]), line[0], int(line[3]), line[9], float(line[5])))

# Insert into session_table
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        session.execute(session_table_insert, (int(line[10]), int(line[8]), line[9], int(line[3]), line[0], line[1], line[4]))       

# Insert into listeners_table
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        session.execute(listeners_table_insert, (line[9], line[1], line[4], int(line[10])))
    
## Close database and cluster connections
session.shutdown()
cluster.shutdown()

# What to do next...
print('Tables successfully loaded.')
print('Now open up run_queries_here.ipynb to execute queries on the tables created...')