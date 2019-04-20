# File contains all SQL queries required for the Sparkify music history app Cassandra 'sparkifydb' data pipeline


# Create Keyspace
create_keyspace = "CREATE KEYSPACE IF NOT EXISTS sparkifydb WITH REPLICATION\
= { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

# Drop Keyspace
drop_keyspace = "DROP KEYSPACE IF EXISTS sparkifydb;"


# Create Tables
create_length_table = "CREATE TABLE IF NOT EXISTS sparkifydb.song_length\
(sessionId text, artist_name text, itemInSession text,\
song_title text, song_length text, PRIMARY KEY(sessionId, itemInSession));"

create_session_table = "CREATE TABLE IF NOT EXISTS sparkifydb.session_table (userId text,\
artist_name text, song_name text, first_name text,\
last_name text, itemInSession text, sessionId text, PRIMARY KEY(userId, sessionId, song_name, itemInSession));"

create_listeners_table = "CREATE TABLE IF NOT EXISTS song_listeners (first_name text,\
last_name text, song text, PRIMARY KEY(song, first_name, last_name));"


# Insert statements
song_lengths_insert = "INSERT INTO song_length (sessionId, artist_name, itemInSession, song_title, song_length)\
VALUES (%s, %s, %s, %s, %s) IF NOT EXISTS "

session_table_insert = "INSERT INTO session_table (userId, artist_name, song_name, first_name, last_name, itemInSession, sessionId)\
VALUES (%s, %s, %s, %s, %s, %s, %s) IF NOT EXISTS "

listeners_table_insert =  "INSERT INTO song_listeners (first_name, last_name,\
song) VALUES (%s, %s, %s) IF NOT EXISTS "


# Select statements
get_lengths_query = "SELECT artist_name, song_title, song_length FROM song_length\
WHERE sessionId = '338' AND itemInSession = '4';"

get_sessions_query = "SELECT artist_name, song_name, first_name, last_name FROM\
session_table WHERE userId = '10' AND sessionId = '182';"

get_listeners_query = "SELECT first_name, last_name FROM song_listeners WHERE song\
= 'All Hands Against His Own';"