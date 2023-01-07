import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA =config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist             varchar(max),
auth               varchar(max),
first_name         varchar(max),
gender             varchar(max),
item_in_session    integer,
last_name          varchar(max),
length             numeric(16,8),
level              varchar(max),
location           varchar(max),
method             varchar(max),
page               varchar(max),
registration       bigint,
session_id         integer,
song               varchar(max),
status             integer,
ts                 bigint,
user_agent         varchar(max),
user_id            integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
  num_songs     	integer        not null,
  artist_id        	varchar(max)   not null,
  artist_latitude   varchar(max),
  artist_longitude  varchar(max),
  artist_location   varchar(max),
  artist_name      	varchar(max),
  song_id       	varchar(max)   not null,
  title             varchar(max),
  duration          numeric(16,8),
  year              integer
  );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
  songplay_id       	integer         IDENTITY (0,1) not null PRIMARY KEY,
  start_time        	bigint      	not null sortkey,
  user_id           	integer     	not null,
  level             	varchar(max),
  song_id           	varchar(max)   	not null distkey,
  artist_id         	varchar(max)   	not null,
  session_id        	integer,
  location          	varchar(max),
  user_agent          	varchar(max)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
  user_id       	integer        not null sortkey PRIMARY KEY,
  first_name    	varchar(max),
  last_name     	varchar(max),
  gender        	varchar(max),
  level         	varchar(max))
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
  song_id       	varchar(max)    not null sortkey  distkey PRIMARY KEY,
  title          	varchar(max),
  artist_id     	varchar(max)    not null,
  year          	integer,
  duration         	numeric(16,8));
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
  artist_id       	varchar(max)    not null sortkey PRIMARY KEY,
  name          	varchar(max),
  location      	varchar(max),
  lattitude        	varchar(max),
  longitude     	varchar(max))
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
  start_time       	bigint        not null sortkey PRIMARY KEY,
  hour          	integer,
  day           	integer,
  week          	integer,
  month         	integer,
  year          	integer,
  weekday         	integer)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {0}
    credentials 'aws_iam_role={1}'
    compupdate off region 'us-west-2'
    format as json {2};    
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {0}
    credentials 'aws_iam_role={1}'
    compupdate off region 'us-west-2'
    format as json 'auto';    
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT b.ts as start_time, b.user_id, b.level, a.song_id, a.artist_id, b.session_id, b.location, b.user_agent
FROM staging_songs a JOIN staging_events b 
ON a.title = b.song
AND a.artist_name = b.artist
AND b.page = 'NextSong'
AND a.song_id IS NOT NULL
AND a.artist_id IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT distinct user_id, first_name, last_name, gender, level 
FROM staging_events
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT distinct song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT ori_ts as start_time, EXTRACT(HOUR FROM ts) as hour, EXTRACT(DAY FROM ts) as day, 
EXTRACT(WEEK FROM ts) as week, EXTRACT(MONTH FROM ts) as month, EXTRACT(YEAR FROM ts) as year, 
EXTRACT(WEEKDAY FROM ts) as weekday
FROM( SELECT distinct ts as ori_ts, (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts
FROM staging_events
WHERE ts IS NOT NULL);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
