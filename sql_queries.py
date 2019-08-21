import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events "
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stg_events(artist varchar(200), auth varchar(35),
firstname	varchar(35), gender varchar(5), iteminsession	integer	, lastname	varchar(35), length	float, 
level	varchar(20), location	varchar(1000), method	varchar(10), page	varchar(20),registration	BIGINT,
sessionId	integer, song varchar(200), status integer, ts varchar(1000) NOT NULL, userAgent varchar(1000), 
userID integer) """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stg_songs(artist_id	varchar(100) NOT NULL, 
artist_latitude	float, artist_location	varchar(200), artist_longitude float, artist_name varchar(200), 
duration	float, num_songs	integer, song_id varchar(200) NOT NULL, title varchar(500), year integer) 
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
(songplay_id INT IDENTITY(1000,1) PRIMARY KEY, start_tm timestamp NOT NULL, user_id varchar NOT NULL, 
level varchar, song_id varchar, artist_id varchar, session_id int, location text, user_agent varchar)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id varchar, 
first_name varchar, last_name varchar, gender char, level varchar)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY NOT NULL, 
song_name varchar, artist_id varchar, year int, duration numeric)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY NOT NULL, 
artist_name varchar, location varchar, lattitude varchar, longitude varchar)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(time timestamp PRIMARY KEY NOT NULL, 
hour int, day int, week int, month int, year int, weekday int)""")

# STAGING TABLES

staging_events_copy = ("""COPY stg_events FROM '{}'
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2'
JSON '{}';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""COPY stg_songs FROM '{}'
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2'
format as json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_tm, user_id , level, song_id, artist_id,
session_id, location, user_agent) 
SELECT distinct t.time start_tm, e.userid, e.level, tmp.song_id, tmp.artist_id, e.sessionid, e.location, e.useragent
from stg_events e,
time t,
(select s.song_id, s.song_name, s.duration, s.artist_id, a.artist_name
FROM artists a, songs s
WHERE a.artist_id = s.artist_id)tmp
WHERE e.page='NextSong'
AND e.artist=tmp.artist_name and e.song = tmp.song_name and CAST(e.length AS INT) = tmp.duration;
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT userid, firstname, lastname, gender, level from stg_events;""")

song_table_insert = ("""INSERT INTO songs(song_id, song_name, artist_id, year, duration)
SELECT song_id, title, artist_id,  year, duration from stg_songs;""")

artist_table_insert = ("""INSERT INTO artists(artist_id, artist_name, location, lattitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from stg_songs;""")

time_table_insert = ("""INSERT INTO time(time , hour , day , week, month, year, weekday)
select distinct start_time,
EXTRACT(HOUR from start_time) as tm_hr,
EXTRACT(DAY from start_time) as tm_day,
EXTRACT(WEEK from start_time) as tm_week,
EXTRACT(MONTH from start_time) as tm_month,
EXTRACT(YEAR from start_time) as tm_year,
EXTRACT(DOW from start_time) as weekday
from (
SELECT TIMESTAMP 'epoch' + CAST(ts AS BIGINT)/1000 * INTERVAL '1 second' as start_time 
FROM stg_events
)tm;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
