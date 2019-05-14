import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('~/.nanodegree_dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
REGION = config.get("S3", "REGION")

AWS_IAM = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events 
    (
      artist    VARCHAR(256),
      auth        VARCHAR(256),
      firstname        VARCHAR(256),
      gender    CHAR,
      iteminsession        DECIMAL ,
      lastname        VARCHAR(256),
      length      DECIMAL ,
      level       VARCHAR(256),
      location        VARCHAR(256),
      method   VARCHAR(256),
      page   VARCHAR(256),
      registration   BIGINT, 
      sessionid        DECIMAL ,
      song   VARCHAR(256),
      status        DECIMAL ,
      ts        int8,
      useragent   VARCHAR(256),
      userid   VARCHAR(256)
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs 
    (
      num_songs    DECIMAL ,
      artist_id        VARCHAR(256),
      artist_latitude        DECIMAL,
      artist_longitude    DECIMAL,
      artist_location        VARCHAR(256),
      artist_name      VARCHAR(256),
      song_id       VARCHAR(256),
      title        VARCHAR(256),
      duration   DECIMAL ,
      year   DECIMAL 
    );
""")

songplay_table_create = ("""
CREATE TABLE songplays (songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY, 
                       start_time timestamp NOT NULL sortkey, 
                       user_id varchar NOT NULL, 
                       level varchar,
                       song_id varchar ,
                       artist_id varchar,
                       session_id DECIMAL,
                       location varchar,
                       user_agent varchar); 

""")

user_table_create = ("""
CREATE TABLE users (user_id varchar PRIMARY KEY sortkey, 
                                   first_name varchar, 
                                   last_name varchar, 
                                   gender char, 
                                   level varchar)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs (song_id varchar PRIMARY KEY sortkey, 
                                   title varchar, 
                                   artist_id varchar, 
                                   year DECIMAL, 
                                   duration DECIMAL)
diststyle all; 
""")

artist_table_create = ("""
CREATE TABLE artists (artist_id varchar PRIMARY KEY , 
                                   name varchar, 
                                   location varchar, 
                                   latitude DECIMAL, 
                                   longitude DECIMAL)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE time (start_time timestamp PRIMARY KEY sortkey,  
                                   hour DECIMAL, 
                                   day DECIMAL, 
                                   week DECIMAL, 
                                   month DECIMAL, 
                                   year DECIMAL, 
                                   weekday varchar)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    compupdate off region '{}' format as json {};
""").format(LOG_DATA, AWS_IAM, REGION, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    compupdate off region '{}' format as json 'auto' truncatecolumns;
""").format(SONG_DATA, AWS_IAM, REGION)


# FINAL TABLES

# songplay_table_insert - Loop into the log_file and get the song info from the song
# using song: title, artist and length

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(        SELECT
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM    
                    (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                    FROM staging_events
                    WHERE page='NextSong') events
                LEFT JOIN staging_songs songs
                    ON events.song = songs.title
                    AND events.artist = songs.artist_name
                    AND events.length = songs.duration)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
(SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong')
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
(SELECT distinct song_id, title, artist_id, year, duration FROM staging_songs)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
(SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    (SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
           extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
    FROM songplays)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
