"""
    This module contains all sql queries/statements, and it's used (imported) in 
    other files or scripts
"""

# Import packages / libraries
import configparser


# CONFIG FILE
config = configparser.ConfigParser()
config.read('dwh.cfg')


# VARIABLES
ROLE_ARN        = config.get('SECURITY', 'ROLE_ARN')
REGION_NAME     = config.get('S3', 'region_name_s3')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')


# DROP TABLES
# Staging
staging_events_table_drop = "DROP TABLE IF EXISTS public.staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS public.staging_songs"

# Factual
songplay_table_drop = "DROP TABLE IF EXISTS public.songplays"

# Dimensions
user_table_drop = "DROP TABLE IF EXISTS public.users"
song_table_drop = "DROP TABLE IF EXISTS public.songs"
artist_table_drop = "DROP TABLE IF EXISTS public.artists"
time_table_drop = "DROP TABLE IF EXISTS public.time"


# CREATE STAGING TABLES

create_staging_songs_table = ("""
   CREATE TABLE public.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4
        ); 
""")

create_songplays_table = ("""
    CREATE TABLE public.songplays (
            play_id varchar(32) NOT NULL,
            start_time timestamp NOT NULL,
            user_id int4 NOT NULL,
            "level" varchar(256),
            song_id varchar(256),
            artist_id varchar(256),
            session_id int4,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pk PRIMARY KEY (play_id)
        );
""")

create_artists_table = ("""
     CREATE TABLE public.artists (
            artist_id varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            lattitude numeric(18,0),
            longitude numeric(18,0),
            CONSTRAINT artists_pk PRIMARY KEY (artist_id)
        );   
""")

create_songs_table = ("""
 CREATE TABLE public.songs (
            song_id varchar(256) NOT NULL,
            title varchar(256),
            artist_id varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pk PRIMARY KEY (song_id)
        );
""")

create_time_table = ("""
    CREATE TABLE public."time" (
            start_time timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            "month" varchar(256),
            "year" int4,
            weekday varchar(256),
            CONSTRAINT time_pk PRIMARY KEY (start_time)
        );
""")

create_users_table = ("""
 CREATE TABLE public.users (
            user_id int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pk PRIMARY KEY (user_id)
        );  
""")

create_staging_events_table  = ("""
    CREATE public.staging_events (
            artist varchar(256),
            auth varchar(256),
            first_name varchar(256),
            gender varchar(256),
            item_in_session int4,
            last_name varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            session_id int4,
            song varchar(256),
            status int4,
            ts int8,
            user_agent varchar(256),
            user_id int4
        );
    
""")

# QUERY LISTS

create_table_queries = [create_staging_events_table, create_staging_songs_table, create_songplays_table, create_users_table, create_songs_table, create_artists_table, create_time_table]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]