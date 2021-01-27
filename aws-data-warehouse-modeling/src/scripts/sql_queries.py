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
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

# Factual
songplay_table_drop = "DROP TABLE IF EXISTS songplays"

# Dimensions
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# TRUNCATE TABLES

# Staging
staging_events_table_truncate = "TRUNCATE TABLE staging_events"
staging_songs_table_truncate = "TRUNCATE TABLE staging_songs"

# Factual
songplay_table_truncate = "TRUNCATE TABLE songplays"

# Dimensions
user_table_truncate = "TRUNCATE TABLE users"
song_table_truncate = "TRUNCATE TABLE songs"
artist_table_truncate = "TRUNCATE TABLE artists"
time_table_truncate = "TRUNCATE TABLE time"


# CREATE STAGING TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
                event_id      BIGINT IDENTITY(0,1)    NOT NULL,
                artist        VARCHAR                 NULL,
                auth          VARCHAR                 NULL,
                first_name    VARCHAR                 NULL,
                gender        VARCHAR                 NULL,
                item_in_session VARCHAR               NULL,
                last_name     VARCHAR                 NULL,
                length        VARCHAR                 NULL,
                level         VARCHAR                 NULL,
                location      VARCHAR                 NULL,
                method        VARCHAR                 NULL,
                page          VARCHAR                 NULL,
                registration  VARCHAR                 NULL,
                session_id    INTEGER                 NULL,
                song          VARCHAR                 NULL,
                status        INTEGER                 NULL,
                ts            BIGINT                  NULL,
                user_agent    VARCHAR                 NULL,
                user_id       INTEGER                 NULL);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
                artist_id           VARCHAR         NOT NULL,
                artist_latitude     FLOAT           NULL,
                artist_longitude    FLOAT           NULL,
                artist_location     VARCHAR         NULL,
                artist_name         VARCHAR         NULL,
                song_id             VARCHAR         NULL,
                title               VARCHAR         NULL,
                year                INTEGER         NULL,
                num_songs           INTEGER         NULL,
                duration            FLOAT           NULL);
""")

# CREATE FINAL TABLES
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
                songplay_id INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
                start_time  TIMESTAMP               NOT NULL,
                user_id     VARCHAR(50)             NOT NULL DISTKEY,
                level       VARCHAR(10)             NOT NULL,
                song_id     VARCHAR(40)             NOT NULL,
                artist_id   VARCHAR(50)             NOT NULL,
                session_id  VARCHAR(50)             NOT NULL,
                location    VARCHAR(100)            NULL,
                user_agent  VARCHAR(255)            NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER                 NOT NULL SORTKEY,
                first_name  VARCHAR(50)             NULL,
                last_name   VARCHAR(80)             NULL,
                gender      VARCHAR(10)             NULL,
                level       VARCHAR(10)             NULL
    ) DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
                song_id     VARCHAR(50)             NOT NULL SORTKEY,
                title       VARCHAR(500)            NOT NULL,
                artist_id   VARCHAR(50)             NOT NULL,
                year        INTEGER                 NOT NULL,
                duration    DECIMAL(9)              NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
                artist_id   VARCHAR(50)             NOT NULL SORTKEY,
                name        VARCHAR(500)            NULL,
                location    VARCHAR(500)            NULL,
                latitude    DECIMAL(9)              NULL,
                longitude   DECIMAL(9)              NULL
    ) DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
                start_time  TIMESTAMP               NOT NULL SORTKEY,
                hour        SMALLINT                NULL,
                day         SMALLINT                NULL,
                week        SMALLINT                NULL,
                month       SMALLINT                NULL,
                year        SMALLINT                NULL,
                weekday     SMALLINT                NULL
    ) DISTSTYLE ALL;
""")

#### INSERT SCRIPTS

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT as JSON {}
    REGION {};
""").format(LOG_DATA, ROLE_ARN, LOG_JSONPATH, REGION_NAME)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT as JSON 'auto'
    REGION {};
""").format(SONG_DATA, ROLE_ARN, REGION_NAME)


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id,
                                        location, user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'   AS start_time,
            se.user_id                   AS user_id,
            se.level                     AS level,
            ss.song_id                   AS song_id,
            ss.artist_id                 AS artist_id,
            se.session_id                AS session_id,
            se.location                  AS location,
            se.user_agent                AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
        ON se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT se.user_id           AS user_id,
            se.first_name                 AS first_name,
            se.last_name                  AS last_name,
            se.gender                     AS gender,
            se.level                      AS level
    FROM staging_events AS se
    WHERE se.user_id IS NOT NULL
        and se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT ss.song_id         AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
    FROM staging_songs AS ss
    WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM staging_songs AS ss
    WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events                   AS se
    WHERE se.page = 'NextSong';
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
truncate_table_queries = [staging_events_table_truncate, staging_songs_table_truncate, songplay_table_truncate, user_table_truncate, song_table_truncate, artist_table_truncate, time_table_truncate]
