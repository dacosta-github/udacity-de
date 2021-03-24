class SqlQueries:
    insert_songplays_table = ("""
            SELECT
                md5(events.session_id || events.start_time) songplay_id,
                events.start_time,
                events.user_id,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.session_id,
                events.location,
                events.user_agent
            FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs  ON events.song = songs.title
                                           AND events.artist = songs.artist_name
                                           AND events.length = songs.duration
            WHERE songs.song_id IS NOT NULL
    """)

    insert_users_table = ("""
        SELECT distinct 
            user_id, 
            first_name, 
            last_name, 
            gender, 
            level
        FROM staging_events
        WHERE page='NextSong'
    """)

    insert_songs_table = ("""
        SELECT distinct 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
        FROM staging_songs
    """)

    insert_artists_table = ("""
        SELECT distinct 
            artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
        FROM staging_songs
    """)

    insert_time_table = ("""
        SELECT start_time, 
                extract(hour from start_time), 
                extract(day from start_time), 
                extract(week from start_time),
                extract(month from start_time), 
                extract(year from start_time), 
                extract(dayofweek from start_time)
        FROM songplays
    """)