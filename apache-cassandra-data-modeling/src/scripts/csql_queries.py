"""
    This module contains all csql queries, and it's used (imported) in other files or scripts
"""

# Scripts

# create keyspace and tables 
csql_create_keyspace = """
        CREATE KEYSPACE IF NOT EXISTS music_app_history
        WITH REPLICATION =
        { 'class': 'SimpleStrategy', 'replication_factor' : 1 }
    """

csql_create_table_1 = """CREATE TABLE IF NOT EXISTS song_in_session_history ( session_id INT, item_in_session INT, artist TEXT, song TEXT, length FLOAT, \
           PRIMARY KEY(session_id, item_in_session) )"""

csql_create_table_2 = """CREATE TABLE IF NOT EXISTS songs_in_user_history ( user_id INT, session_id INT, item_in_session INT, artist TEXT, song TEXT, user_name TEXT, 
           PRIMARY KEY((user_id), session_id, item_in_session) )"""

csql_create_table_3 = """CREATE TABLE IF NOT EXISTS user_in_song_history ( song TEXT, user_name TEXT, 
           PRIMARY KEY(song, user_name) )"""


## insert into: copy from dataframe to cassandra
csql_insert_table_1 = """
    INSERT INTO song_in_session_history ( session_id, item_in_session, artist, song, length )
    VALUES(%s, %s, %s, %s, %s)
"""

csql_insert_table_2 = """
    INSERT INTO songs_in_user_history ( user_id, session_id, item_in_session, artist, song, user_name )
    VALUES(%s, %s, %s, %s, %s, %s)
"""

csql_insert_table_3 = """
    INSERT INTO user_in_song_history ( song, user_name )
    VALUES(%s, %s)
"""

## final to create all tables on keysapce in cassandra
csql_create_table_queries = [csql_create_table_1, csql_create_table_2, csql_create_table_3]

## drop objects
csql_drop_keyspace = """DROP KEYSPACE IF EXISTS music_app_history"""

csql_drop_table_1  = """DROP TABLE IF EXISTS song_in_session_history"""

csql_drop_table_2  = """DROP TABLE IF EXISTS songs_in_user_history"""

csql_drop_table_3  = """DROP TABLE IF EXISTS user_in_song_history"""


csql_drop_table_queries = [csql_drop_table_1, csql_drop_table_2, csql_drop_table_3]


## truncate tables
csql_truncate_table_1  = """TRUNCATE TABLE song_in_session_history"""

csql_truncate_table_2  = """TRUNCATE TABLE songs_in_user_history"""

csql_truncate_table_3  = """TRUNCATE TABLE user_in_song_history"""

csql_truncate_table_queries = [csql_truncate_table_1, csql_truncate_table_2, csql_truncate_table_3]

