import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    ''' 
     - This function:
        - opens a song file and inserts required information into songs and artists dimension tables of a sparkify db
    
    Args: 
        cur (object): psycopg2 cursor to execute PostgreSQL command in a db session
        filepath (str): filepath to a folder containing song files
    
    Returns:
         None
    '''
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = tuple(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = tuple(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - This function:
        - opens a log file and inserts required information into users and time dimensions and songplays fact tables of a sprakify db
    
    Args:
        cur (object): psycopg2 cursor to execute PostgreSQL command in a db session
        filepath (str): filepath to a folder containing log files  
      
    Returns:
        None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'].reset_index(drop=True)

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms', origin='unix')

    # mapping 
    start_time = t
    hour = t.dt.hour
    day = t.dt.day
    week = t.dt.isocalendar().week
    month = t.dt.month
    year = t.dt.year
    weekday = t.dt.weekday
    
    # insert time data records
    time_data = [start_time, hour, day, week, month, year, weekday]
    column_labels = ['start_time','hour','day','weekofyear','month','year','weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data))).reset_index(drop=True)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table and remove duplicates
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record pd.to_datetime(row.ts, unit="ms")
        songplay_data = (pd.to_datetime(row.ts, unit="ms"), int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - This function:
        - process and load data recursively from filepath directory location
    
    Args:
        cur (object): psycopg2 cursor object to execute PostgreSQL command in a db session
        conn (object): psycopg2 connection object to sparkify db
        filepath (str): filepath to a folder containing files
        func (str): name of a function to be used in main()
    
    Returns:
        None
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 port=15432 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='../../data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='../../data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()