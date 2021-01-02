"""
    This module reads and processes files from events data and loads them into final 
    tables in our cassandra cluster and database. This file should always be 
    executed before running the DDL scripts
    The db should be cleaned
"""

# Import Python packages 
import csv
import glob
import os
from os.path import dirname
import pandas as pd
import cassandra 
from csql_queries import *
from database import *
from process_events import *

def main():
    """
    - This function:
        - extract data files
        - load data from files to Apache Cassandra cluster
    
    Args:
        None

    Returns:
        None
    """    
    
    # connect to default database
    print("Creating connection ...")
    cluster, session = create_session()
    
    # change keyspace
    print("Setting keyspace ...")
    session.set_keyspace('music_app_history')

    # get files
        # File
    file = 'event_datafile_new.csv'
    event_datafile_path = dirname(dirname(os.getcwd())) + '/data/event_data/' + file

    # Remove a file
    os.remove(event_datafile_path) 

    # get the current folder and subfolder event data
    filepath = dirname(dirname(os.getcwd())) + '/data/event_data'

    print("Getting files ...")
    file_path_list = get_files(filepath)

    print("Getting records ...")
    full_data_rows_list = get_records(file_path_list)

    print("Creating output file ...")
    output_row_count = create_output_file(full_data_rows_list)

    print(f'Written {output_row_count} rows to the output file.')

    # trunacte tables
    print("Trucating tables...")
    truncate_tables(session)

    # insert data into dataframe
    print("Inserting data ...")
    df = pd.read_csv(event_datafile_path, encoding='utf8')

    # create new column
    print("Creating a new column user name ...")
    df['user_name'] = df['firstName'] + ' ' + df['lastName']

    # insert data
    print("Question 1 - Load data to apache cassandra - csql_insert_table_1 ...")
    load_data(session, csql_insert_table_1, df, ['sessionId', 'itemInSession', 'artist', 'song', 'length'])

    print("Question 2 - Load data to apache cassandra - csql_insert_table_2 ...")
    load_data(session, csql_insert_table_2, df, ['userId', 'sessionId', 'itemInSession', 'artist', 'song', 'user_name'])

    print("Question 3 - Load data to apache cassandra - csql_insert_table_3 ...")
    load_data(session, csql_insert_table_3, df, ['song', 'userId', 'user_name'])

    # Remove a file
    os.remove(event_datafile_path)    

    print('The data has been successfully loaded!!')


if __name__ == '__main__':
    main()