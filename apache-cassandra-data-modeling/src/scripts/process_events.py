"""
    This module provides all the methods to interact with the source files. 
    It includes methods to collect all the events in a directory, collect the data 
    in a structure and create a final file with all the structured data that exists 
    in all the collected files. This module is run in the etl module
"""

# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from os.path import dirname


def get_files(filepath):
    """
    - This function:
        - creating list of filepaths to process original event csv data files
    
    Args:
        filepath (object): qcurrent folder and subfolder event data

    Returns:
        file_path_list (object) list with all files
    """
    
    file_path_list = []

    # collect each filepath
    for root, dirs, files in os.walk(filepath):
        
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root, '*'))
        #print(len(file_path_list))

    return file_path_list


def get_records(file_path_list):
    """
    - This function:
        - processing the files to create the data file csv that will be used for Apache Casssandra tables
    
    Args:
        file_path_list (object): list with the files

    Returns:
        full_data_rows_list (object): dataframe with all records
    """

    full_data_rows_list = []

    # for every filepath in the file path list
    for f in file_path_list:
        
        # reading the csv file
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            # skip header row
            next(csvreader)

            # extracting each data row one by one and append it
            for line in csvreader:
                full_data_rows_list.append(line)

    return full_data_rows_list


def create_output_file(full_data_rows_list):
    """
    - This function:
        - creating a smaller event data csv file called event_datafile_full csv 
        - that will be used to insert data into the Apache Cassandra cluster tables
    
    Args:
        full_data_rows_list (object): list with data rows

    Returns:
        None
    """

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the apache Cassandra tables
    output_row_count = 0
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    file = 'event_datafile_new.csv'
    event_datafile_path = dirname(dirname(os.getcwd())) + '/data/event_data/' + file
   
    with open(event_datafile_path, 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', \
                         'level', 'location', 'sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    print('File event_datafile_new.csv generated!')