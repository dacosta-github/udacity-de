"""
    This module provides all methods to interact with files.
"""
from __future__ import print_function
import os 
import re
import string
import pandas as pd
import numpy as np
import requests as req
import pathlib
import zipfile
import pickle
import argparse
import time
from datetime import datetime
import sys
sys.path.insert(0, '../src')
sys.path.insert(0, '../src/utils/')
from utils.helpers import download, convert_json_to_csv, split_csv

def main():

    ## Initiate Processing on 
    print(str(datetime.now()) + ':: Step 1 - Downloading CSV Files Started.')
    url = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
    dest_folder = 'data/source'
    data_file = download(url, dest_folder, unzip=1)
    
    # Load dataset with pandas to a variable
    df_complaints = pd.read_csv(data_file)
    print("Complaints data read successfully!")
    print("Count Rows: " + str(df_complaints.shape))

    print(str(datetime.now()) + ':: Step 2 - Downloading JSON Files Started.')

    print(str(datetime.now()) + ':: Step 3 - Convert and Split JSON into CSV Files Started.')

    ## CSV
    split_csv('../data/source/complaints.csv', '../data/raw/complaints/', 'complaints_part', 100000)

    ## CONVERT TO JSON
    convert_json_to_csv('../data/source/zip.json', '../data/raw/geo/zip/', 'zip_part_01',  10000)
    convert_json_to_csv('../data/source/counties.json', '../data/raw/geo/counties/', 'counties_part_01',  10000)
    convert_json_to_csv('../data/source/cities.json', '../data/raw/geo/cities/', 'cities_part_01',  10000)
    convert_json_to_csv('../data/source/states.json', '../data/raw/geo/states/', 'states_part_01',  10000)


if __name__ == "__main__":
    main()



