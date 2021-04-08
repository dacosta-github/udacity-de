"""
    This module provides all methods to help in other tasks.
"""
from scipy.stats import randint
import os 
import re
import string
import pandas as pd
import numpy as np
import requests as req
import pathlib
import zipfile
import pickle
import csv
import json
import sys

# Download a file based on url
def download(url: str, dest_folder: str, unzip: int):
    """
        This function:
            -> Reads URL file, list of directory, make a file download and saving this to same folder. 
        
        Args:
            -> param url: A dataframe of file url information to download - including a column for `File`
            -> param dest_folder: the main directory where files are stored
            -> param_unzip: if we want unzip the file
        
        Return: 
            -> A string with file name and path
    """
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder); # create folder if it doesn't exist
        
    file_name = url.split('/')[-1].replace(" ", "_");
    file_path = '../'+os.path.join(dest_folder, file_name);
    file = pathlib.Path(file_path);
    
    # remove ".zip"
    final_file_name = os.path.abspath(file).replace(".zip", "")
    
    # final result file name in path
    final_file_path = file_path.replace(".zip", "")
    
    if file.exists ():
        print ("The " + file_name + " file exist in " + dest_folder + " folder ("+file_path+").");
    else:
        print ("File does not exist. Start downloading " + file_name);

        requests = req.get(url, stream=True);

        unique_file_path = os.path.abspath(file_path);

        if requests.ok:
            print("Saving file to", dest_folder + '/' +file_name);
            with open(file_path, 'wb') as x:
                for chunk in requests.iter_content(chunk_size=1024 * 8):
                    if chunk:
                        x.write(chunk);
                        x.flush();
                        os.fsync(x.fileno());
        else:
            print("Download Failed: Status Code {}\n{}".format(r.status_code, r.text));

        if unzip==1:
            print ("Start unzipping file: " + file_name);
            with zipfile.ZipFile(unique_file_path, "r") as z:
                z.extractall('../'+dest_folder)
        else:  
            print("Download Completed.")
    
    return final_file_path;


# Pickling our subsetted dataframe
def save_pickle(dest_folder, df, file_name: str):
    """
        This function:
            -> Reads df and directory and save a file.
        
        Args:
            -> param file_name: the name for pickle file
            -> param dest_folder: the main directory where files are stored
            -> param df: data fram that we want save in pickle
            -> return: A string with status
    """
    if not os.path.exists(dest_folder): # Make sure that the folder exists
        os.makedirs(dest_folder)

    with open(os.path.join(dest_folder, file_name+'.pkl'), "wb") as f:
        pickle.dump(df, f, protocol=4);

    return file_name + " pickle file converted and loaded successfully!";


# Load pickle file
def load_pickle(folder, file_name: str):
    """
        This function:
            -> Reads df and directory and save a file.
    
        Args:
            -> param file_name: the name for pickle file
            -> param folder: the main directory where files are load
        
        Return: 
            -> A dataframe
    """
    
    with open(os.path.join(folder, file_name+'.pkl'), 'rb') as to_read:
        pickle_df = pickle.load(to_read);
        
    print(file_name+' pickle file loaded successfully!');

    return pickle_df;


# Convert int to string
def to_str(var):
    return str(list(np.reshape(np.asarray(var), (1, np.size(var)))[0]))[1:-1];


# Remove % of data in dataframe    
def reduce_dataset(df, column: str, value: str,  frac: float):
    """
        This function:
            -> Recive a input dataset and based on column and value reduce this same data set by fraction sample
    
        Args:
            -> df: a dataframe
            -> column: a string
            -> value: 
            -> frac: value to delete
        
        Return: 
            -> modified initial dataframe without % of data based on column and value
    """    
    try:
        df = \
            df.drop(
            df[df[column] == value].sample(frac=frac).index)
        return df
    except:
        return df;


# Split csv in several parts based on number of rows
def split_csv(source_filepath, dest_folder, split_file_prefix, records_per_file):
    """
        This function:
            -> Split a source csv into multiple csvs of equal numbers of records,
            except the last file. Includes the initial header row in each split file.

        Return:  
            -> `{prefix}_0.csv`
    """
    if records_per_file <= 0:
        raise Exception('records_per_file must be > 0')

    with open(source_filepath, 'r') as source:
        reader = csv.reader(source)
        headers = next(reader)

        file_idx = 0
        records_exist = True

        while records_exist:
            i = 0
            target_filename = f'{split_file_prefix}_{file_idx}.csv'
            target_filepath = os.path.join(dest_folder, target_filename)
            os.makedirs(os.path.dirname(target_filepath), exist_ok=True)

            with open(target_filepath, 'w', encoding='utf-8') as target:
                writer = csv.writer(target, delimiter = "|")

                while i < records_per_file:
                    if i == 0:
                        writer.writerow(headers)
                    try:
                        writer.writerow(next(reader))
                        i += 1
                    except:
                        records_exist = False
                        break
            
            print(f"Split {target_filepath} completed.")

            if i == 0:
                # delete that file
                os.remove(target_filepath)

            file_idx += 1;

            
# Convert from JSON to CSV
def convert_json_to_csv(source_filepath, dest_folder, split_file_prefix, records_per_file):
    """
        This function:
            -> Converto from json source to multiple csvs of equal numbers of records,
            except the last file.
        
        Return:  
            - >`{prefix}_0.csv`
    """
    # read json
    f = open(source_filepath)
    data = json.load(f)
    f.close()

    # create dataframe
    df = pd.json_normalize(data)

    file_s = dest_folder + 'geo_' + split_file_prefix + '.csv'
    os.makedirs(os.path.dirname(dest_folder), exist_ok=True)

    df.to_csv(dest_folder + 'geo_' + split_file_prefix + '.csv', index=False, encoding='utf-8')

    print(f"Convert JSON to CSV {file_s} completed.");