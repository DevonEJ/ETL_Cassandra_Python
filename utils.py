# Create full dataset

# Imports
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

def collect_data_filepaths(sub_directory):
    """
    Searches a given sub-directory of the current working directory, collecting filepaths
    and returning in a list.
    
    Arguments:
    sub_directory -- subdirectory where data files are contained, prefixed with forward slash
    
    Returns:
    file_paths -- list of file paths from given sub-directory.
    """
    filepath = os.getcwd() + sub_directory
    for root, dirs, files in os.walk(filepath):
        file_paths = glob.glob(os.path.join(root,'*'))
    return file_paths



def create_combined_dataset(file_path_list):
    """
    Takes a file path, reads all csv files in the path, and appends the contents
    to an empty list which is written to the file, 'event_datafile_new.csv' in the
    current working directory. Any rows for which the first item is empty are excluded
    from the file.
    
    Arguments:
    file_path_list -- file path to iterate through for csv files
    """
    full_data_rows_list = []
    for f in file_path_list: 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
                if (line[0] == ''):
                    continue
                else:
                    full_data_rows_list.append(line)
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            else:
                writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


def get_data():
    """
    Implements functions to collect filepaths from given sub-directory, and
    concatenates this into a list of data, and writes to a CSV file, 'event_datafile_new.csv'
    in the current working directory.
    
    """
    files = collect_data_filepaths(sub_directory = '/event_data')
    create_combined_dataset(files)
              