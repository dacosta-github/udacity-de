"""
    This module provides all methods to interact with Cassandra Apache cluster server.
    It contains the cluster and session creation methods, keyspace, tables and data loading
"""

# Import Python packages 
import json
import logging
from csql_queries import *
import cassandra
from cassandra.cluster import Cluster
from cassandra import InvalidRequest

def create_session():
    """
    - This function:
        - creates a session with a local cluster
        - returns the connection and cluster
    
    Args:
        None

    Returns:
        cluster (object): local cluster
        session (object): cassandra connection object
    """
    
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    
    return cluster, session


def load_data(session, query, df, df_columns):
    """
    - This function:
        - load data from event file to apache cassandra database
    
    Args:
        query (object): query to insert data
        df (object): dataframe with data
        session (object): cassandra connection object
        df_columns (list): columns in dataframe

    Returns:
        None
    """
    
    for i, row in df.iterrows():
        data = tuple([row[col] for col in df_columns])
        session.execute(query, data)


def create_tables(session):
    """
    - This function:
        - creates all tables in keyspace on Apache Cassandra cluster
    
    Args:
        session (object): cassandra connection object

    Returns:
        None
    """
   
    for query in csql_create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)


def create_keyspace(session):
    """
    - This function:
        - create keyspace on Apache Cassandra cluster
    
    Args:
        session (object): cassandra connection object

    Returns:
        None
    """
    
    try:
        session.execute(csql_create_keyspace)
    except Exception as e:
        print(e)


def drop_tables(session):
    """
    - This function:
        - drops all tables in keyspace on Apache Cassandra cluster
    
    Args:
        session (object): cassandra connection object

    Returns:
        None
    """

    for query in csql_drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)  


def truncate_tables(session):
    """
    - This function:
        - clean all tables in keyspace on Apache Cassandra cluster
    
    Args:
        session (object): cassandra connection object

    Returns:
        None
    """

    for query in csql_truncate_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)  