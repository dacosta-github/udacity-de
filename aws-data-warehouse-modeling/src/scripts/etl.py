"""
    This module reads and processes files from events and songs data and loads them into final tables in our AWS
    redshift cluster / database (load data from S3 into staging tables on AWS Redshift and then 
    process that data into analytics tables on AWS Redshift). This file should always be 
    executed after running the DDL scripts and the database should be cleaned.
"""

# Import packages / libraries
import configparser
import psycopg2
import sys
sys.path.insert(0, '../src/scripts')
import sql_queries as sql_queries


def load_staging_tables(cur, conn):
    """
    - This function:
        - load data from AWS S3 udacity bucket to AWS Redshift staging tables
    
    Args:
        cur (object): psycopg2 cursor object to execute aws redshift command in a database session
        conn (object): psycopg2 connection object to db

    Returns:
        None
    """

    for query in sql_queries.copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue load staging table: " + query)
            print(e)


def insert_tables(cur, conn):
    """
    - This function:
        - load data from staging tables to AWS redshift analytics database
    
    Args:
        cur (object): psycopg2 cursor object to execute redshift command in a db session
        conn (object): psycopg2 connection object to db

    Returns:
        None
    """

    for query in sql_queries.insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue insert analytics table: " + query)
            print(e)


def truncate_tables(cur, conn):
    """
    - This function: 
        - truncate each table using the queries in truncate_table_queries list (from sql_queries.py)
    
    Args:
        cur (object): psycopg2 cursor object to execute redshift command in a db session
        conn (object): psycopg2 connection object to db
    
    Returns:
        None
    """
    
    for query in sql_queries.truncate_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue truncate table: " + query)
            print(e)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Variables to create connection to Redshift Cluster
    host = config.get('CLUSTER', 'HOST')
    db_name = config.get('CLUSTER', 'DB_NAME')
    db_username = config.get('CLUSTER', 'DB_USER')
    db_password = config.get('CLUSTER', 'DB_PASSWORD')
    port = config.getint('CLUSTER', 'DB_PORT')

    print('1. Executing ETL pipeline...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, db_name, db_username, db_password, port))
    cur = conn.cursor()
    
    print('2. Cleaning all final tables...')
    truncate_tables(cur, conn)

    print('3. Loading staging tables...')
    load_staging_tables(cur, conn)

    print('4. Loading final tables...')
    insert_tables(cur, conn)

    conn.close()
    print('5. ETL pipeline completed!')

if __name__ == "__main__":
    main()