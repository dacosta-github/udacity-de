"""
    This module provides all methods to interact with AWS Redshift cluster server.
    It contains the cluster and session creation methods, database, tables and data loading
"""

# Import packages / libraries
import configparser
import psycopg2
import sys
sys.path.insert(0, '../src/scripts')
import sql_queries as sql_queries


def drop_tables(cur, conn):
    """
    - This function: 
        - drops each table using the queries in drop_table_queries list (from sql_queries.py)
    
    Args:
        cur (object): psycopg2 cursor object to execute aws redshift command in a database session
        conn (object): psycopg2 connection object to database
    
    Returns:
        None
    """

    for query in sql_queries.drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue dropping table: " + query)
            print(e)



def create_tables(cur, conn):
    """
    - This function:
        - creates each table using the queries in create_table_queries list (from sql_queries.py)
    
    Args:
        cur: psycopg2 cursor object to execute aws redshift command in a database session
        conn: psycopg2 connection object to database
    
    Returns:
        None
    """

    for query in sql_queries.create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue creating table: " + query)
            print(e)


def main():
    """
    - This main: 
        - establishes connection with the AWS Redshift database and gets cursor to it 
        - drops (if exists) and creates the database
        - drops all the tables
        - creates all tables needed
        - closes the connection

    Args:
        - host (str): AWS Redshift cluster address
        - dbname (str): database name
        - user (str): username for the database
        - password (str): password for the database
        - port (str): database port to connect
    
    Returns:
        None
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Variables to create connection to Redshift Cluster
    host = config.get('CLUSTER', 'HOST')
    db_name = config.get('CLUSTER', 'DB_NAME')
    db_username = config.get('CLUSTER', 'DB_USER')
    db_password = config.get('CLUSTER', 'DB_PASSWORD')
    port = config.getint('CLUSTER', 'DB_PORT')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, db_name, db_username, db_password, port))
    cur = conn.cursor()
    print('1. The cluster connection has been successfully established!')

    print('2. Dropping tables...')
    drop_tables(cur, conn)

    print('3. Creating tables...')
    create_tables(cur, conn)

    print('4. The database schema was created with success.')

    conn.close()


if __name__ == "__main__":
    main()