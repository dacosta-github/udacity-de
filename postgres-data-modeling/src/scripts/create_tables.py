import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - This function:
        - creates and connects to the sparkify db 
        - returns the connection and cursor to sparkify db
    
    Args:
        None

    Returns:
        cur (object): psycopg2 cursor object to execute PostgreSQL command in a db session
        conn (object): psycopg2 connection object to sparkify db
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 port=15432 dbname=postgres_db  user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 port=15432 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    - This function: 
        - drops each table using the queries in drop_table_queries list (from sql_queries.py)
    
    Args:
        cur (object): psycopg2 cursor object to execute PostgreSQL command in a db session
        conn (object): psycopg2 connection object to sparkify db
    
    Returns:
        None
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    - This function:
        - creates each table using the queries in create_table_queries list (from sql_queries.py)
    
    Args:
        cur: psycopg2 cursor object to execute PostgreSQL command in a db session
        conn: psycopg2 connection object to sparkify db
    
    Returns:
        None
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - This function: 
        - drops (if exists) and creates the sparkify db
        - establishes connection with the sparkify db and gets cursor to it 
        - drops all the tables
        - creates all tables needed
        - closes the connection

    Args:
        None
    
    Returns:
        None
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    print('Process executed with success!')
    conn.close()


if __name__ == "__main__":
    main()