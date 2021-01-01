"""
    This module allows the creation or reset of the entire database, 
    in view of the requirements that have been defined
"""

# Import Python packages 
 
from csql_queries import *
from database import *
import cassandra 
from cassandra.cluster import Cluster
from cassandra import InvalidRequest

def main():
    """
    - This function:
        - creates database  
        - establishes connection
        - changes keyspace
        - drops all tables
        - creates all tables
    
    Args:
        None

    Returns:
        None
    """
    
    # connect to default database
    print("Creating connection...")
    cluster, session = create_session()

    # connect to default database
    print("Creating keyspace...")
    create_keyspace(session)
    
    # change keyspace
    print("Setting keyspace...")
    session.set_keyspace('music_app_history')

    # drop tables
    print("Dropping tables...")
    drop_tables(session)

    # create tables    
    print("Creating tables...")
    create_tables(session)

    # clean sessions
    print("Closing connection...")
    session.shutdown()
    cluster.shutdown()

    print("Database created!")

if __name__ == "__main__":
    main()