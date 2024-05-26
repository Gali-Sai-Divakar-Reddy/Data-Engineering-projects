import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    pass

def drop_tables():
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    pass

def create_tables():
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    pass

def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """

    pass

if __name__ == "__main__":
    main()