import psycopg2
import configparser
from sql_queries import create_table_queries, drop_table_queries

def read_config():
    # Create a ConfigParser object
    config = configparser.ConfigParser()
 
    # Read the configuration file
    config.read('config.ini')
 
    # Access values from the configuration file
    db_host = config.get('Database', 'hostname')
    db_name = config.get('Database', 'database')
    db_username = config.get('Database', 'username')
    db_pwd = config.get('Database', 'pwd')
    db_port = config.get('Database', 'port_id')
 
    # Return a dictionary with the retrieved values
    config_values = {
        'db_name': db_name,
        'db_host': db_host,
        'db_username': db_username,
        'db_pwd': db_pwd,
        'db_port': db_port
    }
 
    return config_values

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