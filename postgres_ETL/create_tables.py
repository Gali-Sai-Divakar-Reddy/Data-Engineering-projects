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
    db_default = config.get('Database', 'default_database')
    db_name = config.get('Database', 'database')
    db_username = config.get('Database', 'username')
    db_pwd = config.get('Database', 'pwd')
    db_port = config.get('Database', 'port_id')
 
    # Return a dictionary with the retrieved values
    config_values = {
        'db_name': db_name,
        'db_default': db_default,
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

    host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id
    """
    config_data = read_config()

    #connect to default database
    conn = psycopg2.connect(
        host= config_data['db_host'],
        dbname = config_data['db_default'],
        user = config_data['db_username'],
        password = config_data['db_pwd'],
        port = config_data['db_port']
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    #connect to sparkify database
    conn = psycopg2.connect(
        host= config_data['db_host'],
        dbname = config_data['db_name'],
        user = config_data['db_username'],
        password = config_data['db_pwd'],
        port = config_data['db_port']
    )
    cur = conn.cursor()

    return cur, conn

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

    cur, conn = create_database()

if __name__ == "__main__":
    main()