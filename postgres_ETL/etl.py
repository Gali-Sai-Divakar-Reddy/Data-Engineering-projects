import os
import glob
import configparser
import psycopg2
import pandas as pd
from sql_queries import *

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

def main():
    pass

if __name__ == "__main__":
    main()