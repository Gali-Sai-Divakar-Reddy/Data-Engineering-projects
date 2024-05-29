# Sparkify ETL Pipeline

## Project Overview

The Sparkify ETL pipeline is designed to extract, transform, and load data from JSON logs and metadata files into a structured PostgreSQL database. This process enables analytical capabilities for understanding user activity and song play metrics on the Sparkify music streaming platform. The project processes two types of data sources:

 - Song Data: Metadata about songs and artists.
 - Log Data: User activity logs detailing song plays.

## Project Structure

```graphql
postgres_ETL/
│
├── data/
│   ├── log_data/            # User activity logs
│   └── song_data/           # Song metadata
│
├── etl.py                   # Main Python script for the ETL pipeline
├── etl_test.ipynb           # Jupyter Notebook for developing and testing the ETL process
├── sql_queries.py           # SQL queries for table creation and data insertion
├── create_tables.py         # Script to initialize the database and tables
├── test.ipynb               # Jupyter Notebook for testing the database
├── README.md                # Project documentation and setup instructions
└── config.ini               # Configuration file for database credentials
```

## Database Schema

The database uses a star schema optimized for queries on song play analysis. This schema includes the following tables:

1. Fact Table

    - `songplays` - Records in log data associated with song plays.

2. Dimension Tables

    - `users` - Users of the Sparkify platform.
    - `songs` - Songs in the music database.
    - `artists` - Artists in the music database.
    - `time` - Timestamps of records, broken down into specific units.

## ETL Pipeline

The ETL pipeline involves the following steps:

1. **Extract**: Load data from JSON files located in data/log_data and data/song_data.
2. **Transform**: Convert data into a format suitable for insertion into the PostgreSQL database.
3. **Load**: Insert data into the database using the schema defined in `sql_queries.py`.

### Transform Functions

- `transform_song_data`: Processes song data and loads it into `songs` and `artists` tables.
- `transform_log_data`: Processes log data and loads it into `time`, `users`, and `songplays` tables.

### Load Functions
- `load_song_data`: Inserts song and artist data into their respective tables.
- `load_log_data`: Inserts log data into time, user, and songplays tables.

## Setup and Execution

### Prerequisites

- Python 3.6+
- PostgreSQL
- psycopg2 Python library
- pandas Python library

### Configuration
Before running the ETL scripts, you need to set up the database connection details. Follow these steps to configure your environment:

1. Create a Configuration File:

    - Create a file named `config.ini` in the root directory of the project.

    - Use the following format for the file:
        ```ini
        [Database]
        hostname = localhost
        database = sparkifydb
        username = your_username
        pwd = your_password
        port_id = 5432
        default_database = postgres
        ```
    - Replace `your_username` and `your_password` with your PostgreSQL database username and password.

    - Update `hostname` and `port_id` if different from the default values.

    - `default_database` is used for initial connections, typically set to postgres.

2. Update Database Connection Settings:

    - Ensure that the details provided in `config.ini` file match your database server configurations.

### Running the Scripts

3. Initialize the Database:

    - Run the `create_tables.py` script to set up the database and create necessary tables
        ```bash
        python create_tables.py
        ```
4. Run the ETL process

    - Execute the `etl.py` script to extract, transform, and load the data into the database
        ```bash
        python etl.py
        ```

5. Verify Data Insertion

    - Optionally, you can run `test.ipynb` using Jupyter Notebook to check that the data has been correctly inserted into the database.