import configparser
import psycopg2
import boto3
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Function to drop the tables
      Parameters: 
            cur:  cursor variable
            conn : connection string
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Function to create the tables
      Parameters: 
            cur:  cursor variable
            conn : connection string
    """    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Function main() - Program execution begins here
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST,DWH_DB,DWH_DB_USER,DWH_DB_PASSWORD,DWH_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()