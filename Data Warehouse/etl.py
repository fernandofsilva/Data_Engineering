import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''

    Load the Redshift staging table data from the source files located on S3

    :param cur: cursor from the Redshift database
    :param conn: connection to the Redshift database

    '''

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''

    Insert the data from the staging tables on Redshift into the fact and dimension tables.

    :param cur: cursor from the Redshift database
    :param conn: connection to the Redshift database

    '''

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
