import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
Process load S3 data to stage table, read sql command variables from sql_queries to execute query. 
"""  
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("Query:", query)
        cur.execute(query)
        conn.commit()

"""
Process query stage table data insert to dimension table and fact table, 
read sql command variables from sql_queries to execute query. 
"""  
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("Query:", query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()