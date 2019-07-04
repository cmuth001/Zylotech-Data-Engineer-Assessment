import psycopg2
from cassandra.cluster import Cluster
from sql_queries import drop_table_queries, create_table_queries

def create_database():
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create zylotechdb database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS zylotechdb")
    cur.execute("CREATE DATABASE zylotechdb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to zylotech database
    conn = psycopg2.connect("host=127.0.0.1 dbname=zylotechdb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn
def ac_create_database(): 
    try: 
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
    except Exception as e:
        print(e)
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS zylotechdb2 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
        )
    except Exception as e:
        print(e)
def drop_tables(cur, conn):
    #code
    for query  in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Issue in droping table")
            print (e)
        
def create_tables(cur, conn):
    #code
    for query  in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Issue in creating table")
            print (e)
        
    
def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()