import psycopg2
from cassandra.cluster import Cluster
from sql_queries import drop_table_queries, create_table_queries, ac_users_table_drop

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
    '''
        This function is used to crate a NoSQL database.
        return: None
    '''
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
        return session, cluster
    except Exception as e:
        print(e)
        
def drop_tables(cur, conn):
    '''
        This function is used to drop the tables in RDBMS
        args:
            cur: cursor connection
            conn: database connection
        return: None
    '''
    for query  in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Issue in droping table")
            print (e)
def ac_drop_tables(session, query):
    '''
        This function is used to drop tables in NoSQL database
        args:
            session: holds connection
            query: string, query statement to drop table.
        return: None
    '''
    try:
        res = session.execute(query)
    except Exception as e:
        print(e)
def create_tables(cur, conn):
    '''
        This function is used to create tables
        args:
            cur: cursor connection
            conn: database connection
        return: None
    '''
    for query  in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Issue in creating table")
            print (e)
        
    
def main():
    cur, conn = create_database()
    session, cluster = ac_create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    ac_drop_tables(session, ac_users_table_drop)
    
    conn.close()
    session.shutdown()
    cluster.shutdown()
if __name__ == "__main__":
    main()