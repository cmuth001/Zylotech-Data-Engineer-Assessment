import psycopg2
from sql_queries import drop_table_queries, user_table_insert, create_table_queries

def create_database():
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS zylotechdb")
    cur.execute("CREATE DATABASE zylotechdb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=zylotechdb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur, conn):
    #code
    for query  in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    #code
    print("Table Created")
    for query  in create_table_queries:
        cur.execute(query)
        conn.commit()
    
def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()