import psycopg2
from sql_queries import user_table_insert, fn_agg, ln_agg, fn_query, ln_query
import requests
import functools
import time
import schedule
import cassandra
from cassandra.cluster import Cluster
from sql_queries import ac_users_table_create, ac_users_table_insert


try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=zylotechdb user=student password=student")
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)

try: 
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as e:
    print(e)

try:
    session.set_keyspace('zylotechdb2')
except Exception as e:
    print(e)

try:
    session.execute(ac_users_table_create)
except Exception as e:
    print(e)
# api-endpoint 
URL = "https://reqres.in/api/users?page="
#data requesting from endpoint pages
pages = [1, 2, 3, 4]
def check_user(id1):
#     print("id1: ", isinstance(id1, int))
    if not isinstance(id1, int):
        return False
    try: 
        
        cur.execute("SELECT * FROM users where user_id ='{0}'".format(id1))
        result = cur.fetchall()
        if len(result):
            return False
        else:
            return True
    except psycopg2.Error as e: 
        print("Error: select *")
        print (e)
def ac_aggregation_metrics(query):
    
    fc_dic = {}
    try:
        rows = session.execute(query)
        for row in rows:
#             print(row.first_char)
            fn_temp = row.first_char[0].upper()
            if fn_temp in fc_dic:
                fc_dic[fn_temp] +=1
            else:
                fc_dic[fn_temp] = 1
    except Exception as e:
        print(e)
    return fc_dic
def insert_data(session, query):
    '''
        This function is used to insert records into tables
        args:
            session: holds connection
            query: string, query statement to insert into table.
        return: None
    '''
    for page in pages:
        URL = "https://reqres.in/api/users?page="+str(page)
#         print(URL)
        try:
            res = requests.get(url = URL)
            res_json = res.json()
        except requests.exceptions.HTTPError as e:
            print(e)
        
        if res_json['data']:
            users = res_json['data']
            for user in users:
                id1 = user['id']
                email = user['email']
                first_name = user['first_name']
                last_name = user['last_name']
                avatar = user['avatar']
                session.execute(query, (int(id1), str(first_name), str(last_name), str(email), str(avatar)))
# Count of last name starting with same letter:
def aggregation_metrics(query):
    try: 
        cur.execute(query)
        result = cur.fetchall()
        return result
    except psycopg2.Error as e: 
        print("Error: ", query)
        print (e)
# This decorator can be applied to
def with_logging(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print('LOG: Running job "%s"' % func.__name__)
        result = func(*args, **kwargs)
        print('LOG: Job "%s" completed' % func.__name__)
        return result
    return wrapper

@with_logging
def job():

    for page in pages:
        URL = "https://reqres.in/api/users?page="+str(page)
#         print(URL)
        try:
            res = requests.get(url = URL)
            res_json = res.json()
        except requests.exceptions.HTTPError as e:
            print(e)
        
        rows_affected = 0
        rows_duplicate = 0
        per_page = res_json['per_page']
        if res_json['data']:
            users = res_json['data']
            for user in users:
                id1 = user['id']
                email = user['email']
                first_name = user['first_name']
                last_name = user['last_name']
                avatar = user['avatar']
    #                 print(id1, email, first_name, last_name, avatar)
                values = (id1,  first_name, last_name,email, avatar, "now()")
        
                try:
                    if check_user(id1):
                        cur.execute(user_table_insert, values)
                        rows_affected += 1
                    else:
#                         print("Record already exists in the DB")
                        rows_duplicate += 1
                        pass
                        
                except psycopg2.Error as e: 
                    print("Error: Inserting Rows")
                    print (e) 
            if rows_affected == per_page:
                print("Page = {0}, Status = All records inserted.".format(page))
            else:
                print("Page = {0}, Status = Missing  few records, per_page = {1}, Failed = {2}, Duplicate_rows = {3}"\
                      .format(page, per_page, (per_page-rows_affected), rows_duplicate))
        else:
            print("No users list in the API end point.")
    
    fn_agg_res = aggregation_metrics(fn_agg)
    ln_agg_res = aggregation_metrics(ln_agg)
    print("RDBMS Aggregation Results: ")
    print("First Name: ", end = '')
    for row in fn_agg_res:
        print(row, end = ' ')
    print("\nLast Name: ", end = '')
    for row in ln_agg_res:
        print(row, end = ' ')
    print()


    #NoSQL datamodeling
    
    # INSERT into the table
    insert_data(session, ac_users_table_insert)
    fn_result = ac_aggregation_metrics(fn_query)

    ln_result = ac_aggregation_metrics(ln_query)
    
    print("NoSQl Aggregation Results: ")
    print("First Name:", fn_result)
    print("Last Name:", ln_result)
schedule.every(1).seconds.do(job)
while 1:
    schedule.run_pending()
    time.sleep(1)