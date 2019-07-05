#DROP TABLES
users_table_drop = "DROP TABLE IF EXISTS users"
ac_users_table_drop = "DROP TABLE IF EXISTS ac_users"
#CREATE TABLES
users_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                            id SERIAL PRIMARY KEY,
                            user_id int NOT NULL,
                            first_name varchar NOT NULL,
                            last_name varchar NOT NULL,
                            email varchar NOT NULL, 
                            avatar text,
                            time_stamp timestamp);
                      """)
ac_users_table_create = '''CREATE TABLE IF NOT EXISTS ac_users
                                (user_id int,
                                first_name text,
                                last_name  text,
                                email text,
                                avatar text,
                                PRIMARY KEY((user_id), first_name, last_name))'''

#INSERT INTO TABLES
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, email, avatar, time_stamp)
                        VALUES (%s, %s, %s, %s, %s, %s)""")

ac_users_table_insert = '''INSERT INTO ac_users(user_id, first_name, last_name, email, avatar)
                            VALUES(%s, %s, %s, %s,%s)'''


#Aggregation Metrics Queries

fn_agg = ("""SELECT LEFT(first_name, 1) first_char, Count(*)
                 FROM users
                 GROUP BY first_char
                 ORDER BY  first_char ASC;""")
ln_agg = ("""SELECT LEFT(last_name, 1) first_char, Count(*)
                FROM users
                GROUP BY first_char
                ORDER BY first_char ASC """)
#select query
fn_query = """SELECT  first_name as first_char
            FROM ac_users
            """
ln_query = """SELECT  last_name as first_char
            FROM ac_users
            """

drop_table_queries = [users_table_drop]
create_table_queries = [users_table_create]
