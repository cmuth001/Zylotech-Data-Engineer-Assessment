#DROP TABLES
users_table_drop = "DROP TABLE IF EXISTS users"

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

#INSERT INTO TABLES
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, email, avatar, time_stamp)
                        VALUES (%s, %s, %s, %s, %s, %s)""")


#Aggregation Metrics Queries


drop_table_queries = [users_table_drop, ]
create_table_queries = [users_table_create]
