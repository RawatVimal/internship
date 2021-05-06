import pymongo
import pprint
import json, sys
import pandas as pd
import csv
from pymongo import MongoClient
import datetime
import psycopg2 as psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE
from psycopg2 import sql

###Check connection

def create_connection():

    conn = psycopg2.connect(dbname='test',

                           user='postgres',

                           host='localhost',

                           port= '5432',

                           password='secret')

    print(conn)
    return conn



def create_tables():

    try:
        conn = create_connection();
        cur = conn.cursor()
        query = 'CREATE TABLE profiles (user_id INTEGER PRIMARY KEY, public text, completion_percentage text, gender text, AGE INTEGER, ' \
                'eye_color text, hair_color text, hair_type text, sign_in_zodiac text, region text, last_login text,' \
                ' registration text, body text, I_am_working_in_field text, I_most_enjoy_good_food text, hobbies text,' \
                ' spoken_languages text, pets text, body_type text, my_eyesight text, completed_level_of_education text,' \
                ' favourite_color text, relation_to_smoking text, relation_to_alcohol text, on_pokec_i_am_looking_for text,' \
                ' love_is_for_me text, relation_to_casual_sex text, my_partner_should_be text, marital_status text,' \
                ' children text, relation_to_children text, I_like_movies text, I_like_watching_movie text,' \
                ' I_like_music text, I_mostly_like_listening_to_music text, the_idea_of_good_evening text,' \
                ' I_like_specialties_from_kitchen text, fun text, I_am_going_to_concerts text, my_active_sports text,' \
                ' my_passive_sports text, profession text, I_like_books text, life_style text, music text, cars text,' \
                ' politics text, relationships text, art_culture text, hobbies_interests text, science_technologies text,' \
                ' computers_internet text, education text, sport text, movies text, travelling text, health text,' \
                ' companies_brands text, more text ); CREATE TABLE relations (_from text, _to text);'


        init_time = datetime.datetime.now()
        cur.execute(query)
        end_time = datetime.datetime.now()
        exec_time =  end_time - init_time
        print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

        # close communication with the PostgreSQL database server

        cur.close()

        # commit the changes

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()



def drop_tables(table_name):

    try:
        conn = create_connection();
        cur = conn.cursor()
        query = "DROP TABLE " + table_name + ';'

        init_time = datetime.datetime.now()
        cur.execute(query)
        end_time = datetime.datetime.now()
        exec_time = end_time - init_time
        print('exec_time = {} Microseconds '.format( exec_time.microseconds))

        # close communication with the PostgreSQL database server

        cur.close()

        # commit the changes

        conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def Insert_INTO_profiles_table():

    try:
        #use Python's open() function to load the JSON data
        with open('soc-pokec-profiles500.json',errors='ignore') as json_data:
            data = json.load(json_data)
            query_sql = """ insert into profiles
                    select * from json_populate_recordset(NULL::profiles, %s) """

            conn = create_connection();
            cur = conn.cursor()

            init_time = datetime.datetime.now()
            cur.execute(query_sql, (json.dumps(data),))
            end_time = datetime.datetime.now()
            exec_time =  end_time - init_time
            print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

            conn.commit()

            print ('\nfinished INSERT INTO execution')
            cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def Insert_INTO_relations_table():

    try:
        #use Python's open() function to load the JSON data
        with open('soc-pokec-relationship5000.json',errors='ignore') as json_data:
            data = json.load(json_data)
            query_sql = """ insert into relations
                    select * from json_populate_recordset(NULL::relations, %s) """

            conn = create_connection();
            cur = conn.cursor()

            init_time = datetime.datetime.now()
            cur.execute(query_sql, (json.dumps(data),))
            end_time = datetime.datetime.now()
            exec_time =  end_time - init_time
            print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

            conn.commit()

            print ('\nfinished INSERT INTO execution')
            cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()

def Read_profiles_table():

    try:
        select_profiles_query = "SELECT * FROM profiles"
        conn = create_connection();
        cur = conn.cursor()

        init_time = datetime.datetime.now()
        cur.execute(select_profiles_query)
        end_time = datetime.datetime.now()
        exec_time =  end_time - init_time
        print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

        profiles = cur.fetchall()

        print("Table contents after insertion ::")


        print(profiles)

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def Read_relationship_table():

    try:
        select_relationship_query = "SELECT * FROM relations"
        conn = create_connection();
        cur = conn.cursor()

        init_time = datetime.datetime.now()
        cur.execute(select_relationship_query)
        end_time = datetime.datetime.now()
        exec_time =  end_time - init_time
        print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

        relations = cur.fetchall()

        print("Table contents after insertion ::")


        print(relations)

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def aggregate():

    try:
        aggregate_query = "select AGE, count(*) from profiles group by AGE"
        conn = create_connection()
        cur = conn.cursor()

        init_time = datetime.datetime.now()
        cur.execute(aggregate_query)
        end_time = datetime.datetime.now()
        exec_time =  end_time - init_time
        print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

        age = cur.fetchall()

        print("Aggregate of AGE ::")

        print(age)

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()

def neighbors():

    try:
        neighbors_query = "SELECT DISTINCT _to FROM relations WHERE _from = '1' limit 100"
        conn = create_connection()
        cur = conn.cursor()

        init_time = datetime.datetime.now()
        cur.execute(neighbors_query)
        end_time = datetime.datetime.now()
        exec_time =  end_time - init_time
        print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

        relations = cur.fetchall()

        print("Neighbours of 1 ::")


        print(relations)

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def neighbors2():

    try:
        neighbors2_query = "select _to from relations where _from = '15' union distinct select _to from relations" \
                          " where _to != '15' and _from in (select  _to from relations where _from = '15')"
        conn = create_connection()
        cur = conn.cursor()

        init_time = datetime.datetime.now()
        cur.execute(neighbors2_query)
        end_time = datetime.datetime.now()
        exec_time =  end_time - init_time
        print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

        relations = cur.fetchall()

        print("Neighbours2 of 15 ::")

        print(relations)

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()

def neighbors2data():

    try:
        neighbors2data_query = "select * from profiles where user_id::text IN (select _to from relations where _from = '20' union" \
                           " distinct select _to from relations where _to != '20' and _from IN" \
                           " (select  _to from relations where _from = '20'))"
        conn = create_connection()
        cur = conn.cursor()

        init_time = datetime.datetime.now()
        cur.execute(neighbors2data_query)
        end_time = datetime.datetime.now()
        exec_time =  end_time - init_time
        print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

        relations = cur.fetchall()

        print("Neighbors2data of 20 ::")

        print(relations)

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


if __name__ == "__main__":

    #create_tables()

    #drop_tables('profiles')

    #drop_tables('relations')

    #Insert_INTO_profiles_table()

    #Insert_INTO_relations_table()

    #Read_profiles_table()

    #Read_relationship_table()

    #neighbors()

    neighbors2()

    #neighbors2data()

    #aggregate()