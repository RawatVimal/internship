import pprint
import psycopg2
import psycopg2 as psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import AsIs
import os

# creating connection to the database

my_con = psycopg2.connect(
            dbname='test',
            user='postgres',
            host='localhost',
            port='5432',
            password='secret'
)

print(my_con)

# Creating cursor object to execute queries

my_con.autocommit = True
cursor = my_con.cursor()

# Create table inside our database

create_table_query = """
CREATE TABLE IF NOT EXISTS cars (
id SERIAL PRIMARY KEY,
name TEXT NOT NULL,
model INTEGER,
number TEXT,
color TEXT,
company TEXT
);
"""

cursor.execute(create_table_query)

# Insert records into the table

cars = [
    ("Aqua", 2009, "ABC123", "Red", "Toyota"),
    ("700s", 2015, "XXXX22", "Black", "BMW"),
    ("Vezel", 2018, "XXX111", "White", "Honda"),
    ("200C", 2001, "MMMM11", "Black", "Mercedez"),
    ("Vitz", 2010, "XXXX", "Red", "Toyota"),
]

car_records = ", ".join(["%s"] * len(cars))

insert_query = (
    f"INSERT INTO cars (name, model, number, color, company) VALUES {car_records}"
)

cursor.execute(insert_query, cars)

# reading the records from the database table

select_cars_query = "SELECT * FROM cars"
cursor.execute(select_cars_query)

cars = cursor.fetchall()

print("Table contents after insertion ::")

for car in cars:
    print(car)


# updating record in a table

update_car_colors = """
UPDATE
cars
SET
color = 'Blue'
WHERE
model >= 2010
"""

cursor.execute(update_car_colors)

cursor.execute(select_cars_query)

cars = cursor.fetchall()

print("Table contents after upadating records ::")

for car in cars:
    print(car)

# deleting records from table

delete_car_records = "DELETE FROM cars WHERE color = 'Red'"

cursor.execute(delete_car_records)

cursor.execute(select_cars_query)

cars = cursor.fetchall()

print("Table contents after deleting specific records ::")

for car in cars:
    print(car)

# closing database connection

my_con.close()
