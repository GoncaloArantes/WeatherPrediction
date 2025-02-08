import os
import pandas as pd
from psycopg2 import Error, connect, sql
from psycopg2.extras import execute_values

# Connect to PostgreSQL
try:
    conn = connect(database="dbname",
                        host="host",
                        user="username",
                        password="password",
                        port="5432"
    )

except Error as e:
       raise ConnectionError('Error while connecting to Postgre: ', e)

# Create a cursor
cur = conn.cursor()

# Gather the dataframe from the CSV file
df = pd.read_csv(os.path.join(os.getcwd(),'WeatherPrediction/weather_df.csv'))

# Rename two columns per PostgreSQL rules
df.rename(columns={'45_chance_rain': 'chance_rain_45', '100_chance_rain': 'chance_rain_100'}, inplace=True)

# Create a table in the database
table_name = 'barcelos_weather_jan'

# Generate the SQL for inserting data
cols = list(df.columns)
col_names = ", ".join(cols)
data = [tuple(row) for row in df.to_numpy()]

create_table_query = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    temp_c FLOAT,
    condition VARCHAR(100),
    wind_kph FLOAT,
    wind_degree INT,
    wind_dir INT,
    pressure_mb FLOAT,
    precip_mm FLOAT,
    humidity INT,
    cloud INT,
    windchill_c FLOAT,
    dewpoint_c FLOAT,
    will_it_rain INT,
    vis_km FLOAT,
    uv FLOAT,
    hour_of_day INT,
    day INT,
    chance_rain_45 INT,
    chance_rain_100 INT
)
'''

insert_query = sql.SQL(f'INSERT INTO "{table_name}" ({col_names}) VALUES %s')

# Create the table
try:
    cur.execute(create_table_query)
    conn.commit()
    print("Table created successfully!")

except Error as e:
    print("Error while creating table:", e)

# Efficiently insert the data
try:
    execute_values(cur, insert_query, data)
    conn.commit()
    print('Data inserted successfully!')

except Error as e:
    print("Error while inserting data:", e)

finally:
    # Close the connection
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")