import time

from datetime import datetime
from pandas.io import json

import requests
from requests.exceptions import Timeout

import pandas as pd

from pandas import json_normalize

from sqlalchemy import create_engine

#? global ?
raw_data_table_creation = """CREATE TABLE raw_data (
    `id` int NOT NULL AUTO_INCREMENT,
    `country` VARCHAR(255) NOT NULL,
    `country_code` VARCHAR(4) NOT NULL,
    `lat` DOUBLE NOT NULL,
    `lon` DOUBLE NOT NULL,
    `confirmed` BIGINT,
    `deaths` BIGINT, 
    `recovered` BIGINT,
    `active` BIGINT,
    `date` DATE,
    PRIMARY KEY(`id`)
);
"""


countries_table_creation = """CREATE TABLE countries (
    `id` int NOT NULL AUTO_INCREMENT,
    `country` VARCHAR(255) NOT NULL,
    `slug` VARCHAR(255) NOT NULL,
    `iso2` VARCHAR(4) NOT NULL,
    PRIMARY KEY(`id`)
);
"""


def get_country_data(country):
    print(f'LOG: getting country {country} data')

    base_url = 'https://api.covid19api.com' ## open API

    url = f'{base_url}/country/{country}?'

    payload={}
    headers = {}

    try:
        response = requests.request("GET", url, headers=headers, data=payload, timeout=3.0)
    except Timeout:
        print(f'LOG: timeout while requesting country {country} data')
        return pd.DataFrame()

    # json_response = response.json()
    try:
        json_data = json.loads(response.text)
    except ValueError as err:
        print(f'LOG: error while loading json country {country} data ({err})')
        return pd.DataFrame()

    df = json_normalize(json_data)

    if(df.empty or response.status_code != 200): #!Todo some status_code deserves other treatments
        print(f'LOG: error in request country {country} data')
        return pd.DataFrame()

    rename = {
        'Country' : 'country',
        'CountryCode': 'country_code',
        'Lat': 'lat',
        'Lon': 'lon',
        'Confirmed': 'confirmed',
        'Deaths': 'deaths', 
        'Recovered': 'recovered',
        'Active': 'active',
        'Date': 'date',
    }

    columns = ['country', 'country_code', 'lat', 'lon', 'confirmed', 'deaths', 'recovered', 'active', 'date']

    df = df.rename(columns=rename)

    df = df[columns]

    df['date'] = df['date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))

    return df

def get_countries():
    base_url = 'https://api.covid19api.com'

    url = f'{base_url}/countries'

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    df = json_normalize(response.json())

    df.columns = df.columns.to_series().apply(name_to_lower)

    df = df[['country', 'slug', 'iso2']]

    return df

def connect_mysql(pw, database=''):
    import mysql.connector

    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password=pw,
    database=database
    )

    return mydb

def alchemy_engine(pw):
    return create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
        .format(user="root",
                pw=pw,
                db="covid_data"))

def list_databases(mydb):
    mycursor = mydb.cursor()

    mycursor.execute("SHOW DATABASES;")

    return [database[0] for database in mycursor]

def list_tables(mydb):
    mycursor = mydb.cursor()

    mycursor.execute("SHOW TABLES;")

    return [database[0] for database in mycursor]

def create_database(mydb, database_name):
    print(f'LOG: creating database {database_name}')
    mydb.cursor().execute(f"CREATE DATABASE {database_name};")

def create_table(mydb, table_name, sql):
    print(f'LOG: creating table {table_name}')
    mydb.cursor().execute(sql)

def df_to_sql(df, engine, table_name, country_name='', if_exists='replace'):
    print(f'LOG: sending data to table {table_name} {country_name}')
    df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)

def name_to_lower(string):
    return string.lower()

def get_countries_from_db(mydb, countries_table):
    
    mycursor = mydb.cursor()

    sql = f'SELECT `country` from {countries_table};'

    mycursor.execute(sql)

    countries = mycursor.fetchall()
    
    return [country[0] for country in countries]

def main():
    pw = "rootinho@" #! should be in a config file for security, but now it doesnt matter, its just an invented password

    covid_database_name = 'covid_data'

    countries_table = 'countries'
    raw_data_table = 'raw_data'

    mydb = connect_mysql(pw)
    databases = list_databases(mydb)
    print(f'LOG: database {covid_database_name} already_exists') if covid_database_name in databases else create_database(mydb, covid_database_name)

    mydb.close()

    mydb = connect_mysql(pw, covid_database_name) ## connects to specific database
    tables = list_tables(mydb)
    print(f'LOG: table {countries_table} already exists') if countries_table in tables else create_table(mydb, countries_table, countries_table_creation)
    print(f'LOG: table {raw_data_table} already exists') if raw_data_table in tables else create_table(mydb, raw_data_table, raw_data_table_creation)
    countries = get_countries_from_db(mydb, countries_table)
    mydb.close()

    df_countries = get_countries()

    engine = alchemy_engine(pw)

    df_to_sql(df_countries, engine, countries_table)

    for country in countries:
        df = get_country_data(country)
        if(df.empty == False):
            df_to_sql(df, engine, raw_data_table, country, if_exists='append')
        else:
            print(f'LOG: country {country} has no data')
        del df
        time.sleep(1)

    engine.dispose() # close sqlalchemy engine

if __name__ == "__main__":
    main()