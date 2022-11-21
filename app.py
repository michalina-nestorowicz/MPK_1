import os
from dotenv import load_dotenv
from flask import Flask
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import json

import dataset_scrapper

CREATE_ROUTES_TABLE = (
    'CREATE TABLE IF NOT EXISTS routes (route_id VARCHAR(255), route_short_name VARCHAR(255), '
    'route_desc VARCHAR(1000) , FOREIGN KEY(city_id)), date TIMESTAMP;'
)

INSERT_ROUTE_TABLE = (
    'INSERT INTO routes (route_id, route_short_name ,route_desc, city_id, date) VALUES (%s, %s, %s, %s, %s)'
)

UPDATE_ROUTE_TABLE = (
    """ UPDATE routes r
        SET
            route_id = t.route_id,
            route_short_name = t.route_short_name,
            route_desc = t.route_desc,
            date = t.date
        FROM temp_table t
        WHERE r.city_id = %s;
    """
)

GET_ROUTE_TABLE = (
    """
    SELECT route_id,route_short_name,route_desc FROM routes
    WHERE city_id = %s
    """
)

GET_ALL_CITIES = (
    """
    SELECT * FROM cities
    """
)

load_dotenv()
app = Flask(__name__)
url = os.getenv("DATABASE_URL")
url_alchemy = os.getenv("SQLALCHEMY_URL")
engine = create_engine(url_alchemy)
connection = psycopg2.connect(url)
current_cwd = os.getcwd()


def update_sql_table(cwd, city_name):
    path_csv = f'{cwd}\\csv_files'
    file = [f for f in os.listdir(path_csv) if f.startswith(city_name)][0]
    # print(file)

    df = pd.read_csv(os.path.join(path_csv, file), encoding='ansi', sep=';')
    df.to_sql(name='temp_table', con=engine, if_exists='replace', index=False, index_label='route_id')

    with open('cities.json') as f:
        data = json.load(f)
        # print(cities)
    cities = data['cities']
    city = [x for x in cities if x.get('city_name') == city_name][0]
    city_id = city['city_id']

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(UPDATE_ROUTE_TABLE, (city_id,))


def get_dataset(cwd, city_name):
    # check if .csv file exist and how old are they
    check = dataset_scrapper.check_csv_age(cwd)

    if check == 'file not old':
        update_sql_table(cwd, city_name)
    elif check == 'file old':
        old_deleted = dataset_scrapper.delete_old_zip_file(cwd)
        dataset_scrapper.delete_txt_files(cwd)

        if old_deleted:
            is_downloaded = dataset_scrapper.dataset_download(cwd, city_name)
            if is_downloaded:
                dataset_scrapper.unzip_files(cwd, city_name)
                dataset_scrapper.txt_to_csv(cwd, city_name, table_name='routes')

        update_sql_table(cwd, city_name)

    else:
        dataset_scrapper.delete_txt_files(cwd)
        is_downloaded = dataset_scrapper.dataset_download(cwd, city_name)

        if is_downloaded:
            dataset_scrapper.unzip_files(cwd, city_name)
            dataset_scrapper.txt_to_csv(cwd, city_name, table_name='routes')

        update_sql_table(cwd, city_name)


@app.get("/mpk")
def get_cities():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(GET_ALL_CITIES)
            r = [dict((cursor.description[i][0], value)
                      for i, value in enumerate(row)) for row in cursor.fetchall()]
    return {"cities": r}


@app.get("/routes/<string:city_name>")
def get_city_routes(city_name):

    with open('cities.json') as f:
        data = json.load(f)
        # print(cities)
    cities = data['cities']
    city = [x for x in cities if x.get('city_name') == city_name][0]
    city_id = city['city_id']

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(GET_ROUTE_TABLE, (city_id,))
                r = [dict((cursor.description[i][0], value)
                          for i, value in enumerate(row)) for row in cursor.fetchall()]
        return {"routes": r}

    except KeyError:
        return{"message": "City not found"}, 404


# update Wroclaw routes
wd = os.getcwd()
get_dataset(wd, 'Wroclaw')
