import os
from dotenv import load_dotenv
from flask import Flask
import psycopg2
from psycopg2 import sql
import pandas as pd
from sqlalchemy import create_engine
import json

import dataset_scrapper_old_2


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

DELETE_OLD_RECORDS = sql.SQL(
    "DELETE FROM {table_name} where city_id = %s"
)

INSERT_NEW_RECORDS_ROUTES = sql.SQL(
    """
    INSERT INTO routes(route_id, route_short_name, route_desc, city_id, date)
    SELECT route_id, route_short_name, route_desc, city_id, date
    FROM temp_routes
    WHERE temp_routes.city_id = %s
    """
)

INSERT_NEW_RECORDS_STOP_TIMES = sql.SQL(
    """
    INSERT INTO stop_times(trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, city_id, date)
    SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, city_id, date
    FROM temp_stop_times
    WHERE temp_stop_times.city_id = %s
    """
)

INSERT_NEW_RECORDS_STOPS = sql.SQL(
    """
    INSERT INTO stops(stop_id, stop_code, stop_name, stop_lat, stop_lon, city_id, date)
    SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon, city_id, date
    FROM temp_stops
    WHERE temp_stops.city_id = %s
    """
)


INSERT_NEW_RECORDS_TRIPS = sql.SQL(
    """
    INSERT INTO trips (route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, brigade_id, vehicle_id, variant_id, city_id, date) 
    SELECT route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, brigade_id, vehicle_id, variant_id, city_id, date
    FROM temp_trips
    WHERE temp_trips.city_id = %s
    """
)


GET_ROUTE_TABLE = sql.SQL(
    """
    SELECT route_id,route_short_name,route_desc FROM routes
    WHERE city_id = %s
    """
)

GET_ALL_CITIES = sql.SQL(
    """
    SELECT city_id, city_name FROM cities
    """
)

load_dotenv()
app = Flask(__name__)
url = os.getenv("DATABASE_URL")
url_alchemy = os.getenv("SQLALCHEMY_URL")
engine = create_engine(url_alchemy)
current_cwd = os.getcwd()


def create_sql_temp(cwd, city_name, table_name='routes'):
    path_csv = f'{cwd}\\csv_files'
    files = [f for f in os.listdir(path_csv) if f.startswith(city_name)]
    file = [e for e in files if table_name in e][0]
    df = pd.read_csv(os.path.join(path_csv, file), encoding='utf-8', sep=';')
    name = f'temp_{table_name}'
    df.to_sql(name=name, con=engine, if_exists='replace', index=False)


def update_sql_table(cwd, city_name, table_name='routes'):
    create_sql_temp(cwd, city_name)

    city = dataset_scrapper.read_json(city_name)
    city_id = city['city_id']
    
    if table_name == 'routes':
        insert_query = INSERT_NEW_RECORDS_ROUTES
    elif table_name == 'stop_times':
        insert_query = INSERT_NEW_RECORDS_STOP_TIMES
    elif table_name == 'stops':
        insert_query = INSERT_NEW_RECORDS_STOPS
    elif table_name == 'trips':
        insert_query = INSERT_NEW_RECORDS_TRIPS
    else:
        insert_query = INSERT_NEW_RECORDS_ROUTES

    with psycopg2.connect(url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(DELETE_OLD_RECORDS.format(
                table_name=sql.Identifier(table_name)), (city_id,))
            
            cursor.execute(insert_query, (city_id,))


def update_all_tables(cwd, city_name):
    update_sql_table(cwd, city_name, table_name='routes')
    update_sql_table(cwd, city_name, table_name='stop_times')
    update_sql_table(cwd, city_name, table_name='stops')
    update_sql_table(cwd, city_name, table_name='trips')


# test get_dataset when files are old! some weird error
def get_dataset(cwd, city_name) -> None:
    dataset_scrapper.create_expected_dirs(cwd)
    check = dataset_scrapper.check_csv_age(cwd)

    if check == 'file not old':
        update_all_tables(cwd, city_name)
    elif check == 'file old':
        old_deleted = dataset_scrapper.delete_old_zip_file(cwd)
        dataset_scrapper.delete_txt_files(cwd, city_name)

        if old_deleted:
            is_downloaded = dataset_scrapper.dataset_download(cwd, city_name)
            if is_downloaded:
                dataset_scrapper.unzip_files(cwd, city_name)
                dataset_scrapper.txt_to_csv(cwd, city_name, table_name='routes')
                dataset_scrapper.txt_to_csv(cwd, city_name, table_name='stop_times')
                dataset_scrapper.txt_to_csv(cwd, city_name, table_name='trips')
                dataset_scrapper.txt_to_csv(cwd, city_name, table_name='stops')

        update_all_tables(cwd, city_name)

    else:
        dataset_scrapper.delete_txt_files(cwd, city_name)
        is_downloaded = dataset_scrapper.dataset_download(cwd, city_name)

        if is_downloaded:
            dataset_scrapper.unzip_files(cwd, city_name)
            dataset_scrapper.txt_to_csv(cwd, city_name, table_name='routes')
            dataset_scrapper.txt_to_csv(cwd, city_name, table_name='stop_times')
            dataset_scrapper.txt_to_csv(cwd, city_name, table_name='trips')
            dataset_scrapper.txt_to_csv(cwd, city_name, table_name='stops')

        update_all_tables(cwd, city_name)


@app.get("/mpk/")
def get_cities():
    with psycopg2.connect(url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(GET_ALL_CITIES)
            r = []
            column = [column[0] for column in cursor.description]
            for row in cursor.fetchall():
                r.append(dict(zip(column, row)))
    return json.dumps(r), 200


@app.get("/routes/<string:city_name>")
def get_city_routes(city_name):
    city = dataset_scrapper.read_json(city_name)
    city_id = city['city_id']

    try:
        with psycopg2.connect(url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(GET_ROUTE_TABLE, (city_id,))
                r = []
                column = [column[0] for column in cursor.description]
                for row in cursor.fetchall():
                    r.append(dict(zip(column, row)))

        return json.dumps(r), 200

    except KeyError:
        return {"message": "City not found"}, 404


@app.get('/')
def home():
    return {'message': 'Hello, world!'}


if __name__ == '__main__':
    print('App has started')
    wd = os.getcwd()
    get_dataset(wd, 'Wroclaw')
    app.run(debug=True)
    