import os
from dotenv import load_dotenv
from flask import Flask
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import json
import dataset_scrapper
import queries

load_dotenv()
app = Flask(__name__)
url = os.getenv("DATABASE_URL")
url_alchemy = os.getenv("SQLALCHEMY_URL")
engine = create_engine(url_alchemy)
current_cwd = os.getcwd()


def get_city_dict(city_name) -> dict:
    data = dataset_scrapper.read_json(current_cwd)
    for city in data:
        if city['city_name'] == city_name:
            return city


@app.get("/mpk/")
def get_cities():
    with psycopg2.connect(url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL(queries.GET_ALL_CITIES))
            r = []
            column = [column[0] for column in cursor.description]
            for row in cursor.fetchall():
                r.append(dict(zip(column, row)))
    return json.dumps(r), 200


@app.get("/routes/<string:city_name>")
def get_city_routes(city_name):
    city = get_city_dict(city_name)
    city_id = city['city_id']
    try:
        with psycopg2.connect(url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL(queries.GET_ROUTE_TABLE), (city_id,))
                r = []
                column = [column[0] for column in cursor.description]
                for row in cursor.fetchall():
                    r.append(dict(zip(column, row)))
        return json.dumps(r), 200
    except KeyError:
        return {"message": f"City {city_name} with table routes not found"}, 404


@app.get("/trips/<string:city_name>")
def get_city_trips(city_name):
    city = get_city_dict(city_name)
    city_id = city['city_id']
    try:
        with psycopg2.connect(url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL(queries.GET_TRIPS_TABLE), (city_id,))
                r = []
                column = [column[0] for column in cursor.description]
                for row in cursor.fetchall():
                    r.append(dict(zip(column, row)))
        return json.dumps(r), 200
    except KeyError:
        return {"message": f"City {city_name} with table trips not found"}, 404


@app.get("/stops/<string:city_name>")
def get_city_stops(city_name):
    city = get_city_dict(city_name)
    city_id = city['city_id']
    try:
        with psycopg2.connect(url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL(queries.GET_STOPS_TABLE), (city_id,))
                r = []
                column = [column[0] for column in cursor.description]
                for row in cursor.fetchall():
                    r.append(dict(zip(column, row)))
        return json.dumps(r), 200
    except KeyError:
        return {"message": f"City {city_name} with table stops not found"}, 404


@app.get("/stop_times/<string:city_name>")
def get_city_stop_times(city_name):
    city = get_city_dict(city_name)
    city_id = city['city_id']
    try:
        with psycopg2.connect(url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL(queries.GET_STOP_TIMES_TABLE), (city_id,))
                r = []
                column = [column[0] for column in cursor.description]
                for row in cursor.fetchall():
                    r.append(dict(zip(column, row)))
        return json.dumps(r), 200
    except KeyError:
        return {"message": f"City {city_name} with table stop_times not found"}, 404


@app.get('/')
def home():
    return {'message': 'Hello, world!'}


if __name__ == '__main__':
    app.run(debug=True)
    