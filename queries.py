
UPDATE_ROUTE_TABLE = """ 
    UPDATE routes r
        SET
            route_id = t.route_id,
            route_short_name = t.route_short_name,
            route_desc = t.route_desc,
            date = t.date
        FROM temp_table t
        WHERE r.city_id = %s;
    """

DELETE_OLD_RECORDS ="DELETE FROM {table_name} where city_id = %s"


INSERT_NEW_RECORDS_ROUTES = """
    INSERT INTO routes(route_id,route_short_name,route_desc,city_id,date)
    SELECT route_id, route_short_name, route_desc, city_id, date
    FROM temp_routes
    WHERE temp_routes.city_id = %s
    """


INSERT_NEW_RECORDS_STOP_TIMES = """
    INSERT INTO stop_times(trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type,city_id,date)
    SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, city_id, date
    FROM temp_stop_times
    WHERE temp_stop_times.city_id = %s
    """

INSERT_NEW_RECORDS_STOPS = """
    INSERT INTO stops(stop_id,stop_code,stop_name,stop_lat,stop_lon,city_id,date)
    SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon, city_id, date
    FROM temp_stops
    WHERE temp_stops.city_id = %s
    """

INSERT_NEW_RECORDS_TRIPS = """
    INSERT INTO trips (route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,city_id,date) 
    SELECT route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, city_id, date
    FROM temp_trips
    WHERE temp_trips.city_id = %s
    """

GET_ROUTE_TABLE = """
    SELECT route_id,route_short_name,route_desc FROM routes
    WHERE city_id = %s
    """

GET_TRIPS_TABLE = """
    SELECT * FROM trips
    WHERE city_id = %s
    """

GET_STOPS_TABLE = """
    SELECT * FROM stops
    WHERE city_id = %s
    """

GET_STOP_TIMES_TABLE = """
    SELECT * FROM stop_times
    WHERE city_id = %s
    """


GET_ALL_CITIES = """
    SELECT city_id, city_name FROM cities
    """


TABLE_LIST = [{'table_name': 'routes', 'insert_query': INSERT_NEW_RECORDS_ROUTES,
               'important_columns': ['route_id', 'route_short_name', 'route_desc', 'city_id', 'date']},
              {'table_name': 'stop_times', 'insert_query': INSERT_NEW_RECORDS_STOP_TIMES,
               'important_columns': ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence',
                                     'pickup_type', 'drop_off_type', 'city_id', 'date']},
              {'table_name': 'stops', 'insert_query': INSERT_NEW_RECORDS_STOPS,
               'important_columns': ['stop_id', 'stop_code', 'stop_name', 'stop_lat', 'stop_lon', 'city_id', 'date']},
              {'table_name': 'trips', 'insert_query': INSERT_NEW_RECORDS_TRIPS,
               'important_columns': ['route_id', 'service_id', 'trip_id', 'trip_headsign', 'direction_id', 'shape_id',
                                     'city_id', 'date']}]


