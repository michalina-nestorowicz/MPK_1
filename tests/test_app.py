from unittest import TestCase

import pandas


import app
import json
import dataset_scrapper
from test_datascrapper import MockFile, mock_json_load
from pyfakefs import fake_filesystem_unittest
import os
from unittest import mock
from psycopg2 import sql


class TestApp(TestCase):
    def setUp(self):
        self.ctx = app.app.app_context()
        self.ctx.push()
        self.client = app.app.test_client

    def tearDown(self):
        self.ctx.pop()

    def test_home(self):
        with self.client() as c:
            r = c.get('/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(json.loads(r.get_data()), {'message': 'Hello, world!'})

    def test_get_cities(self):
        with self.client() as c:
            r = c.get('/mpk/')
        self.assertEqual(r.status_code, 200)
        result = json.loads(r.get_data())[0]
        result_keys = list(result.keys())

        expected = ["city_id", "city_name"]
        self.assertListEqual(result_keys, expected)

    def test_get_routes(self):
        original_json_load = dataset_scrapper.json.load
        dataset_scrapper.json.load = mock_json_load
        dataset_scrapper.open = MockFile

        with self.client() as c:
            r = c.get('/routes/Wroclaw')
        self.assertEqual(r.status_code, 200)

        dataset_scrapper.json.load = original_json_load
        dataset_scrapper.open = open

        result = json.loads(r.get_data())[0]
        result_keys = list(result.keys())
        expected = ["route_id", "route_short_name", "route_desc"]

        self.assertListEqual(result_keys, expected)


class TestDataScrapper(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def mock_to_sql(self, name, **kwargs):
        self.last_called_name = name
        return None

    def test_create_sql_temp(self):
        os.makedirs('/home/mpk/csv_files')
        first_row = 'route_id;route_short_name;route_desc,'

        with open('/home/mpk/csv_files/Wroclaw-routes.csv', 'w') as f:
            f.write(first_row)

        original_to_sql = pandas.DataFrame.to_sql
        pandas.DataFrame.to_sql = self.mock_to_sql

        app.create_sql_temp('/home/mpk/', 'Wroclaw')
        self.assertEqual(self.last_called_name, 'temp_routes')

        pandas.DataFrame.to_sql = original_to_sql

    def mock_create_sql_temp(self, *args, **kwargs):
        return


    @mock.patch('psycopg2.connect')
    def test_update_sql_table(self, mock_connect):
        os.makedirs('/home/mpk')
        original_json_load = dataset_scrapper.json.load
        dataset_scrapper.json.load = mock_json_load
        dataset_scrapper.open = MockFile

        original_create_sql_temp = app.create_sql_temp
        app.create_sql_temp = self.mock_create_sql_temp

        app.update_sql_table('/home/mpk', 'Wroclaw', 'routes')
        expected = []
        expected_delete = mock.call(app.DELETE_OLD_RECORDS.format(table_name=sql.Identifier('routes')), (1,))
        expected_insert = mock.call(app.INSERT_NEW_RECORDS_ROUTES, (1,))

        expected.append(expected_delete)
        expected.append(expected_insert)

        mock_connect().__enter__().cursor().__enter__().execute.assert_has_calls(expected)

        dataset_scrapper.json.load = original_json_load
        dataset_scrapper.open = open
        app.create_sql_temp = original_create_sql_temp









