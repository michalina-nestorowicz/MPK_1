from unittest import TestCase
from app import app
import json
import dataset_scrapper
from test_datascrapper import MockFile, mock_json_load


class TestApp(TestCase):
    def setUp(self):
        self.ctx = app.app_context()
        self.ctx.push()
        self.client = app.test_client

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
        expected = [{"city_id": 1, "city_name": "Wroc\u0142aw"}]
        self.assertEqual(json.loads(r.get_data()), expected)

    def test_get_routes(self):
        original_json_load = dataset_scrapper.json.load
        dataset_scrapper.json.load = mock_json_load
        dataset_scrapper.open = MockFile

        with self.client() as c:
            r = c.get('/routes/Wroclaw')
        self.assertEqual(r.status_code, 200)

        dataset_scrapper.json.load = original_json_load
        dataset_scrapper.open = open

        self.assertEqual(len(json.loads(r.get_data())), 129)
