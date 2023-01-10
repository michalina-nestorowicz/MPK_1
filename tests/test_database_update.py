import unittest
import database_updater
import queries
from unittest import mock
import pyfakefs.fake_filesystem_unittest
import pandas as pd
from psycopg2 import sql


class TestUpdater(pyfakefs.fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()
        self.fs.create_dir('\\project')
        self.fs.create_dir('\\project\\csv_files')

    def tearDown(self):
        pass

    def test_read_csv_file_exists_return_df(self):
        test_csv_content = "test1,test2,test3\nvalue1,value2,value3"
        self.fs.create_file('\\project\\csv_files\\Test-example.csv', contents=test_csv_content)
        result = database_updater.get_df_from_csv('\\project\\csv_files', 'Test', 'example')
        self.assertEqual(type(result), pd.DataFrame)

    def test_read_csv_file_exists_called_with_file_path(self):
        test_csv_content = "test1,test2,test3\nvalue1,value2,value3"
        file_path = '\\project\\csv_files\\Test-example.csv'
        self.fs.create_file(file_path, contents=test_csv_content)
        with mock.patch('database_updater.pd.read_csv') as mock_csv:
            database_updater.get_df_from_csv('\\project\\csv_files', 'Test', 'example')
        mock_csv.assert_called_with(file_path, delimiter=',')

    def test_read_csv_file_no_file_raise_error(self):
        with self.assertRaises(OSError) as cm:
            database_updater.get_df_from_csv('\\project\\csv_files', 'Test', 'example')
        self.assertEqual(cm.exception.args, ('Test-example.csv does not exists', ))

    def test_delete_unnecessary_columns_in_csv_return_True(self):
        test_csv_content = "test1,test2,test3\nvalue1,value2,value3"
        file_path = '\\project\\csv_files\\Test-example.csv'
        self.fs.create_file(file_path, contents=test_csv_content)
        result = database_updater.delete_unnecessary_columns_in_csv(file_path, ['test1', 'test2'])
        self.assertTrue(result)
        with open(file_path,encoding='utf-8-sig') as f:
            contents = f.read()
        self.assertEqual("test1,test2\nvalue1,value2\n", contents)

    def test_delete_unnecessary_columns_in_empty_expected_list_return_empty_csv(self):
        test_csv_content = "test1,test2,test3\nvalue1,value2,value3"
        file_path = '\\project\\csv_files\\Test-example.csv'
        self.fs.create_file(file_path, contents=test_csv_content)
        result = database_updater.delete_unnecessary_columns_in_csv(file_path, [])
        self.assertTrue(result)
        with open(file_path, encoding='utf-8-sig') as f:
            contents = f.read()
        self.assertEqual("\n\n", contents)

    def test_delete_unnecessary_columns_no_file_raise_error(self):
        file_path = '\\project\\csv_files\\Test-example.csv'
        with self.assertRaises(OSError) as cm:
            database_updater.delete_unnecessary_columns_in_csv(file_path, ['test1', 'test2'])
        self.assertEqual(cm.exception.args, (f'Error when deleting columns from file {file_path}', ))


class TestSQLDatabase(unittest.TestCase):
    def test_create_temp_table(self):
        test_df = pd.DataFrame({'TEST' : ['value 1', 'value 2', 'value 3']})
        with mock.patch('database_updater.pd.DataFrame.to_sql') as mock_sql:
            result = database_updater.create_temp_table(test_df, 'Test_table')
        mock_sql.assert_called_once_with(name='temp_Test_table', con=mock.ANY, if_exists='replace', index=False)
        self.assertTrue(result)

    def test_get_table_dict_correct_name_return_dict(self):
        correct_name = 'routes'
        result = database_updater.get_table_dict(correct_name)
        expected = {'table_name': 'routes', 'insert_query': queries.INSERT_NEW_RECORDS_ROUTES,
                    'important_columns': ['route_id', 'route_short_name', 'route_desc', 'city_id', 'date']}
        self.assertDictEqual(result, expected)

    def test_get_table_incorrect_name_raise_KeyError(self):
        wrong_name = 'test'
        with self.assertRaises(KeyError) as cm:
            database_updater.get_table_dict(wrong_name)
        self.assertEqual(cm.exception.args, (f'No {wrong_name} table',))

    # I only test if function correctly calls query, I don't test if query is correct and all data is correct

    @mock.patch('database_updater.psycopg2.connect')
    def test_delete_old_records_from_table_return_True(self, mock_connect):
        result = database_updater.delete_old_records_from_table('routes', 1)
        expected_call = mock.call(sql.SQL(queries.DELETE_OLD_RECORDS).format(table_name=sql.Identifier('routes')), (1,))
        mock_connect().__enter__().cursor().__enter__().execute.assert_has_calls([expected_call])
        self.assertTrue(result)

    @mock.patch('database_updater.psycopg2.connect')
    def test_insert_new_records_to_table_return_True(self, mock_connect):
        result = database_updater.insert_new_records_to_table('routes', 1)
        expected_call = mock.call(sql.SQL(queries.INSERT_NEW_RECORDS_ROUTES), (1, ))
        mock_connect().__enter__().cursor().__enter__().execute.assert_has_calls([expected_call])
        self.assertTrue(result)

