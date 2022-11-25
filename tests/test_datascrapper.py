from unittest import mock
import os
from zipfile import ZipFile
from pyfakefs import fake_filesystem_unittest

import dataset_scrapper


class TestDataScrapper(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()
        self.maxDiff = None

    def test_check_correct_dir_in_project(self):
        os.makedirs('/home/mpk/csv_files')
        os.makedirs('/home/mpk/zip_files')

        results = dataset_scrapper.check_correct_dir_in_project('/home/mpk/',
                                                                expected_dirs_list=['csv_files', 'zip_files'])
        self.assertEqual(results, True)

    def test_check_if_expected_dirs_are_created_if_not_exists(self):
        os.makedirs('/home/mpk/')

        with mock.patch('builtins.print') as mocked_print:
            dataset_scrapper.create_expected_dirs('/home/mpk/')
            mocked_print.assert_called_with('Creating directories')

    def test_check_if_expected_dirs_are_created_if_exists(self):
        os.makedirs('/home/mpk/csv_files')
        os.makedirs('/home/mpk/zip_files')

        with mock.patch('builtins.print') as mocked_print:
            dataset_scrapper.create_expected_dirs('/home/mpk/')
            mocked_print.assert_called_with('No directories were created')

    def test_check_csv_age_new_file(self):
        os.makedirs('/home/mpk/csv_files')

        with open('/home/mpk/csv_files/test.csv', 'w') as f:
            f.write('test')

        result = dataset_scrapper.check_csv_age('/home/mpk/')
        self.assertEqual(result, 'file not old')

    def test_check_csv_age_no_file(self):
        os.makedirs('/home/mpk/csv_files')
        result = dataset_scrapper.check_csv_age('/home/mpk/')
        self.assertEqual(result, 'No file')

    def test_check_csv_age_old_file(self):
        os.makedirs('/home/mpk/csv_files')

        with open('/home/mpk/csv_files/test.csv', 'w') as f:
            f.write('test')

        result = dataset_scrapper.check_csv_age('/home/mpk/', days=0)
        self.assertEqual(result, 'file old')

    def test_delete_old_zip_files_no_files(self):
        os.makedirs('/home/mpk/zip_files')

        with mock.patch('builtins.print') as mocked_print:
            result = dataset_scrapper.delete_old_zip_file('/home/mpk/')
            mocked_print.assert_called_with('No .zip files where found')
            self.assertEqual(result, True)

    def test_delete_old_zip_files_new_files(self):
        os.makedirs('/home/mpk/zip_files')
        archive_name = '/home/mpk/zip_files/test_file.zip'
        with ZipFile(archive_name, 'w') as file:
            pass

        with mock.patch('builtins.print') as mocked_print:
            result = dataset_scrapper.delete_old_zip_file('/home/mpk/')

            mocked_print.assert_called_with('File not old enough')
            self.assertEqual(result, False)
            self.assertTrue(os.path.exists(archive_name))

    def test_delete_old_zip_files_old_files(self):
        os.makedirs('/home/mpk/zip_files')
        archive_name = '/home/mpk/zip_files/test_file.zip'
        with ZipFile(archive_name, 'w') as file:
            pass

        with mock.patch('builtins.print') as mocked_print:
            result = dataset_scrapper.delete_old_zip_file('/home/mpk/', days=0)

            # add if mocked_filed is removed
            mocked_print.assert_called_with('File removed')
            self.assertEqual(result, True)
            self.assertFalse(os.path.exists(archive_name))

    def mock_request_get(self, url, **kwargs):
        self.last_called_url = url
        return None

    def test_get_request(self):
        original_get_requests = dataset_scrapper.requests.get
        dataset_scrapper.requests.get = self.mock_request_get

        original_read_json = dataset_scrapper.read_json
        dataset_scrapper.read_json = lambda city_name: {'url': 'test.url'}

        dataset_scrapper.get_request(city_name='wroclaw')
        self.assertEqual(self.last_called_url, 'test.url')

        dataset_scrapper.requests.get = original_get_requests
        dataset_scrapper.read_json = original_read_json

    def test_read_json(self):
        original_json_load = dataset_scrapper.json.load
        dataset_scrapper.json.load = mock_json_load

        dataset_scrapper.open = MockFile

        result = dataset_scrapper.read_json('Wroclaw')
        expected = {"city_id": 1,
                    "city_name": "Wroclaw",
                    "url": "https://www.wroclaw.pl/open-data/dataset/"
                           "rozkladjazdytransportupublicznegoplik_data/resource/62b3f371-2375-4979-874c-05c6bbb9b09e"}

        self.assertDictEqual(result, expected)
        dataset_scrapper.json.load = original_json_load

        dataset_scrapper.open = open

    def test_unzip_files_if_none(self):
        os.makedirs('/home/mpk/zip_files')

        with mock.patch('builtins.print') as mocked_print:
            dataset_scrapper.unzip_files('/home/mpk/', 'test')

        mocked_print.assert_called_with('No .zip files were found')
        self.assertTrue(os.path.exists('/home/mpk/zip_files/unzipped_test'))

    def test_unzip_files_if_exists(self):
        os.makedirs('/home/mpk/zip_files')

        with open('/home/mpk/zip_files/test.txt', 'w') as f:
            f.write('test')

        zipf = ZipFile('/home/mpk/zip_files/test.zip', mode='w')
        zipf.write('/home/mpk/zip_files/test.txt')
        zipf.close()

        dataset_scrapper.unzip_files('/home/mpk/', 'test')

        self.assertTrue(os.path.exists('/home/mpk/zip_files/unzipped_test'))
        self.assertEqual(len(os.listdir('/home/mpk/zip_files/unzipped_test')), 1)

    def test_txt_to_csv_no_file(self):
        os.makedirs('/home/mpk/zip_files/unzipped_Wroclaw')

        with mock.patch('builtins.print') as mocked_print:
            dataset_scrapper.txt_to_csv('/home/mpk/', 'Wroclaw')

        mocked_print.assert_called_with(f'File routes.txt not found')

    def test_txt_to_csv_file_exists(self):
        os.makedirs('/home/mpk/zip_files/unzipped_Wroclaw')
        os.makedirs('/home/mpk/csv_files')

        first_row = 'route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_type2_id'
        with open('/home/mpk/zip_files/unzipped_Wroclaw/routes.txt', 'w') as f:
            f.write(first_row)

        original_json_load = dataset_scrapper.json.load
        dataset_scrapper.json.load = mock_json_load

        dataset_scrapper.open = MockFile

        dataset_scrapper.txt_to_csv('/home/mpk/', 'Wroclaw')

        self.assertTrue(os.path.exists('/home/mpk/csv_files/Wroclaw-routes.csv'))

        dataset_scrapper.json.load = original_json_load
        dataset_scrapper.open = open

        expected = 'route_id;route_short_name;route_desc;city_id;date\n'
        with open('/home/mpk/csv_files/Wroclaw-routes.csv', 'r') as f:
            data = f.readline()

        self.assertEqual(expected, data)


class MockFile:
    def __init__(self, *args):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return True


def mock_json_load(*args):
    return {
        "cities": [
            {
                "city_id": 1,
                "city_name": "Wroclaw",
                "url": "https://www.wroclaw.pl/open-data/dataset/"
                       "rozkladjazdytransportupublicznegoplik_data/resource/62b3f371-2375-4979-874c-05c6bbb9b09e"
            }
        ]
    }
