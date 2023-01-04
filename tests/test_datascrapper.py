import dataset_scrapper

from unittest import mock
import unittest
import os
from pyfakefs.fake_filesystem_unittest import TestCase
from bs4 import BeautifulSoup


# class with test where requests are needed
class TestWebsite(unittest.TestCase):
    def test_get_website_correct_website_return_response_object(self):
        correct_url = 'https://www.google.com'
        result = dataset_scrapper.get_website(correct_url)
        self.assertEqual(result.status_code, 200)

    def test_get_website_wrong_website_return_None(self):
        test_url = 'wrong_url'
        result = dataset_scrapper.get_website(test_url)
        self.assertIsNone(result)

    def test_check_returned_config_is_correct_file_correct_return_true(self):
        original_load = dataset_scrapper.read_json
        dataset_scrapper.read_json = mock_json_load
        result = dataset_scrapper.check_returned_config_is_correct('\\project', 'cities.json')
        self.assertTrue(result)
        dataset_scrapper.read_json = original_load

    def test_check_returned_config_not_correct_keys_return_false(self):
        original_load = dataset_scrapper.read_json
        dataset_scrapper.read_json = mock_json_wrong_load
        result = dataset_scrapper.check_returned_config_is_correct('\\project', 'cities.json')
        self.assertFalse(result)
        dataset_scrapper.read_json = original_load


# Class testing file functions
class TestDatasetScrapperFiles(TestCase):
    def setUp(self):
        self.setUpPyfakefs()
        self.fs.create_dir('\\project')
        self.json_content = """"[
{
    "city_id": 1,
    "city_name": "Wroclaw",
    "url": "https://www.wroclaw.pl/open-data/dataset/rozkladjazdytransportupublicznegoplik_data/
    resource/62b3f371-2375-4979-874c-05c6bbb9b09e",
    "direct_link": false
  }
]"""

    def tearDown(self):
        pass

    def test_check_correct_dir_in_project_if_all_dirs_exists_assert_true(self):
        dir_path_1 = "\\project\\test_1"
        dir_path_2 = "\\project\\test_2"
        test_list = ['test_1', 'test_2']
        self.assertFalse(os.path.exists(dir_path_1))
        self.assertFalse(os.path.exists(dir_path_2))
        self.fs.create_dir(dir_path_1)
        self.fs.create_dir(dir_path_2)
        self.assertTrue(os.path.exists(dir_path_1))
        self.assertTrue(os.path.exists(dir_path_2))

        result = dataset_scrapper.check_correct_dir_in_project('\\project', test_list)
        self.assertTrue(result)

    def test_check_correct_dir_in_project_if_not_all_dirs_exists_assert_false(self):
        test_list = ['test_1', 'test_2']
        self.assertFalse(os.path.exists("\\project\\test_1"))
        self.fs.create_dir("\\project\\test_1")
        self.assertTrue(os.path.exists("\\project\\test_1"))
        self.assertFalse(os.path.exists("\\project\\test_2"))

        result = dataset_scrapper.check_correct_dir_in_project('\\project', test_list)
        self.assertFalse(result)

    def test_check_correct_dir_in_project_if_more_dirs_exists_assert_true(self):
        test_list = ['test_1', 'test_2']
        self.assertFalse(os.path.exists("\\project\\test_1"))
        self.assertFalse(os.path.exists("\\project\\test_2"))
        self.assertFalse(os.path.exists("\\project\\test_3"))
        self.fs.create_dir("\\project\\test_1")
        self.fs.create_dir("\\project\\test_2")
        self.fs.create_dir("\\project\\test_3")
        self.assertTrue(os.path.exists("\\project\\test_1"))
        self.assertTrue(os.path.exists("\\project\\test_2"))
        self.assertTrue(os.path.exists("\\project\\test_3"))

        result = dataset_scrapper.check_correct_dir_in_project('\\project', test_list)
        self.assertTrue(result)

    def test_create_expected_dirs_dirs_exist_return_true(self):
        test_list = ['test_1', 'test_2']
        self.assertFalse(os.path.exists("\\project\\test_1"))
        self.assertFalse(os.path.exists("\\project\\test_2"))
        self.fs.create_dir("\\project\\test_1")
        self.fs.create_dir("\\project\\test_2")
        self.assertTrue(os.path.exists("\\project\\test_1"))
        self.assertTrue(os.path.exists("\\project\\test_2"))
        with mock.patch('dataset_scrapper.os.makedirs') as mocked_makedirs:
            result = dataset_scrapper.create_expected_dirs('\\project', test_list)
        self.assertTrue(result)
        mocked_makedirs.assert_not_called()

    def test_create_expected_dirs_if_dirs_not_exists_return_true(self):
        test_list = ['test_1', 'test_2']
        self.assertFalse(os.path.exists("\\project\\test_1"))
        self.assertFalse(os.path.exists("\\project\\test_2"))
        expected_calls = [mock.call("\\project\\test_1"), mock.call("\\project\\test_2")]
        with mock.patch('dataset_scrapper.os.makedirs') as mocked_makedirs:
            result = dataset_scrapper.create_expected_dirs('\\project', test_list)
            mocked_makedirs.assert_has_calls(expected_calls)
        self.assertTrue(result)

    def test_create_expected_dirs_if_one_dir_exists_return_true(self):
        test_list = ['test_1', 'test_2']
        self.assertFalse(os.path.exists("\\project\\test_1"))
        self.assertFalse(os.path.exists("\\project\\test_2"))
        self.fs.create_dir("\\project\\test_1")
        self.assertTrue(os.path.exists("\\project\\test_1"))
        expected_calls = [mock.call("\\project\\test_2")]
        with mock.patch('dataset_scrapper.os.makedirs') as mocked_makedirs:
            result = dataset_scrapper.create_expected_dirs('\\project', test_list)
            mocked_makedirs.assert_has_calls(expected_calls)
        self.assertTrue(result)

    def test_create_expected_dirs_if_test_list_is_empty_return_true(self):
        test_list = []
        self.assertFalse(os.path.exists("\\project\\test_1"))
        self.assertFalse(os.path.exists("\\project\\test_2"))

        with mock.patch('dataset_scrapper.os.makedirs') as mocked_makedirs:
            result = dataset_scrapper.create_expected_dirs('\\project', test_list)
            mocked_makedirs.assert_not_called()
        self.assertTrue(result)

    def test_check_file_age_file_new_return_true(self):
        self.fs.create_file('\\project\\test.txt')
        self.assertTrue(os.path.exists("\\project\\test.txt"))

        result = dataset_scrapper.check_file_not_old('\\project\\test.txt', 100)
        self.assertTrue(result)

    def test_check_file_age_file_old_return_false(self):
        self.fs.create_file('\\project\\test.txt')
        self.assertTrue(os.path.exists("\\project\\test.txt"))

        result = dataset_scrapper.check_file_not_old('\\project\\test.txt', 0)
        self.assertFalse(result)

    def test_check_file_age_no_file_return_false(self):
        result = dataset_scrapper.check_file_not_old('\\project\\test.txt', 10)
        self.assertFalse(result)

    def test_check_file_age_in_directory_new_file_return_dict_with_true_value(self):
        self.fs.create_file('\\project\\test.txt')
        self.assertTrue(os.path.exists("\\project\\test.txt"))

        result = dataset_scrapper.check_file_age_in_directory('\\project', 7, '.txt')
        expected = {'\\project\\test.txt': True}
        self.assertDictEqual(result, expected)

    def test_check_file_age_in_directory_no_file_return_empty_dict(self):
        result = dataset_scrapper.check_file_age_in_directory('\\project', 7, '.txt')
        expected = {}
        self.assertDictEqual(result, expected)

    def test_check_file_age_in_directory_old_file_return_dict_with_false_value(self):
        self.fs.create_file('\\project\\test.txt')
        self.assertTrue(os.path.exists("\\project\\test.txt"))
        result = dataset_scrapper.check_file_age_in_directory('\\project', 0, '.txt')
        expected = {'\\project\\test.txt': False}
        self.assertDictEqual(result, expected)

    def test_delete_old_files_do_nothing_when_file_new_return_true(self):
        file_dict = {'\\project\\test.txt': True}

        with mock.patch('dataset_scrapper.os.remove') as mock_remove:
            result = dataset_scrapper.delete_old_files(file_dict)
            mock_remove.assert_not_called()
        self.assertTrue(result)

    def test_delete_old_files_calls_filepath_when_file_old_return_true(self):
        file_dict = {'\\project\\test.txt': False}

        with mock.patch('dataset_scrapper.os.remove') as mock_remove:
            result = dataset_scrapper.delete_old_files(file_dict)
            mock_remove.assert_called_with('\\project\\test.txt')
        self.assertTrue(result)

    def test_delete_old_files_calls_filepath_when_no_file(self):
        file_dict = {'\\project\\test.txt': False}

        with mock.patch('builtins.print') as mocked_print:
            result = dataset_scrapper.delete_old_files(file_dict)
        mocked_print.assert_called_with("Can't remove file: [Errno 2] No such file or directory in the fake filesystem:"
                                        " 'C:\\\\project\\\\test.txt'")
        self.assertFalse(result)

    def test_check_json_config_exists_json_exist_return_true(self):
        self.fs.create_file('\\project\\cities.json', contents=self.json_content)
        result = dataset_scrapper.check_json_config_exists('\\project')
        self.assertTrue(result)

    def test_check_json_config_exists_no_config_return_false(self):
        result = dataset_scrapper.check_json_config_exists('\\project')
        self.assertFalse(result)

    def test_create_default_json_config_file_exists_return_true(self):
        self.fs.create_file('\\project\\cities.json', contents=self.json_content)
        open_mock = mock.mock_open()
        with mock.patch("dataset_scrapper.open", open_mock, create=True):
            result = dataset_scrapper.create_default_json_config('\\project', 'cities.json')
        self.assertTrue(result)
        open_mock.assert_called_with('\\project\\cities.json', 'w')
        open_mock.return_value.write.assert_called_once_with(dataset_scrapper.DEFAULT_CONFIG)

    def test_create_default_json_config_no_file_creates_one_returns_true(self):
        open_mock = mock.mock_open()
        with mock.patch("dataset_scrapper.open", open_mock, create=True):
            result = dataset_scrapper.create_default_json_config('\\project', 'cities.json')
        self.assertTrue(result)
        open_mock.assert_called_with('\\project\\cities.json', 'w')
        open_mock.return_value.write.assert_called_once_with(dataset_scrapper.DEFAULT_CONFIG)

    def test_read_json_no_file_return_empty_list(self):
        open_mock = mock.mock_open()
        with mock.patch("dataset_scrapper.open", open_mock, create=True):
            result = dataset_scrapper.read_json('\\project', 'cities.json')
        open_mock.assert_not_called()
        self.assertListEqual(result, [])

    def test_read_json_file_found_return_data(self):
        self.fs.create_file('\\project\\cities.json', contents=self.json_content)
        open_mock = mock.mock_open()
        original = dataset_scrapper.json.load
        dataset_scrapper.json.load = mock_json_load
        with mock.patch("dataset_scrapper.open", open_mock, create=True):
            result = dataset_scrapper.read_json('\\project', 'cities.json')
        open_mock.assert_called_with('\\project\\cities.json')
        expected = [{
            "city_id": 1,
            "city_name": "Wroclaw",
            "url": "https://www.wroclaw.pl/open-data/dataset/rozkladjazdytransportupublicznegoplik_data/resource/"
                   "62b3f371-2375-4979-874c-05c6bbb9b09e",
            "direct_link": False,
        }]
        self.assertListEqual(result, expected)
        dataset_scrapper.json.load = original



    def test_overwrite_incorrect_json_file_not_correct_overwrite_file_called_return_true(self):
        with mock.patch("dataset_scrapper.create_default_json_config") as mock_create_json:
            with mock.patch("dataset_scrapper.check_returned_config_is_correct", return_value=False):
                result = dataset_scrapper.overwrite_incorrect_json_file('\\project', 'cities.json')
        mock_create_json.assert_called_with('\\project', 'cities.json')
        self.assertTrue(result)

    def test_overwrite_incorrect_json_file_correct_file_return_true(self):
        with mock.patch("dataset_scrapper.create_default_json_config") as mock_create_json:
            with mock.patch("dataset_scrapper.check_returned_config_is_correct", return_value=True):
                result = dataset_scrapper.overwrite_incorrect_json_file('\\project', 'cities.json')
        mock_create_json.assert_not_called()
        self.assertTrue(result)

    def test_get_city_dict_return_dict_with_correct_city_name(self):
        test_list = mock_json_load()
        result = dataset_scrapper.get_city_dict(test_list, 'Wroclaw')
        expected = test_list[0]
        self.assertDictEqual(result, expected)

    def test_get_city_dict_return_wrong_keys_return_empty_dict(self):
        test_list = [{'test': 'value', 'url': 'test_url'}]
        result = dataset_scrapper.get_city_dict(test_list, 'Wroclaw')
        expected = {}
        self.assertDictEqual(result, expected)

    def test_get_zip_link_no_direct_link_return_link_from_href(self):
        r = '<a class="resource-url-analytics"' \
            ' href="https://www.wroclaw.pl/open-data/87b09b32-f076-4475-8ec9-6020ed1f9ac0/' \
            'OtwartyWroclaw_rozklad_jazdy_GTFS.zip">'
        mock_response = BeautifulSoup(r, features="html.parser")
        test_dict = {'city_name': 'Test', 'url': 'test_url', 'direct_link': False}
        with mock.patch('dataset_scrapper.get_website'):
            with mock.patch('dataset_scrapper.BeautifulSoup', return_value=mock_response):
                result = dataset_scrapper.get_zip_link(test_dict['url'],test_dict['direct_link'])
        self.assertEqual(result, 'https://www.wroclaw.pl/open-data/'
                                 '87b09b32-f076-4475-8ec9-6020ed1f9ac0/OtwartyWroclaw_rozklad_jazdy_GTFS.zip')

    def test_get_zip_link_direct_link_return_url_value(self):
        test_dict = {'city_name': 'Test', 'url': 'test_url', 'direct_link': True}
        result = dataset_scrapper.get_zip_link(test_dict['url'],test_dict['direct_link'])
        self.assertEqual(result, test_dict['url'])

    def test_download_zip_from_url_link_empty_return_False(self):
        with mock.patch('dataset_scrapper.wget.download') as mocked_download:
            result = dataset_scrapper.download_zip_from_url('\\project', '', 'Test_city')
        mocked_download.assert_not_called()
        self.assertFalse(result)

    def test_download_zip_from_url_link_correct_return_True(self):
        with mock.patch('dataset_scrapper.wget.download') as mocked_download:
            result = dataset_scrapper.download_zip_from_url('\\project',
                                                            'https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile',
                                                            'Test_city')
        mocked_download.assert_called_with('https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile',
                                           out='\\project\\zip_files\\Test_city.zip')
        self.assertTrue(result)

    def test_create_unzipped_folder_folder_exists_returns_true(self):
        self.fs.create_dir('\\project\\zip_files\\unzipped_test')
        with mock.patch('dataset_scrapper.os.makedirs') as mocked_makedirs:
            result = dataset_scrapper.create_unzipped_folder('\\project', 'test')
        mocked_makedirs.assert_not_called()
        self.assertTrue(result)
        self.assertTrue(os.path.exists('\\project\\zip_files\\unzipped_test'))

    def test_create_unzipped_folder_no_folder_creates_one_returns_true(self):
        self.assertFalse(os.path.exists('\\project\\zip_files\\unzipped_test'))
        with mock.patch('dataset_scrapper.os.makedirs') as mocked_makedirs:
            result = dataset_scrapper.create_unzipped_folder('\\project', 'test')
        mocked_makedirs.assert_called_with('\\project\\zip_files\\unzipped_test')
        self.assertTrue(result)

    def test_check_if_zip_file_exists_file_exists_return_path(self):
        self.fs.create_dir('\\project\\zip_files')
        self.fs.create_file('\\project\\zip_files\\test.zip')
        result = dataset_scrapper.check_if_zip_file_exists('\\project', 'test')
        self.assertEqual(result, '\\project\\zip_files\\test.zip')

    def test_check_if_zip_file_exists_no_file_return_empty_string(self):
        self.fs.create_dir('\\project\\zip_files')
        result = dataset_scrapper.check_if_zip_file_exists('\\project', 'test')
        self.assertEqual(result, '')

    def test_unzip_file_zip_file_exists_return_true(self):
        self.fs.create_dir('\\project\\zip_files')
        self.fs.create_file('\\project\\zip_files\\test.zip')
        with mock.patch('dataset_scrapper.zipfile.ZipFile') as mocked_zip:
            result = dataset_scrapper.unzip_file('\\project', 'test')
        mocked_zip.assert_called_once_with('\\project\\zip_files\\test.zip', 'r')
        mocked_zip().__enter__().extractall.assert_called_with('\\project\\zip_files\\unzipped_test')
        self.assertTrue(result)

    def test_unzip_file_zip_no_file_return_false(self):
        self.fs.create_dir('\\project\\zip_files')
        with mock.patch('dataset_scrapper.zipfile.ZipFile') as mocked_zip:
            result = dataset_scrapper.unzip_file('\\project', 'test')
        mocked_zip.assert_not_called()
        self.assertFalse(result)

    def test_save_txt_to_csv_file_exists_returns_true_creates_file_with_two_added_columns(self):
        example_content = "test1,test2,test3\nvalue1,value2,value3"
        self.fs.create_dir('\\project\\unzipped_test')
        self.fs.create_dir('\\project\\csv_files')
        self.fs.create_dir('\\project\\zip_files')
        self.fs.create_file('\\project\\zip_files\\unzipped_test\\example.txt', contents=example_content)
        test_dict = {"city_id": 50, "city_name": "test"}
        result = dataset_scrapper.save_txt_to_csv('\\project', test_dict, 'example')
        self.assertTrue(result)
        self.assertTrue(os.path.exists('\\project\\csv_files\\test-example.csv'))
        with open('\\project\\csv_files\\test-example.csv') as f:
            contents = f.read()
        headers = contents.split('\n')[0]  # check headers of the created file
        self.assertEqual(headers, 'test1,test2,test3,city_id,date')

    def test_save_txt_to_csv_no_file_returns_false(self):
        test_dict = {"city_id": 50, "city_name": "test"}
        result = dataset_scrapper.save_txt_to_csv('\\project', test_dict, 'example')
        self.assertFalse(result)


def mock_json_load(*args):
    return [{
        "city_id": 1,
        "city_name": "Wroclaw",
        "url": "https://www.wroclaw.pl/open-data/dataset/rozkladjazdytransportupublicznegoplik_data/resource/"
               "62b3f371-2375-4979-874c-05c6bbb9b09e",
        "direct_link": False, }]


def mock_json_wrong_load(*args):
    return [{
        "city_id": 1,
        "city_name": "Wroclaw"}]



