import os
import time
import json
import requests
from bs4 import BeautifulSoup
import wget
import zipfile
from datetime import datetime, timezone
import csv

SEC_IN_DAYS = 86400
DEFAULT_CONFIG = """[
  {
    "city_id": 1,
    "city_name": "Wroclaw",
    "url": "https://www.wroclaw.pl/open-data/dataset/rozkladjazdytransportupublicznegoplik_data/resource/62b3f371-2375-4979-874c-05c6bbb9b09e",
    "direct_link": false
  },
  {
    "city_id": 2,
    "city_name": "Poznan",
    "url": "https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile",
    "direct_link": true
  }
]
"""

CORRECT_KEYS = {'city_id', 'city_name', 'url', 'direct_link'}


def check_correct_dir_in_project(path: str, expected_dirs_list: list) -> bool:
    """ Functions checks if path contains correct directories

    Parameters
    ----------
    path: str
        Path where functions checks directories.
    expected_dirs_list: list
        A  list of expected directories.

    Returns
    -------
    bool
        Returns true if all correct directories are in path,
         False if one or more expected directories are missing from path
    """
    filelist = [f for f in os.listdir(path)]
    return set(expected_dirs_list).issubset(set(filelist))


def create_expected_dirs(path: str, expected_dirs_list: list[str]) -> bool:
    """ Function takes path, checks if necessary directories exists, if not creates ones

        Parameters
        ----------
        path: str
            Path where functions checks directories.
        expected_dirs_list: list
             A list of expected directories.
        Returns
        -------
        bool
            returns True if directories are correct / all directories were created
            False if directories couldn't be created, exception was raised
    """
    is_dir = check_correct_dir_in_project(path, expected_dirs_list)
    if is_dir:
        return True
    else:
        for expected_dir in expected_dirs_list:
            if not os.path.exists(os.path.join(path, expected_dir)):
                try:
                    os.makedirs(os.path.join(path, expected_dir))
                except Exception as e:
                    print(f"Couldn't create directory {expected_dir}: {e}")
                    return False
        return True


def check_file_not_old(filepath: str, days: int = 7) -> bool:
    """ Functions checks if file is not old

    Parameters
    ----------
    filepath: str
        path to checked file
    days: int
        number of days when file starts to get old

    Returns
    -------
    result: bool
        Returns true if file is not old, false if file is old or doesn't exist

    """
    now = time.time()
    if not os.path.exists(filepath):
        return False
    elapsed_time = os.path.getmtime(filepath)  # time when file was last time modified
    if (now-elapsed_time) < SEC_IN_DAYS * days:
        return True
    else:
        return False


def check_file_age_in_directory(path: str, days: int, file_type: str) -> dict:
    """ Get list of all files in directory with specified file type,
     returns dict of file names with boolean value whether file is old or not
    Parameters
    ----------
    path: str
        path to checked directory
    file_type: str
        format of files that function checks
    days: int
        number of days when file starts to get old
    Returns
    -------
    result
        Returns dictionary with path to a file as a key, true if file is not old as value, false if file is old

    """
    filelist = [os.path.join(path, f) for f in os.listdir(path) if f.endswith(file_type)]
    result_list = [check_file_not_old(f, days) for f in filelist]
    return dict(zip(filelist, result_list))


def get_city_name_from_zip_filepath(filepath: str):
    return filepath.split('\\')[-1].split('.')[0]


def delete_old_files(file_dict: dict) -> bool:
    """ Deletes old files

    Parameters
    ----------
    file_dict
        result of the *check_file_age_in_directory*, dictionary with file path and if file is old

    Returns
    -------
    result: bool
        returns False if exception occurred, True if only new files where left

    """
    for key, value in file_dict.items():
        if not value:
            try:
                os.remove(key)
            except OSError as e:
                print(f"Can't remove old file {key}: {e}")
                return False
    return True


def delete_all_files_in_directory(path: str, file_type: str):
    filelist = [os.path.join(path, f) for f in os.listdir(path) if f.endswith(file_type)]
    for file in filelist:
        try:
            os.remove(file)
        except OSError as e:
            print(f"Can't remove file {file}: {e}")
            return False


def delete_all_files_in_directory_beginning_with_name(path: str, file_type: str, name: str):
    filelist = [os.path.join(path, f) for f in os.listdir(path) if (f.endswith(file_type) and f.startswith(name))]
    for file in filelist:
        try:
            os.remove(file)
        except OSError as e:
            print(f"Can't remove file {file}: {e}")
            return False


def check_json_config_exists(project_path: str, config_name: str = 'cities.json') -> bool:
    """ Checks if config file exists.

        Parameters
        ----------
        project_path: str
            current working directory
        config_name: str
            name of the config file. Defaults to cities.json

        Returns
        -------
        result: bool
            return True if config file exists

        """
    return os.path.exists(os.path.join(project_path, config_name))


def create_default_json_config(project_path: str, config_name: str = 'cities.json') -> bool:
    """ Creates default config file or overwrites it

    Parameters
    ----------
    project_path: str
        Path to project
    config_name
        name of the config file

    Returns
    -------
    result: bool
        Returns True if file already existed or file was created. False if exception occurred

    """
    try:
        with open(os.path.join(project_path, config_name), "w") as outfile:
            outfile.write(DEFAULT_CONFIG)
    except Exception as e:
        print(f'Error when creating json file: {e}')
        return False
    return True


def read_json(project_path: str, config_name: str = 'cities.json'):
    """ Reads config file if file is present.
     If no file was found returns empty list
    Parameters
    ----------
    project_path
        working directory
    config_name
        name of the config file
    Returns
    -------
    result
        return data read from json file
    """
    if check_json_config_exists(project_path, config_name):
        with open(os.path.join(project_path, config_name)) as f:
            data = json.load(f)
        return data
    else:
        return []


def get_website(city_url: str):
    """ Function returns requests response object from url given from url

    Parameters
    ----------
    city_url
        Url to a city specific website

    Returns
    -------
    result
        returns response if url was correctly given, None if
    """
    try:
        r = requests.get(city_url, timeout=5)
        if r.status_code == 200:
            return r
        else:
            return None
    except Exception as e:
        print(f"Raised exception when getting website: {e}")
        return None


def check_returned_config_is_correct(project_path: str, config_name: str = 'cities.json') -> bool:
    """ Function checks if config files has correct parameters as keys
     and if website from given url is responding

    Parameters
    ----------
    project_path
        Path to a project
    config_name
        Name of the config file

    Returns
    -------
    result
        return true if file is correct, false if some keys are missing or data is not a list

    """
    data = read_json(project_path, config_name)
    if type(data) == list:
        # check correct keys
        for city in data:
            if not CORRECT_KEYS.issubset(set(city.keys())):
                print('Data file does not have all keys.')
                return False
            if get_website(city['url']) is None: # check if website is responding
                print(f'Url in {city} is incorrect')
                return False
        return True
    else:
        print('Incorrect data')
        return False


def overwrite_incorrect_json_file(project_path: str, config_name: str = 'cities.json') -> bool:
    """
    Function overwrites incorrect config files with default arguments
    Parameters
    ----------
    project_path
        Path to a project
    config_name
        Name of the config file

    Returns
    -------
    result
        return True if file was correct, or it was successfully overwritten

    """
    if not check_returned_config_is_correct(project_path, config_name):
        return create_default_json_config(project_path, config_name)
    return True


def get_zip_link(city_url: str, direct_link: bool):
    """ function returns download link, giver either direct link from dictionary or parses one to find correct link

    Parameters
    ----------
    city_url
         url associated with city
    direct_link
        parameter if given url is direct link to zip file

    Returns
    -------

    """
    if direct_link:
        return city_url
    else:
        r = get_website(city_url)
        soup = BeautifulSoup(r.text, features="html.parser")
        for link in soup.find_all('a'):
            if link.get('href').endswith('.zip'):
                return link.get('href')


def download_zip_from_url(cwd: str, link: str, city_name: str) -> bool:
    """ Function downloads file from given link

    Parameters
    ----------
    cwd
        Current working directory
    link
        String of the link file should be downloaded
    city_name
        name of the city for which link should be downloaded

    Returns
    -------
    result
        Returns True if file was succsefully downloaded,
         False if link was empty or error has occurred when downloading

    """
    path = cwd + '\\zip_files'
    if link == '':
        print("Empty link, can't download")
        return False
    try:
        file_path = os.path.join(path, city_name + '.' + 'zip')
        wget.download(link, out=file_path)
        return True
    except Exception as e:
        print(f'Exception occurred when downloading: {e}')
        return False


def create_unzipped_folder(cwd: str, city_name: str) -> bool:
    """ Creates folder where files can be unzipped with its designated city name if it doesn't exists

    Parameters
    ----------
    cwd
        current working directory
    city_name
        Name of the city for which directory should be created

    Returns
    -------
    result
        Returns true if directory exists, or it was successfully created.
         False if exception has occurred when creating directory

    """
    path = cwd + '\\zip_files\\unzipped_' + city_name
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            return True
        except Exception as e:
            print(f"Couldn't create unzipped_{city_name} directory: {e}")
            return False
    else:
        return True


def check_if_zip_file_exists(cwd: str, city_name: str) -> str:
    """ Checks if zip file associated to a city name exists

    Parameters
    ----------
    cwd: str
        Current working directory
    city_name: str
        Name of the city for which check file

    Returns
    -------

    """
    path_zip = cwd + '\\zip_files'
    for file in os.listdir(path_zip):
        if file.startswith(city_name) and file.endswith('.zip'):
            print(f'Found file {file}')
            return os.path.join(path_zip, file)
    print(f'No {city_name}.zip file found')
    return ''


def unzip_file(cwd: str, city_name: str):
    """ Unzips the specified from the city name file

    Parameters
    ----------
    cwd
    city_name

    Returns
    -------

    """
    path_unzipped = cwd + '\\zip_files\\unzipped_' + city_name
    file_path = check_if_zip_file_exists(cwd, city_name)
    if file_path != '':
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(path_unzipped)
            return True
        except Exception as e:
            print(f'Error when downloading file: {e}')
            return False
    else:
        print('No correct files in directory')
        return False


def save_txt_to_csv_with_city_id_and_date(cwd: str, city_dict: dict, table_name: str):
    """ Saves txt file to csv format in different directory

    Parameters
    ----------
    cwd
    city_dict
    table_name

    Returns
    -------

    """
    path_unzipped = cwd + '\\zip_files\\unzipped_' + city_dict['city_name']
    path_csv = cwd + '\\csv_files'
    filename = table_name + '.txt'
    if not os.path.exists(os.path.join(path_unzipped, filename)):
        print(f'File {filename} not found')
        return False
    city_id = str(city_dict['city_id'])
    date = str(datetime.now(timezone.utc))
    outfile_name = city_dict.get('city_name') + '-' + table_name + '.csv'
    outfile_path = (os.path.join(path_csv, outfile_name))

    try:
        with open(os.path.join(path_unzipped, filename), 'r', encoding="utf8") as read_obj,\
                open(outfile_path, 'w', encoding="utf8", newline='') as write_obj:
            csv_reader = csv.reader(read_obj)
            csv_writer = csv.writer(write_obj)
            csv_writer.writerow(next(csv_reader) + ['city_id'] + ['date'])
            for row in csv_reader:
                row.append(city_id)
                row.append(date)
                csv_writer.writerow(row)
        return True
    except Exception as e:
        print(f"Error has occurred when writing to {table_name} file: {e}")
        return False




