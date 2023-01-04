import requests
from bs4 import BeautifulSoup
import wget
import os
import time
import zipfile
import pandas as pd
import json
from datetime import datetime, timezone

SEC_IN_DAYS = 86400


def check_correct_dir_in_project(path: str, expected_dirs_list: list = None) -> bool:
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
        Returns true if all correct directories are in path, False if one or more expected directories is missing from path

    """
    filelist = [f for f in os.listdir(path)]
    result = all(item in set(filelist) for item in expected_dirs_list)
    return result


def create_expected_dirs(path: str, expected_dirs_list: list[str] = None) -> bool:
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
        returns True if files are correct / all directories where created
        False if directories couldn't be created, exception was raised

    """
    is_exist = check_correct_dir_in_project(path, expected_dirs_list)
    if not is_exist:
        print('Creating directories')
        for expected_dir in expected_dirs_list:
            if not os.path.exists(os.path.join(path, expected_dir)):
                try:
                    os.makedirs(os.path.join(path, expected_dir))
                    print(f'Directory {expected_dir} is created')
                except Exception as e:
                    print(f"Couldn't create directories: {e}")
                    return False
        return True
    else:
        print('No directories were created')
        return True


def check_file_age(filepath: str, days: int = 7) -> bool:
    """ Functions checks if file is old

    Parameters
    ----------
    filepath: str
        path to checked file
    days: int
        number of days when file starts to get old

    Returns
    -------
    result: bool
        Returns true if file is old or doesn't exist, false if file is not old

    """
    if not os.path.exists(filepath):
        print("File doesn't exists")
        return True
    now = time.time()
    elapsed_time = now - os.path.getmtime(filepath)
    if elapsed_time >= (days * SEC_IN_DAYS):
        return True
    else:
        return False


def check_file_age_in_directory(path: str, file_type: str, days: int):
    """ Get list of all files in directory with specified file type,
     return dict of file name with boolean value whether file is old or not
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

    """
    filelist = [os.path.join(path, f) for f in os.listdir(path) if f.endswith(file_type)]
    result_list = [check_file_age(f, days) for f in filelist]
    result_dict = dict(zip(filelist, result_list))
    if len(result_dict) == 0:
        print(f'No {file_type} files where found')
    return result_dict


def delete_old_files(file_dict) -> bool:
    """ Deletes old files

    Parameters
    ----------
    file_dict
        result of the *check_file_age_in_directory*, dictionary with file path and if file is old

    Returns
    -------
    result: bool
        returns False if exception occurred, True if only not old files where left

    """
    for key, value in file_dict.items():
        if value:
            print(f'old file {key}')
            try:
                os.remove(key)
            except Exception as e:
                print('')
                return False
    return True


def check_json_exists(cwd: str, config_name: str = 'cities.json') -> bool:
    """ Checks if config file cities.json exists.

    Parameters
    ----------
    cwd: str
        current working directory
    config_name

    Returns
    -------
    result: bool
        return True if cities.json file exists

    """
    return os.path.exists(os.path.join(cwd, config_name))


def create_dummy_json_if_absent(cwd: str, config_name: str = 'cities.json') -> bool:
    """ Creates dummy config file if absent

    Parameters
    ----------
    cwd
        Current working directory
    config_name
        Name of config file

    Returns
    -------
    result: bool
        Returns True if file already existed, or it was successfully created. returns false when exception occurs

    """
    is_file = check_json_exists(cwd, config_name)
    if is_file:
        print('File exists')
        return True
    else:
        try:
            with open(config_name, "w") as outfile:
                pass
            return True
        except Exception as e:
            print(f"Couldn't create a dummy json file: {e}")
            return False


def read_json(city_name: str) -> dict:
    """ Function reads cities.json file and returns dictionary with city name and all of their variables like url and city_id

    Parameters
    ----------
    city_name: str
        Name of the city

    Returns
    -------
    city
        Returns dictionary with city name as key and its configuration, returns empty dictionary if city not found

    """

    with open('cities.json') as f:
        data = json.load(f)
    for city in data:
        if city['city_name'] == city_name:
            return city

    print(f"Returns empty dictionary, couldn't find config for {city_name}")
    return {}


def get_request(city: dict):
    """ Function return response object from url in provided dictionary

    Parameters
    ----------
    city: dict
        City dictionary with its parameters

    Returns
    -------


    """
    if city == {}:
        raise KeyError('City directory is empty')
    url = city.get('url')
    if url is None:
        raise KeyError('No url value in directory')

    r = requests.get(url, timeout=5)

    return r


def get_zip_link(city: dict):
    """ Parses website to find url to zip file
    Parameters
    ----------
    city: dict
        City dictionary with its parameters

    Returns
    -------
    link
        Return link to zip file
    """
    if city['city_name'] == 'Wroclaw':
        r = get_request(city)
        soup = BeautifulSoup(r.text, features="html.parser")
        for link in soup.find_all('a'):
            if link.get('href').endswith('.zip'):
                return link.get('href')
    if city['city_name'] == 'Poznan':
        return city['url']
    return ''

def dataset_download(cwd: str, city_name: str, link: str) -> bool:
    """ Download file from provided link and saves as 'city_name'.zip

    Parameters
    ----------
    cwd: str
        Current working directory
    city_name: str
        String with city name
    link: str


    Returns
    -------
    result
        Returns True if file was successfully downloaded, False if link was empty or exception has occurred

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


def create_unzip_directory(cwd: str, city_name: str) -> bool:
    """ Creates directory where .zip file can be unzipped if it doesn't exists

    Parameters
    ----------
    cwd: str
        current working directory
    city_name: str
        Name of the city where zip file will be unzipped

    Returns
    -------
    result
        returns True if directory already existed, or it was successfully created. False when exception has occurred

    """
    path_unzipped = cwd + '\\zip_files\\unzipped_' + city_name
    if not os.path.exists(path_unzipped):
        try:
            os.makedirs(path_unzipped)
            return True
        except Exception as e:
            print(f"Couldn't create directory: {e}")
    return True


def unzip_files(cwd: str, city_name: str) -> bool:
    """ Unzips file found in zip_files folder with city_name as the title of the file

    Parameters
    ----------
    cwd: str
        current working directory
    city_name: str
        Name of the city which file should be unzipped

    Returns
    -------
    result: bool
        Returns True if file was successfully uzipped. False if there were no zip file with city_name or exception has occurred

    """
    path_zip = cwd + '\\zip_files'
    path_unzipped = cwd + '\\zip_files\\unzipped_' + city_name

    if len(os.listdir(path_zip)) == 0:
        print('No .zip files were found')
        return False

    for filename in os.listdir(path_zip):
        if filename.startswith(city_name) and filename.endswith('.zip'):
            file_path = os.path.join(path_zip, filename)
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(path_unzipped)
                return True
            except Exception as e:
                print(f"Couldn't extract zip file: {e}")


def save_txt_to_csv(cwd: str, city: dict, table_name: str) -> bool:
    path_unzipped = cwd + '\\zip_files\\unzipped_' + city['city_name']
    path_csv = cwd + '\\csv_files'

    if city['city_name'] == 'Wroclaw' or city['city_name'] == 'Poznan':
        filename = table_name + '.txt'
    else:
        filename = table_name + '.txt'

    if not os.path.exists(os.path.join(path_unzipped, filename)):
        print(f'File {filename} not found')
        return False

    # TODO: change to not use pandas
    data = pd.read_csv(os.path.join(path_unzipped, filename), encoding='utf-8')
    data = data.assign(city_id=city['city_id'])
    data = data.assign(date=datetime.now(timezone.utc))

    name = city.get('city_name') + '-' + table_name + '.csv'
    file_path = (os.path.join(path_csv, name))
    data.to_csv(file_path, encoding='utf-8', sep=';', index=False)

