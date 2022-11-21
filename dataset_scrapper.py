import requests
from bs4 import BeautifulSoup
import wget
import os
import time
import zipfile
import pandas as pd
import json
from datetime import datetime, timezone


def check_csv_age(cwd, days=7) -> str:
    path_csv = cwd + '\\csv_files'
    now = time.time()
    filelist = [f for f in os.listdir(path_csv) if f.endswith(".csv")]
    if len(filelist) == 0:
        print('No csv files where found')
        return 'No file'
    for f in filelist:
        elapsed_time = now - os.path.getmtime(os.path.join(path_csv, f))
        if elapsed_time > (days * 86400):
            print('csv file are old')
            return 'file old'
    print('Files not old enough')
    return 'file not old'


def delete_txt_files(cwd):
    path_unzipped = cwd + '\\zip_files\\unzipped_txt'
    filelist = [f for f in os.listdir(path_unzipped) if f.endswith(".txt")]
    for f in filelist:
        os.remove((os.path.join(path_unzipped, f)))


def delete_old_zip_file(cwd, days=7):
    path = cwd + '\\zip_files'
    now = time.time()
    for filename in os.listdir(path):
        # print(now - os.path.getmtime(os.path.join(path, filename)))
        elapsed_time = now - os.path.getmtime(os.path.join(path, filename))
        if elapsed_time > (days * 86400):
            print('Old .zip files - deleting')
            try:
                os.remove(os.path.join(path, filename))
                print('File removed')
                delete_txt_files(cwd)
                print(' Old .txt files deleted')
                return True

            except Exception as e:
                print('Could not remove file: ', e)
                return False

        else:
            print('File not old enough')
            return False

    print('No .zip files where found')
    return False


def dataset_download(cwd, city_name) -> bool:
    path = cwd + '\\zip_files'
    # print(path)
    with open('cities.json') as f:
        data = json.load(f)
        # print(cities)
    cities = data['cities']
    city = [x for x in cities if x.get('city_name') == city_name][0]
    # print(city['url'])
    url = city['url']
    r = requests.get(url, timeout=5)
    soup = BeautifulSoup(r.text, features="html.parser")
    # print(soup.prettify())
    for link in soup.find_all('a'):
        if link.get('href').endswith('.zip'):
            print(link.get('href'))
            try:

                file_path = os.path.join(path, city_name + '.' + 'zip')
                wget.download(link.get('href'), out=file_path)
                # wget.download(link.get('href'))
                return True
            except Exception as e:
                print(e)
                return False
    print("Message: couldn't find any .zip files")
    return False


def unzip_files(cwd, city_name):
    path_zip = cwd + '\\zip_files'
    # path_csv = cwd + '\csv_files'
    path_unzipped = cwd + '\\zip_files\\unzipped_'+city_name
    if not os.path.exists(path_unzipped):  # create directory if it doesn't exist
        os.makedirs(path_unzipped)

    for filename in os.listdir(path_zip):
        if filename.startswith(city_name):
            if filename.endswith('.zip'):
                file_path = os.path.join(path_zip, filename)
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(path_unzipped)


def txt_to_csv(cwd, city_name, table_name='routes') -> None:
    path_unzipped = cwd + '\\zip_files\\unzipped_'+city_name
    path_csv = cwd + '\\csv_files'
    filename = 'routes.txt'

    if table_name == 'routes':
        if city_name == 'Wroclaw':
            filename = 'routes.txt'  # TODO something more clever than this

    with open('cities.json') as f:
        cities_data = json.load(f)

    cities = cities_data['cities']
    city = [x for x in cities if x.get('city_name') == city_name][0]

    data = pd.read_csv(os.path.join(path_unzipped, filename))
    data = data.assign(city_id=city['city_id'])
    data = data.assign(date=datetime.now(timezone.utc))

    data_col = ['route_id', 'route_short_name', 'route_desc', 'city_id', 'date']

    for col in data.columns:
        if col not in data_col:
            data.drop(col, axis=1, inplace=True)

    name = city_name + '-' + table_name + '.csv'
    routes_path = (os.path.join(path_csv, name))
    data.to_csv(routes_path, encoding='ansi', sep=';', index=False)
