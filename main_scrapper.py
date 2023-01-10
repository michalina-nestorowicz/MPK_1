from dataset_scrapper import *
from database_updater import *
import os


DAYS_WHEN_FILE_IS_OLD = 1
EXPECTED_DIRS_IN_PROJECT = ['csv_files', 'zip_files']
LIST_OF_TABLES = ['routes', 'trips', 'stops', 'stop_times']

"""
STEPS - set_up:
1.check if correct dirs are in project, if not create them
2.check if json config exists
3.create default config file if it doesnt exists
4.check if config file is correct and Overwrite it if its not 
5.check if zip files are old - return dict that decides whether or not other functions should be used
6.delete old zip files
7.if zip file from city was old, delete all files in directory unzipped 
8.delete all csv files with old city name
"""


def set_up(project_path: str, config_name: str = 'cities.json'):
    create_expected_dirs(project_path, EXPECTED_DIRS_IN_PROJECT)  # 1
    if not check_json_config_exists(project_path, config_name):  # 2
        create_default_json_config(project_path, config_name)  # 3
    overwrite_incorrect_json_file(project_path, config_name)  # 4
    zip_path = os.path.join(project_path, 'zip_files')
    csv_path = os.path.join(project_path, 'csv_files')
    zip_old_dict = check_file_age_in_directory(zip_path, DAYS_WHEN_FILE_IS_OLD, '.zip')  # 5
    print(zip_old_dict)
    delete_old_files(zip_old_dict)  # 6
    for key, value in zip_old_dict.items():
        if not value:
            city_name = get_city_name_from_zip_filepath(key)
            unzipped_path = os.path.join(project_path, ('zip_files\\unzipped_' + city_name))
            delete_all_files_in_directory(unzipped_path, '.txt') # 7
            delete_all_files_in_directory_beginning_with_name(csv_path, '.csv', city_name) # 8


"""
STEPS - get_data
1. read json data
2. For every city in data:
3. Check if zip file exists (new)
3.1. if no, download
4. create unzipped folder
5. Unzip zip file 
6. Create csv file for every table
"""


def get_data(project_path: str,  list_of_tables: list, config_name: str = 'cities.json'):
    data = read_json(project_path, config_name)  # 1
    for city in data:  # 2
        zip_file = check_if_zip_file_exists(project_path, city['city_name'])  # 3
        if zip_file == '':  # 3.1
            link = get_zip_link(city['url'], city['direct_link'])
            download_zip_from_url(project_path, link, city['city_name'])
        create_unzipped_folder(project_path, city['city_name'])  # 4
        unzip_file(project_path, city['city_name'])  # 5
        for table in list_of_tables:  # 6
            save_txt_to_csv_with_city_id_and_date(project_path, city, table)


"""
STEPS: update_table
iterate over tables and cities
1.Delete unimportant columns from csv
2. Get df from csv
3. Create temp table from df
4. Delete old records
5. Insert new records
"""


def update_tables(project_path: str):
    csv_path = os.path.join(project_path, 'csv_files')
    for city in read_json(project_path):
        for table in queries.TABLE_LIST:
            table_path = os.path.join(csv_path, (city['city_name'])+'-' + table['table_name'] + '.csv')
            delete_unnecessary_columns_in_csv(table_path, table.get('important_columns'))
            table_df = get_df_from_csv(csv_path, city['city_name'], table['table_name'])
            create_temp_table(table_df, table['table_name'])
            delete_old_records_from_table(table['table_name'], city['city_id'])
            insert_new_records_to_table(table['table_name'], city['city_id'])


if __name__ == '__main__':
    set_up(os.getcwd())
    get_data(os.getcwd(), LIST_OF_TABLES)
    update_tables(os.getcwd())


