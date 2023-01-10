import pandas as pd
import os
from psycopg2 import sql
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import csv

import queries

load_dotenv()
url = os.getenv("DATABASE_URL")
url_alchemy = os.getenv("SQLALCHEMY_URL")
engine = create_engine(url_alchemy)


def delete_unnecessary_columns_in_csv(file_path: str, important_columns: list):
    """

    Parameters
    ----------
    file_path
    important_columns

    Returns
    -------

    """
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8-sig') as read_obj:
            reader = csv.DictReader(read_obj)
            headers = reader.fieldnames
            rows = list(reader)
            columns_to_delete = list(set(headers) - set(important_columns))
            for column in columns_to_delete:
                headers.remove(column)
        with open(file_path, 'w', encoding='utf-8-sig',  newline='') as write_obj:
            writer = csv.DictWriter(write_obj, fieldnames=headers,  extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        return True
    else:
        raise OSError(f'Error when deleting columns from file {file_path}')


def get_df_from_csv(csv_path: str, city_name: str, table_name: str):
    """

    Parameters
    ----------
    csv_path
    city_name
    table_name

    Returns
    -------

    """
    filename = city_name + '-' + table_name + '.csv'
    filepath = os.path.join(csv_path, filename)
    if os.path.exists(filepath):
        df = pd.read_csv(filepath, delimiter=',')
        return df
    else:
        raise OSError(f"{filename} does not exists")


def create_temp_table(table_df: pd.DataFrame, table_name: str):
    """

    Parameters
    ----------
    table_df
    table_name

    Returns
    -------

    """
    name = f'temp_{table_name}'
    try:
        table_df.to_sql(name=name, con=engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"Error when creating temporary tables: {e}")
        return False


def delete_old_records_from_table(table_name: str, city_id: int):
    """

    Parameters
    ----------
    table_name
    city_id

    Returns
    -------

    """
    try:
        with psycopg2.connect(url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL(queries.DELETE_OLD_RECORDS).format(
                    table_name=sql.Identifier(table_name)), (city_id,))
        return True
    except Exception as e:
        print(f'Error occurred when deleting old records: {e}')
        return False


def get_table_dict(table_name: str) -> dict:
    """

    Parameters
    ----------
    table_name

    Returns
    -------

    """
    for table in queries.TABLE_LIST:
        if table.get('table_name') == table_name:
            return table
    else:
        raise KeyError(f'No {table_name} table')


def insert_new_records_to_table(table_name: str, city_id: int):
    """

    Parameters
    ----------
    table_name
    city_id

    Returns
    -------

    """
    try:
        insert_query = get_table_dict(table_name)['insert_query']
        with psycopg2.connect(url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL(insert_query), (city_id,))
        return True
    except Exception as e:
        print(f'Error has occurred when inserting new records to {table_name}: {e}')
        return False

