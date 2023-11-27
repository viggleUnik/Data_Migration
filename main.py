# Main script to run your project processes
import os
from re import S
import faker


from scripts.utils.query_generator import QueryGenerator
import logging
from scripts.init_config import config
from scripts.utils.file_utils import read_config
from scripts.datagen.database_functions import DatabaseFunctions
from scripts.utils.file_utils import save_dataframe_to_csv
from scripts.utils.database_utils import execute_sql_file_to_df, get_oracle_table_data_to_csv, load_data_file_s3_to_postgres_db
from scripts.utils.s3_data_utils import S3DataUtils


def main_data_generation_run(service: str):

    config.setup(logs_level='info')

    obj = DatabaseFunctions(service=service)

    obj.insert_data_fake_regions(config.PARAMS['regions'])
    obj.insert_data_fake_countries(config.PARAMS['countries'])
    obj.insert_data_fake_locations(config.PARAMS['locations'])
    obj.insert_data_fake_warehouses(config.PARAMS['warehouses'])
    obj.insert_data_fake_employees(config.PARAMS['employees'])
    obj.insert_data_fake_product_categories(config.PARAMS['product_categories'])
    obj.insert_data_fake_products(config.PARAMS['products'])
    obj.insert_data_fake_customers(config.PARAMS['customers'])
    obj.insert_data_fake_contacts(config.PARAMS['contacts'])
    obj.insert_data_fake_orders(config.PARAMS['orders'])
    obj.insert_data_fake_order_items(nr_of_orders=config.PARAMS['order_items'])
    obj.insert_data_fake_inventories(nr_products=config.PARAMS['inventories'])


table_names = [
    'regions',
'countries',
'locations',
'warehouses',
'employees',
'product_categories',
'products',
'customers',
'contacts',
'orders',
'order_items',
'inventories'
]

def main_delete_data_from_db(service: str):

    obj = DatabaseFunctions(service=service)
    obj.delete_data_from_tables(table_names=table_names)

def from_sql_to_csv_saved(service: str, order_date: str):

    config.setup_logging(logs='info')
    sql_file_path = os.path.join(config.DIR_SQLS, 'big_sql_join.sql')
    df = execute_sql_file_to_df(service, sql_file_path, order_date)
    save_dataframe_to_csv(df)

def _run():
    # set directories and log file
    config.setup(logs_level='info')

    # delete and generate
    main_delete_data_from_db(service='ORACLE')
    main_delete_data_from_db(service='POSTGRE')

    main_data_generation_run(service='ORACLE')

    # big_sql_join task
    from_sql_to_csv_saved('POSTGRE', '2021-08-09')

    # save localy oracle data to csv

    for t in table_names:
        get_oracle_table_data_to_csv(table_name=t)

    # list objects in bucket before
    S3DataUtils.list_objects_in_folder('student4/migrationData/')

    # upload all csv files to s3
    S3DataUtils.upload_to_s3()

    # list objects after
    S3DataUtils.list_objects_in_folder('student4/migrationData/')

    # load csv file from s3 bucket to postgres

    load_data_file_s3_to_postgres_db('inventories')


if __name__ == '__main__':
    _run()















