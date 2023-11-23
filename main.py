# Main script to run your project processes
import os
from re import S
import faker

import logging
from scripts.init_config import config
from scripts.utils.file_utils import read_config
from scripts.datagen.database_functions import DatabaseFunctions
from scripts.utils.file_utils import save_dataframe_to_csv
from scripts.utils.database_utils import execute_sql_file_to_df, get_oracle_table_data_to_csv

def run():
    config.setup(logs_level='debug')



def data_generation_run(service: str):

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


def from_sql_to_csv_saved(service: str, order_date: str):

    config.setup_logging(logs='info')
    sql_file_path = os.path.join(config.DIR_SQLS, 'big_sql_join.sql')
    df = execute_sql_file_to_df(service, sql_file_path, order_date)
    save_dataframe_to_csv(df)



if __name__ == '__main__':

    # set directories
    config.setup_dirs()

    #from_sql_to_csv_saved('POSTGRE', '2021-08-09')

    #get_oracle_table_data_to_csv(table_name='order_items')














