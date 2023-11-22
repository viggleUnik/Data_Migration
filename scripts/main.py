# Main script to run your project processes
import os
from re import S
import faker

import logging
from init_config import config
from scripts.utils.file_utils import read_config
from scripts.datagen.database_functions import DatabaseFunctions
from scripts.utils.file_utils import save_dataframe_to_csv
from scripts.utils.database_utils import execute_sql_file_to_df

def run():
    
    config.setup(logs_level='debug')
    log = logging.getLogger(os.path.basename(__file__))

    for i in range(10):
        log.info(f'Runing {os.path.basename(__file__)} looping {i}')


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



if __name__ == '__main__':

    df = execute_sql_file_to_df('POSTGRE', './sqls/big_sql_join.sql', '2021-08-09')

    #project_directory = os.path.abspath(os.path.dirname(__file__))

    save_dataframe_to_csv(df)












