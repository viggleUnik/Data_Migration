# Helper functions for file operations
from configparser import ConfigParser
import os
from scripts.init_config import config
import logging


def read_config(section : str):

    config_file_path = os.path.join(config.DIR_CONFIG, 'config.ini')

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(config_file_path)

     # get section 
    section_params = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            section_params[param[0].upper()] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return section_params


def save_dataframe_to_csv(dataframe):

    log = logging.getLogger(os.path.basename(__file__))
    csv_file_name = f'{dataframe.columns[1]}.csv'
    # Construct the full path to the CSV file
    csv_path = os.path.join(config.DIR_CSV, csv_file_name)
    # Save the DataFrame to the CSV file
    dataframe.to_csv(csv_path, index=False)
    log.info(f"DataFrame saved to {csv_path}")






