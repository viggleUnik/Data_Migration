# Helper functions for file operations
from configparser import ConfigParser
import os


config_file_path = 'C:\\Users\\crvicol\\WorkAndStudy\\Python_Workspace\\epic_8\\config\\config.ini'

def read_config(section : str, filename=config_file_path):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

     # get section 
    section_params = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            section_params[param[0].upper()] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return section_params


