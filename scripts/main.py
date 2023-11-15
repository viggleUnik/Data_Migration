# Main script to run your project processes
import os
from re import S
import faker

import logging
from init_config import config
from scripts.utils.file_utils import read_config

def run():
    
    config.setup(logs_level='debug')
    log = logging.getLogger(os.path.basename(__file__))

    for i in range(10):
        log.info(f'Runing {os.path.basename(__file__)} looping {i}')



if __name__ == '__main__':

    ssh = read_config(section='SSH')
    print(ssh)
    #run()

