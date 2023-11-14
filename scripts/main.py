# Main script to run your project processes
import os
import faker

import logging
from logs_config import config


def run():
    
    config.setup_logging()
    log = logging.getLogger(os.path.basename(__file__))

    for i in range(10):
        log.info(f'Runing {os.path.basename(__file__)} looping {i}')

if __name__ == '__main__':
    run()


