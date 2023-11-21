import logging
import os
from pickle import NONE

class config:

    FOLDER_OUTPUT = 'output'
    FOLDER_CONFIG = 'config'
    FOLDER_CSV = 'csv'
    FOLDER_LOGS = 'logs'
    
    DIR_CONFIG = None
    DIR_OUTPUT = None
    DIR_CSV = None
    DIR_LOGS = 'c:\\Users\\crvicol\\WorkAndStudy\\Python_Workspace\\epic_8\\output\\logs'

    # Logging
    LOGS_LEVEL = 'info'
    FILE_LOGS = None

    # nr of records
    MAX = 10

    @staticmethod
    def setup(logs_level : str):
        config.setup_dirs()
        config.setup_logging(logs_level)


    @staticmethod
    def setup_dirs():

        if config.DIR_CONFIG is None:
            config.DIR_CONFIG = os.path.join(os.getcwd(), config.FOLDER_CONFIG)

        if config.DIR_OUTPUT is None:
            config.DIR_OUTPUT = os.path.join(os.getcwd(), config.FOLDER_OUTPUT)
        
        if config.DIR_CSV is None:
            config.DIR_CSV = os.path.join(config.FOLDER_OUTPUT, config.FOLDER_CSV )

        if config.DIR_LOGS is None:
            config.DIR_LOGS = os.path.join(config.FOLDER_OUTPUT, config.FOLDER_LOGS )

        os.makedirs(config.DIR_CONFIG, exist_ok=True)
        os.makedirs(config.DIR_OUTPUT, exist_ok=True)
        os.makedirs(config.DIR_CSV, exist_ok=True)
        os.makedirs(config.DIR_LOGS, exist_ok=True)



    @staticmethod
    def setup_logging(logs : str):
        
        if logs != 'info':
            config.LOGS_LEVEL = logs

        config.FILE_LOGS = f"{config.DIR_LOGS}/logs.log"

        # set logging config:
        loglevels_dict = {'debug': logging.DEBUG, 'info': logging.INFO, 'error': logging.ERROR}
        
        logs_level = loglevels_dict.get(config.LOGS_LEVEL.lower(), logging.INFO)

        logging.basicConfig(
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler(config.FILE_LOGS), logging.StreamHandler()],
            level=logs_level,
        )

        # set all existing loggers to level=LOGS_LEVEL
        loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
        for logger in loggers:
            logger.setLevel(loglevels_dict.get(config.LOGS_LEVEL.lower(), logging.INFO))

