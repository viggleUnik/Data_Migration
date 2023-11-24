import logging
import os
from pickle import NONE

class SQLAlchemyFilter(logging.Filter):
    def filter(self, record):
        # Check if the log record is from the SQLAlchemy engine
        if 'sqlalchemy.engine' in record.name:
            # Include ROLLBACK and COMMIT logs
            return 'ROLLBACK' in record.getMessage() or 'COMMIT' in record.getMessage()
        # Exclude all other logs
        return False

class config:

    FOLDER_OUTPUT = 'output'
    FOLDER_CONFIG = 'config'
    FOLDER_CSV = 'csv'
    FOLDER_LOGS = 'logs'
    FOLDER_SCRIPTS = 'scripts'
    FOLDER_SQLS = 'sqls'
    FOLDER_S3_DOWNLOAD = 's3_down'

    DIR_CONFIG = None
    DIR_OUTPUT = None
    DIR_CSV = None
    #DIR_LOGS = 'c:\\Users\\crvicol\\WorkAndStudy\\Python_Workspace\\epic_8\\output\\logs'
    DIR_LOGS = None
    DIR_SQLS = None
    DIR_S3_DOWNLOAD = None

    # Logging
    LOGS_LEVEL = 'info'
    FILE_LOGS = None

    # nr of records
    MAX = 30


    # params for data generation
    PARAMS = {
        'regions' : 100,
        'countries' : 150,
        'locations' : 500,
        'warehouses' : 100,
        'employees' : 300,
        'customers' : 600,
        'contacts' : 500,
        'product_categories' : 35,
        'products' : 550,
        'inventories': 300,
        'orders' : 300,
        'order_items': 250
    }



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
            config.DIR_CSV = os.path.join(os.getcwd(), config.FOLDER_OUTPUT, config.FOLDER_CSV )

        if config.DIR_LOGS is None:
            config.DIR_LOGS = os.path.join(os.getcwd(), config.FOLDER_OUTPUT, config.FOLDER_LOGS )

        if config.DIR_SQLS is None:
            config.DIR_SQLS = os.path.join(os.getcwd(), config.FOLDER_SCRIPTS, config.FOLDER_SQLS )

        if config.DIR_S3_DOWNLOAD is None:
            config.DIR_S3_DOWNLOAD = os.path.join(os.getcwd(), config.FOLDER_OUTPUT, config.FOLDER_S3_DOWNLOAD)

        os.makedirs(config.DIR_CONFIG, exist_ok=True)
        os.makedirs(config.DIR_OUTPUT, exist_ok=True)
        os.makedirs(config.DIR_CSV, exist_ok=True)
        os.makedirs(config.DIR_LOGS, exist_ok=True)
        os.makedirs(config.DIR_SQLS, exist_ok=True)
        os.makedirs(config.DIR_S3_DOWNLOAD, exist_ok=True)

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



        # Create a handler with the custom filter for 'sqlalchemy.engine'
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        handler.addFilter(SQLAlchemyFilter())

        # Add the handler to the 'sqlalchemy.engine' logger
        log_sqlalchemy_engine = logging.getLogger('sqlalchemy.engine')
        log_sqlalchemy_engine.addHandler(handler)
        log_sqlalchemy_engine.setLevel(logging.INFO)
        log_sqlalchemy_engine.propagate = False

