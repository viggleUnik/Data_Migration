import logging

class config:

    FOLDER_OUTPUT_LOGS = 'output/logs'

    # Logging
    LOGS_LEVEL = 'debug'
    FILE_LOGS = None

    @staticmethod
    def setup_logging():
        config.FILE_LOGS = f"{config.FOLDER_OUTPUT_LOGS}/logs.log"

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

