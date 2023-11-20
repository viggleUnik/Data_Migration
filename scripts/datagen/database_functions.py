import logging
import os

from scripts.init_config import config
from scripts.utils.database_utils import get_ssh_tunnel

class Database_Functions:

    def __init__(self, service):
        config.setup_logging(logs='info')
        self.log = logging.getLogger(os.path.basename(__file__))
        self.service = service
        self.tunnel = get_ssh_tunnel(service=self.service)

    def disable_constraints(self):
        raise NotImplementedError("Method for disabling constraints")

    def enable_constraints(self):
        raise NotImplementedError("Method for enabling constraints")

