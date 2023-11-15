import unittest

from scripts.utils.file_utils import read_config

class test_utils_read_config(unittest.TestCase):

    def test_ssh_tunnel_params(self):

        params = {
            'SSH_HOST' : '3.122.51.140',
            'SSH_USER' : 'cvicol',
            'PRIVATE_KEY' : 'c:\\Users\\crvicol\\.ssh\\crvicol',
            'SSH_PORT' : 22,
            'LOCAL_PORT' : 3000 
        }


        self.assertEqual(params, read_config(section='SSH'))


