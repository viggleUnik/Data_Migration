 # Helper functions for database connections
import logging
import os

from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts.utils.file_utils import read_config
from scripts.init_config import config

config.setup_logging('error')
log = logging.getLogger(os.path.basename(__file__))

def create_oracle_connection():

    #get ssh and oracle params
    ssh_params = read_config(section='SSH')
    oracle_params = read_config(section='ORACLE')

    # assign params
    # SSH
    ssh_host = ssh_params['SSH_HOST']
    ssh_user = ssh_params['SSH_USER']
    private_key = ssh_params['PRIVATE_KEY']
    ssh_port = int(ssh_params['SSH_PORT'])
    local_port = ssh_params['LOCAL_PORT']

    # ORACLE
    orc_user = oracle_params['ORC_USER']
    orc_password = oracle_params['ORC_PASS']
    orc_host = oracle_params['ORC_HOST']
    orc_port = int(oracle_params['ORC_PORT'])

    # Create an SSHTunnelForwarder object
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=private_key,
        remote_bind_address=(orc_host, orc_port)
    )

    try:
        tunnel.start()
        local_port = str(tunnel.local_bind_port)

        DIALECT = 'oracle'
        SQL_DRIVER = 'cx_oracle'
        SERVICE = 'testgen'
        ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + orc_user + ':' + orc_password + '@' + '127.0.0.1' + ':' + local_port + '/?service_name=' + SERVICE


        engine = create_engine(ENGINE_PATH_WIN_AUTH)

        Session1 = sessionmaker(bind=engine)
        session = Session1()
        test = session.execute("select * from user_tables")

        for row in test:
            print(row)

        session.close()

    except Exception as e:
        log.info(f'Error starting the tunnel: {e}')
    finally:
        tunnel.stop()


def get_oracle_connection(credentials):
    connection_str = f"oracle+cx_oracle://{credentials['oracle_username']}:{credentials['oracle_password']}@127.0.0.1:{credentials['local_port']}/?service_name=orclpdb"
    engine = create_engine(connection_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


if __name__ == '__main__':
    create_oracle_connection()