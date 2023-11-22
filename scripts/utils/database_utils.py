 # Helper functions for database connections
import logging
import os
import pandas as pd
import cx_Oracle
from sqlalchemy.exc import StatementError, DBAPIError

from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker

from scripts.utils.file_utils import read_config
from scripts.init_config import config

def get_engine_path(db: str, local_port):

    engine_path_auth = ''
    # create engine string with specific service
    if db == 'ORACLE':
        db_params = read_config(db)
        driver = db_params['DRIVER']
        user = db_params['USER']
        _pass = db_params['PASS']
        service = db_params['SERVICE']
        engine_path_auth = f"oracle+{driver}://{user}:{db_params['PASS']}@127.0.0.1:{local_port}/?service_name={service}"

    elif db == 'POSTGRE':
        db_params = read_config(db)
        user = db_params['USER']
        _pass = db_params['PASS']
        service = db_params['SERVICE']
        engine_path_auth = f'postgresql://{user}:{_pass}@127.0.0.1:{local_port}/{service}'

    return engine_path_auth

def get_ssh_tunnel(service: str) -> SSHTunnelForwarder:

    #get ssh params
    ssh_params = read_config(section='SSH')

    # get service params
    service_params = read_config(section=service)

    # assign params
    ssh_host = ssh_params['SSH_HOST']
    ssh_user = ssh_params['SSH_USER']
    private_key = ssh_params['PRIVATE_KEY']
    ssh_port = int(ssh_params['SSH_PORT'])
    bind_host = service_params['HOST']
    bind_port = int(service_params['PORT'])


    # Create an SSHTunnelForwarder object
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=private_key,
        remote_bind_address=(bind_host, bind_port)
    )
    return tunnel


def create_session(db: str, local_port: str):

    # get engine path
    engine_path_auth = get_engine_path(db, local_port)

    # create engine and session
    engine = create_engine(engine_path_auth)
    Session = sessionmaker(bind=engine)
    _session = Session()

    return _session


def execute_sql_file_to_df(service: str, file: str, order_date: str):

    config.setup_logging(logs='info')
    log = logging.getLogger(os.path.basename(__file__))

    tunnel = None
    session = None
    result_df = None


    # Read the SQL script from the file
    with open(file, 'r') as sql_file:
        sql_script = sql_file.read()

    # Replace the parameter in the SQL script with the actual order date
    sql_query = sql_script.replace(':target_order_date', f"'{order_date}'")

    try:
        # create tunnel
        tunnel = get_ssh_tunnel(service=service)
        tunnel.start()
        local_port = str(tunnel.local_bind_port)

        # create session
        session = create_session(db=service, local_port=local_port)

        try:
            # Execute the SQL query and fetch the result into a DataFrame
            result_df = pd.read_sql_query(sql_query, session.bind)

        except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
            log.error(f'Error executing SQL query: {e}', exc_info=True)
            session.rollback()  # Rollback changes in case of an error

    except Exception as e:
        log.error(f'Error setting up database {service} connection: {e}', exc_info=True)

    finally:
        if (session is not None):
            session.close()
        if (tunnel is not None):
            tunnel.stop()

    return result_df









