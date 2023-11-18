# Generator script for Oracle

import logging
import os
from sqlalchemy.orm import sessionmaker

from sqlalchemy import exc, text, create_engine, bindparam


from scripts.init_config import config
from scripts.utils.database_utils import get_ssh_tunnel, create_session, get_engine_path
from scripts.datagen.generate_data import gen_data_fake_regions



"""
    Before adding records from postgres, need to think about seting autoincremented ids
    to new value
    
"""


def insert_data_into_regions():
    data_list = gen_data_fake_regions(10)

    config.setup_logging(logs='info')
    log = logging.getLogger(os.path.basename(__file__))

    # Initialize tunnel and session to None
    tunnel = None
    session = None

    try:
        # create tunnel
        tunnel = get_ssh_tunnel(service='ORACLE')
        tunnel.start()
        local_port = str(tunnel.local_bind_port)

        try:
            # create engine and session
            engine_path_auth = get_engine_path('ORACLE', local_port)
            engine = create_engine(engine_path_auth)
            Session = sessionmaker(bind=engine)

            with Session() as session:
                try:
                    # Define the SQL query with bind parameters
                    sql_query = text("INSERT INTO REGIONS (REGION_ID, REGION_NAME) VALUES (:region_id, :region_name)")
                    sql_query = sql_query.bindparams(bindparam('region_id'), bindparam('region_name'))

                    for r in data_list:
                        # Execute the query for each row, passing values as a dictionary
                        session.execute(sql_query, r)

                    # commit the changes
                    session.commit()

                except exc.SQLAlchemyError as e:
                    log.error(f'Error executing SQL query: {e}', exc_info=True)

        except Exception as e:
            log.error(f'Error setting up database connection: {e}', exc_info=True)

    finally:

        if tunnel:
            tunnel.stop()





if __name__ == '__main__':

    data = gen_data_fake_regions(100)
    # print(data)


