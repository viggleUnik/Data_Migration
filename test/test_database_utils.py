import unittest
import logging
import os

from scripts.init_config import config
from scripts.utils.database_utils import get_ssh_tunnel, create_session, get_engine_path
from sqlalchemy import exc, text, create_engine






class test_database_utils(unittest.TestCase):


    def test_get_ssh_tunnel_oracle(self):

        config.setup_logging(logs='info')
        log = logging.getLogger(os.path.basename(__file__))

        orc_tunnel = get_ssh_tunnel('ORACLE')
        try:
            orc_tunnel.start()
            log.info('Tunnel start %s! ', orc_tunnel.tunnel_is_up)
        except Exception as e:
            log.error(f'Error starting the tunnel: {e}')
        finally:
            orc_tunnel.close()

    def test_get_ssh_tunnel_postgre(self):
        config.setup_logging(logs='info')
        log = logging.getLogger(os.path.basename(__file__))

        pg_tunnel = get_ssh_tunnel('POSTGRE')
        try:
            pg_tunnel.start()
            log.info('Tunnel start %s! ', pg_tunnel.tunnel_is_up)
        except Exception as e:
            log.error(f'Error starting the tunnel: {e}')
        finally:
            pg_tunnel.close()

    def test_session_oracle(self):

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

            # create session
            session = create_session(db='ORACLE', local_port=local_port)

            # test connection
            test = session.execute(text('select table_name from user_tables'))
            for row in test:
                print(row)


        except exc.SQLAlchemyError as e:
            log.error(f'Error connecting to the database: {e}')
        finally:
            # Close the session and stop the tunnel in the finally block
            if session is not None:
                session.close()
            if tunnel is not None:
                tunnel.stop()

    def test_session_postgre(self):
        config.setup_logging(logs='info')
        log = logging.getLogger(os.path.basename(__file__))

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service='POSTGRE')
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db='POSTGRE', local_port=local_port)

            # test connection
            test = session.execute(text('select * from regions'))
            for row in test:
                print(row)
        except exc.SQLAlchemyError as e:
            log.error(f'Error connecting to the database: {e}')
        finally:
            # Close the session and stop the tunnel in the finally block
            if session is not None:
                session.close()
            if tunnel is not None:
                tunnel.stop()

    def test_with_connection(self):
        conn = None
        tunnel = None
        try:
            tunnel = get_ssh_tunnel(service='ORACLE')
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            engine_path_auth = get_engine_path(db='ORACLE', local_port=local_port)
            print(engine_path_auth)

            engine = create_engine(engine_path_auth)

            with engine.connect() as conn:
                result = conn.execute(text('SELECT * FROM regions'))
                for row in result:
                    print(row)

            conn.close()

        except Exception as e:
            print(f"Error: {e}")
        finally:
            tunnel.close()