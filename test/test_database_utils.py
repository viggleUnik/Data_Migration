import unittest
from scripts.utils.database_utils import get_ssh_tunnel, create_session
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import exc, text


class test_database_utils(unittest.TestCase):

    def test_get_ssh_tunnel_oracle(self):

        orc_tunnel = get_ssh_tunnel('ORACLE')
        try:
            orc_tunnel.start()
        except Exception as e:
            print(f'Error starting the tunnel: {e}')
        finally:
            orc_tunnel.close()

    def test_get_ssh_tunnel_postgre(self):

        pg_tunnel = get_ssh_tunnel('POSTGRE')
        try:
            pg_tunnel.start()
        except Exception as e:
            print(f'Error starting the tunnel: {e}')
        finally:
            pg_tunnel.close()

    def test_session_oracle(self):
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
            test = session.execute(text('select * from regions'))
            for row in test:
                print(row)
        except exc.SQLAlchemyError as e:
            print(f'Error connecting to the database: {e}')
        finally:
            # Close the session and stop the tunnel in the finally block
            if session is not None:
                session.close()
            if tunnel is not None:
                tunnel.stop()

    def test_session_postgre(self):
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
            print(f'Error connecting to the database: {e}')
        finally:
            # Close the session and stop the tunnel in the finally block
            if session is not None:
                session.close()
            if tunnel is not None:
                tunnel.stop()
