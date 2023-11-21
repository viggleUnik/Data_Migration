# Generator script for Oracle
from time import sleep
import logging
import os

from sqlalchemy.exc import StatementError, DBAPIError
from sqlalchemy.orm import sessionmaker

from sqlalchemy import exc, text, create_engine, bindparam

from scripts.datagen.database_functions import Database_Functions
from scripts.init_config import config
from scripts.utils.database_utils import get_ssh_tunnel, get_engine_path, create_session
from scripts.datagen.generate_data import gen_data_fake_regions




class Oracle_Data(Database_Functions):

    constraints = [
        ("countries", "fk_countries_regions"),
        ("locations", "fk_locations_countries"),
        ("warehouses", "fk_warehouses_locations"),
        ("employees", "fk_employees_manager"),
        ("products", "fk_products_categories"),
        ("contacts", "fk_contacts_customers"),
        ("orders", "fk_orders_customers"),
        ("orders", "fk_orders_employees"),
        ("order_items", "fk_order_items_products"),
        ("order_items", "fk_order_items_orders"),
        ("inventories", "fk_inventories_products"),
        ("inventories", "fk_inventories_warehouses"),
    ]

    def __init__(self, service):
        # Call the __init__ method of the superclass
        super().__init__(service)

    def disable_constraints(self):

        tunnel = None
        session = None

        try:

            # create tunnel
            tunnel = self.tunnel
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:

                for table_name, constraint_name in self.constraints:

                    statement = text(f"ALTER TABLE {table_name} DISABLE CONSTRAINT {constraint_name}")
                    session.execute(statement)

                    self.log.info(
                        f"Disabled constraint {constraint_name} for table {table_name} in Oracle Data ")

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database connection: {e}', exc_info=True)

        finally:
            #close session, tunnel
            if (session is not None):
                session.close()
            if (tunnel is not None):
                tunnel.stop()


    def enable_constraints(self):
        tunnel = None
        session = None

        try:

            # create tunnel
            tunnel = self.tunnel
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            engine_path = get_engine_path(db=self.service, local_port=local_port)
            engine = create_engine(engine_path)
            Session_En = sessionmaker(bind=engine)
            session = Session_En()

            try:

                for table_name, constraint_name in self.constraints:

                    statement = text(f"ALTER TABLE {table_name} ENABLE CONSTRAINT {constraint_name}")
                    session.execute(statement)
                    # Add logic to execute the statement (for example, using SQLAlchemy)
                    self.log.info(
                            f"Enabled constraint '{constraint_name}' for table '{table_name}' in {self.service} Data ")

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError)  as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database connection: {e}', exc_info=True)
        finally:

            # close session, tunnel
            if (session is not None):
                session.close()

            if (tunnel is not None):
                tunnel.stop()

    def insert_fake_data_regions(self, nr_recs: int):
        super().insert_data_fake_regions(nr_recs)






if __name__ == '__main__':

    orc = Oracle_Data(service='ORACLE')

    #orc.enable_constraints()

    #orc.disable_constraints()

    orc.insert_data_fake_regions(5)

 #obj.disable_constraints()

