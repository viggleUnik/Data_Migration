 # Generator script for PostgreSQL

from sqlalchemy.exc import StatementError, DBAPIError
from sqlalchemy.orm import sessionmaker

from sqlalchemy import exc, text, create_engine, bindparam

from scripts.datagen.database_functions import Database_Functions

from scripts.utils.database_utils import get_ssh_tunnel, get_engine_path, create_session
from scripts.datagen.generate_data import gen_data_fake_regions

class Postgre_Data(Database_Functions):

    constraints = [
        ("contacts", "fk_contacts_customers", "customer_id", "customers", "customer_id"),
        ("countries", "fk_countries_regions", "region_id", "regions", "region_id"),
        ("inventories", "fk_inventories_products", "product_id", "products", "product_id"),
        ("inventories", "fk_inventories_warehouses", "warehouse_id", "warehouses", "warehouse_id"),
        ("locations", "fk_locations_countries", "country_id", "countries", "country_id"),
        ("order_items", "fk_order_items_orders", "order_id", "orders", "order_id"),
        ("order_items", "fk_order_items_products", "product_id", "products", "product_id"),
        ("orders", "fk_orders_customers", "customer_id", "customers", "customer_id"),
        ("products", "fk_products_categories", "category_id", "product_categories", "category_id"),
        ("warehouses", "fk_warehouses_locations", "location_id", "locations", "location_id")
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

                for table_name, constraint_name, _, _, _ in self.constraints:
                    statement = text(f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name}")
                    session.execute(statement)
                    self.log.info(f"Disabled constraint '{constraint_name}' for table '{table_name}' in PostgreSQL")

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database connection: {e}', exc_info=True)

        finally:
            print('aici disable')
            # close session, tunnel
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
            session = create_session(db=self.service, local_port=local_port)

            try:

                for table_name, constraint_name, column_name, ref_table, ref_column in self.constraints:
                    statement = text(
                            f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column_name}) REFERENCES {ref_table} ({ref_column})")
                    session.execute(statement)
                    self.log.info(f"Enabled constraint '{constraint_name}' for table '{table_name}' in PostgreSQL")

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError)  as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database connection: {e}', exc_info=True)
        finally:
            print('aici enable')
            # close session, tunnel
            if (session is not None):
                session.close()

            if (tunnel is not None):
                tunnel.stop()


if __name__ == '__main__':

    pg = Postgre_Data('POSTGRE')

    pg.enable_constraints()

