import logging
import os
import pandas as pd

from sqlalchemy.exc import StatementError, DBAPIError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc, text

from scripts.utils.query_generator import QueryGenerator as QGen
import scripts.datagen.generate_data as DGen
from scripts.init_config import config
from scripts.utils.database_utils import get_ssh_tunnel, create_session


class DatabaseFunctions:

    def __init__(self, service):
        config.setup_logging(logs='info')
        self.log = logging.getLogger(os.path.basename(__file__))
        self.service = service
        self.tunnel = get_ssh_tunnel(service=self.service)

    def control_constraints(self, control: str):

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

                _queries = []

                if control == 'ENABLE':
                    _queries = QGen.generate_enable_constraints(service=self.service)
                elif control == 'DISABLE':
                    _queries = QGen.generate_disable_constraints(service=self.service)

                for q in _queries:
                    print(q)
                    session.execute(text(q))

                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:
            if (session is not None):
                session.close()
            if (tunnel is not None):
                tunnel.stop()


    def insert_data_fake_regions(self, nr_recs: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            session = create_session(db=self.service, local_port=local_port)

            try:

                # generate dataframe with fake data
                data = DGen.gen_data_fake_regions(nr_recs)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'regions')

                for q in _queries:
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:
            if (session is not None):
                session.close()
            if (tunnel is not None):
                tunnel.stop()

    def insert_data_fake_countries(self, nr_recs: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:

                # select available region_ids
                res = session.execute(text('SELECT region_id FROM regions'))
                region_ids = [row[0] for row in res.fetchall()]

                # select country_id in order to not repeat same generated id
                res = session.execute(text('SELECT country_id FROM countries'))
                country_ids = [row[0] for row in res.fetchall()]

                # if empty lists
                if len(country_ids) == 0:
                    country_ids = None

                if len(region_ids) == 0:
                    region_ids = None

                # generate dataframe with fake data
                data = DGen.gen_data_fake_countries(nr_recs, region_ids, country_ids)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'countries')

                for q in _queries:
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:
            if (session is not None):
                session.close()
            if (tunnel is not None):
                tunnel.stop()

    def insert_data_fake_locations(self, nr_recs: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:
                # select available country_ids
                res = session.execute(text('SELECT country_id FROM countries'))
                country_ids = [row[0] for row in res.fetchall()]

                # case countries table is empty
                if len(country_ids) == 0:
                    country_ids = None

                # generate dataframe with fake data
                data = DGen.gen_data_fake_locations(nr_recs, country_ids)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'locations')

                for q in _queries:
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:
            if (session is not None):
                session.close()
            if (tunnel is not None):
                tunnel.stop()

    def insert_data_fake_warehouses(self, nr_recs: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:
                # select available locations
                res = session.execute(text('select location_id from locations'))
                location_ids = [row[0] for row in res.fetchall()]

                # case when locations table is empty
                if len(location_ids) == 0:
                    location_ids = None

                # generate dataframe with fake data
                data = DGen.gen_data_fake_warehouses(nr_recs, location_ids)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'warehouses')

                for q in _queries:
                    print(q)
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:

            if (session is not None):
                session.close()

            if (tunnel is not None):
                tunnel.stop()

    def insert_data_fake_employees(self, nr_recs: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:

                # generate dataframe with fake data
                data = DGen.gen_data_fake_employees(nr_recs, self.service)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'employees')

                for q in _queries:
                    print(q)
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:

            if (session is not None):
                session.close()

            if (tunnel is not None):
                tunnel.stop()

    def insert_data_fake_product_categories(self, nr_recs: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:

                # generate dataframe with fake data
                data = DGen.gen_data_fake_product_categories(nr_recs)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'product_categories')

                for q in _queries:
                    print(q)
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:

            if (session is not None):
                session.close()

            if (tunnel is not None):
                tunnel.stop()

    def insert_data_fake_products(self, nr_recs: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:

                # select available locations
                res = session.execute(text('select category_id from product_categories'))
                category_ids = [row[0] for row in res.fetchall()]

                if len(category_ids) == 0:
                    category_ids = None

                # generate dataframe with fake data
                data = DGen.gen_data_fake_products(nr_recs, category_ids)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'products')

                for q in _queries:
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:

            if session is not None:
                session.close()

            if tunnel is not None:
                tunnel.stop()

    def insert_data_fake_customers(self, nr_recs: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:

                # generate dataframe with fake data
                data = DGen.gen_data_fake_customers(nr_recs)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'customers')

                for q in _queries:
                    print(q)
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:

            if session is not None:
                session.close()

            if tunnel is not None:
                tunnel.stop()

    def insert_data_fake_contacts(self, nr_recs: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:

                # select available customer ids
                res = session.execute(text('SELECT customer_id FROM customers'))
                customer_ids = [row[0] for row in res.fetchall()]

                if len(customer_ids) == 0:
                    customer_ids = None

                # generate dataframe with fake data
                data = DGen.gen_data_fake_contacts(nr_recs, customer_ids)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'contacts')

                for q in _queries:
                    print(q)
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:

            if session is not None:
                session.close()

            if tunnel is not None:
                tunnel.stop()


    def insert_data_fake_orders(self, nr_recs: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:


                # select available customer ids
                res_1 = session.execute(text('SELECT customer_id FROM customers'))
                customer_ids = [row[0] for row in res_1.fetchall()]

                # select available employees
                res_2 = session.execute(text('SELECT employee_id FROM employees'))
                salesman_ids = [row[0] for row in res_2.fetchall()]

                # in case we have order_items, but we dont have any orders
                res_3 = session.execute(text('SELECT MAX(order_id) FROM orders'))
                max_order_id = res_3.scalar()
                max_order_id = max_order_id if max_order_id is not None else 0

                res_4 = session.execute(text('SELECT MAX(order_id) FROM order_items'))
                temp = res_4.scalar()
                temp = temp if temp is not None else 0

                # in this case we see if we insert enough records and set nr_recs
                if (max_order_id == 0) and temp > nr_recs:
                    nr_recs = temp

                if len(customer_ids) == 0:
                    customer_ids = None

                if len(salesman_ids) == 0:
                    salesman_ids = None

                # generate dataframe with fake data
                data = DGen.gen_data_fake_orders( nr_recs, customer_ids, salesman_ids, self.service)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'orders')

                for q in _queries:
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:

            if session is not None:
                session.close()

            if tunnel is not None:
                tunnel.stop()

    def insert_data_fake_order_items(self, nr_of_orders: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:

                # select all available orders ids
                res_1 = session.execute(text('SELECT order_id FROM orders'))
                all_orders_ids = [row[0] for row in res_1.fetchall()]

                res_2 = session.execute(text('SELECT order_id FROM order_items'))
                wr_orders_ids = [row[0] for row in res_2.fetchall()]

                remaining_order_items = [item for item in all_orders_ids if item not in wr_orders_ids]


                # select available products
                res = session.execute(text('SELECT product_id FROM products'))
                product_ids = [row[0] for row in res.fetchall()]

                # if we have empty orders, products
                if len(remaining_order_items) == 0:
                    remaining_order_items = None

                print(f'remain ord {remaining_order_items}')

                if len(product_ids) == 0:
                    product_ids = None

                # generate dataframe with fake data
                data = DGen.gen_data_fake_order_items(remaining_order_items, product_ids, nr_of_orders)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'order_items')

                for q in _queries:
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:

            if session is not None:
                session.close()

            if tunnel is not None:
                tunnel.stop()


    def insert_data_fake_inventories(self, nr_products: int):

        # Initialize tunnel and session to None
        tunnel = None
        session = None

        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:

                # select all available orders ids
                res_1 = session.execute(text('SELECT product_id FROM products'))
                product_ids = [row[0] for row in res_1.fetchall()]

                res_2 = session.execute(text('SELECT warehouse_id FROM warehouses'))
                warehouse_ids = [row[0] for row in res_2.fetchall()]

                # if we have empty products, warehouses

                if len(warehouse_ids) == 0:
                    warehouse_ids = None

                if len(product_ids) == 0:
                    product_ids = None

                # generate dataframe with fake data
                data = DGen.gen_data_fake_inventories(product_ids, warehouse_ids, nr_products)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'inventories')

                for q in _queries:
                    print(q)
                    session.execute(text(q))

                # commit the changes
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:

            if session is not None:
                session.close()

            if tunnel is not None:
                tunnel.stop()

    def delete_data_from_tables(self, table_names: list[str]):
        # Initialize tunnel and session to None
        tunnel = None


        try:
            # create tunnel
            tunnel = get_ssh_tunnel(service=self.service)
            tunnel.start()
            local_port = str(tunnel.local_bind_port)

            # create session
            session = create_session(db=self.service, local_port=local_port)

            try:

                _queries = QGen.generate_delete_statements(table_names=table_names)
                for q in _queries:
                    session.execute(text(q))
                session.commit()

            except (exc.SQLAlchemyError, StatementError, DBAPIError) as e:
                self.log.error(f'Error executing SQL query: {e}', exc_info=True)
                session.rollback()  # Rollback changes in case of an error
            finally:
                if session is not None and session.is_active:
                    session.close()

        except Exception as e:
            self.log.error(f'Error setting up database {self.service} connection: {e}', exc_info=True)

        finally:

            if tunnel is not None:
                tunnel.stop()




if __name__ == '__main__':
    project_directory = os.path.abspath(os.path.dirname(__file__))
    print(project_directory)
    file = os.path.dirname(__file__)
    print(file)