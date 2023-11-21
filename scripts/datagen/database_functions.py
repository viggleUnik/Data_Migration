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

    def disable_constraints(self):
        raise NotImplementedError("Method for disabling constraints")

    def enable_constraints(self):
        raise NotImplementedError("Method for enabling constraints")

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
                res = session.execute(text('SELECT customer_id FROM customers'))
                customer_ids = [row[0] for row in res.fetchall()]

                print(customer_ids)
                # select available
                res = session.execute(text('SELECT employee_id FROM employees'))
                salesman_ids = [row[0] for row in res.fetchall()]

                print(salesman_ids)

                if len(customer_ids) == 0:
                    customer_ids = None

                if len(salesman_ids) == 0:
                    salesman_ids = None

                # generate dataframe with fake data
                data = DGen.gen_data_fake_orders( nr_recs, customer_ids, salesman_ids, self.service)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'orders')

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



    def insert_data_fake_order_items(self):

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
                res = session.execute(text('SELECT order_id FROM orders'))
                all_orders_ids = [row[0] for row in res.fetchall()]
                print('All Orders: ')
                print(all_orders_ids)

                res = session.execute(text('SELECT order_id FROM order_items'))
                wr_orders_ids = [row[0] for row in res.fetchall()]

                remaining_order_items = [item for item in all_orders_ids if item not in wr_orders_ids]

                print('remaing')
                print(remaining_order_items)
                # select available products
                res = session.execute(text('SELECT product_id FROM products'))
                product_ids = [row[0] for row in res.fetchall()]

                # if we have empty orders, products
                if len(remaining_order_items) == 0:
                    remaining_order_items = None

                if len(product_ids) == 0:
                    product_ids = None


                # generate dataframe with fake data
                data = DGen.gen_data_fake_order_items(remaining_order_items, product_ids)

                # generate queries for insert
                _queries = QGen.generate_insert_statement(data, 'orders')

                for q in _queries:
                    print(q)
                    #session.execute(text(q))

                # commit the changes
                #session.commit()

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



if __name__ == '__main__':


    obj = DatabaseFunctions(service='ORACLE')
    #obj.insert_data_fake_countries(5)
    #obj.insert_data_fake_locations(5)
    #obj.insert_data_fake_warehouses(5)
    #obj.insert_data_fake_employees(20)
    #obj.insert_data_fake_product_categories(10)
    #obj.insert_data_fake_products(10)
    #obj.insert_data_fake_customers(10)
    #obj.insert_data_fake_contacts(10)
    #obj.insert_data_fake_orders(10)
    obj.insert_data_fake_order_items()




