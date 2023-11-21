from faker import Faker
import pandas as pd
import pycountry
import logging
import os
from datetime import datetime, timedelta

from scripts.init_config import config


from scripts.init_config import config

config.setup_logging(logs='info')
log = logging.getLogger(os.path.basename(__file__))

def gen_data_fake_regions(nr_recs: int, id_start: int = 1 ):

    fake = Faker()

    # Initialize the DataFrame with the columns
    df = pd.DataFrame(columns=['region_name'])

    for i in range(nr_recs):

        df.loc[i] = [
                f'r_{fake.unique.word()}'
        ]
    return df


def gen_data_fake_countries(nr_recs: int, regions_ids: list[int], country_ids: list[str] ):

    fake = Faker('en_US')

    if nr_recs > 249:
        log.info('Maximum nr of countries are 249')
        nr_recs = 249

    # Initialize the DataFrame with the columns
    df = pd.DataFrame(columns=['country_id', 'country_name', 'region_id'])

    # Use a set to keep track of generated country codes
    if country_ids is not None:
        generated_country_codes = set(country_ids)
    else:
        generated_country_codes = set()

    # when regions table is empty
    if regions_ids is None:
        regions_ids = [x for x in range(config.MAX)]

    for i in range(1, nr_recs):

        # Select a random country and region
        country = fake.random_element(elements=pycountry.countries)
        country_code = country.alpha_2
        region = fake.random_element(elements=regions_ids)

        # country code is unique
        while country_code in generated_country_codes:
            country = fake.random_element(elements=pycountry.countries)
            country_code = country.alpha_2

        # Add the country code to the set
        generated_country_codes.add(country_code)

        # Add a new row to the DataFrame in each iteration
        df.loc[i] = {
            'country_id': country_code,
            'country_name': country.name,
            'region_id': region
        }

    return df


def gen_data_fake_locations(nr_recs: int, country_ids: list[str]):
    fake = Faker()

    # Initialize the DataFrame with the columns
    df = pd.DataFrame(columns=[
        'address',
        'postal_code',
        'city',
        'state',
        'country_id'
    ])

    # generate some countries
    if country_ids is None:
        country_ids = [x for x in range(config.MAX)]

    for i in range(1, nr_recs):
        # Add a new row to the DataFrame in each iteration
        df.loc[i] = [
            fake.street_address(),
            str(fake.zipcode()),
            fake.city(),
            fake.state(),
            fake.random_element(elements=country_ids)
        ]

    return df

def gen_data_fake_warehouses(nr_recs: int,  location_ids: list[int]):
    fake = Faker()

    # Initialize the DataFrame with the columns
    df = pd.DataFrame(columns=[
        'warehouse_name',
        'location_id'
    ])

    # generate some  locations
    if location_ids is None:
        location_ids = [x for x in range(config.MAX)]

    for i in range(1, nr_recs):
        # Add a new row to the DataFrame in each iteration

        df.loc[i] = [
            fake.company(),
            fake.random_element(elements=location_ids)
        ]

    return df


def gen_data_fake_employees(nr_recs: int, db: str):

    fake = Faker()

    df = pd.DataFrame(columns=[

        'first_name',
        'last_name',
        'email',
        'phone',
        'hire_date',
        'manager_id',
        'job_title'
    ])

    for i in range(1, nr_recs):

        if db == 'ORACLE':
            fake_date = fake.date_this_decade()
            # Convert the datetime.date to a string with the desired format
            hire_date = fake_date.strftime("%d-%b-%y").upper()
        else:
            hire_date = fake.date_this_decade()

        df.loc[i] = [
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            fake.phone_number(),
            hire_date, # hire_date
            fake.random_int(min=1, max=config.MAX), # manager_id
            fake.job()
        ]

    return df


def gen_data_fake_product_categories(nr_recs: int):
    fake = Faker()

    df = pd.DataFrame(columns=['category_name'])

    for i in range(1, nr_recs):
        df.loc[i] = [
            f'cat_{fake.word()}'
        ]

    return df


def gen_data_fake_products(nr_recs: int, category_ids: list[int]):

    fake = Faker()

    df = pd.DataFrame(columns=[
        'product_name',
        'description',
        'standard_cost',
        'list_price',
        'category_id'
    ])

    # generate categories
    if category_ids is None:
        category_ids = [x for x in range(config.MAX)]

    for i in range(1, nr_recs):
        df.loc[i] = [
            f'p_{fake.word()}',  # product_name
            fake.text(max_nb_chars=300),  # description
            fake.random_int(min=1, max=3000),  # standard_cost
            fake.random_int(min=1001, max=4000),  # list_price
            fake.random_element(elements=category_ids)  # category_
        ]

    return df


def gen_data_fake_customers(nr_recs: int):

    fake = Faker()

    df = pd.DataFrame(columns=[
        'name',
        'address',
        'website',
        'credit_limit'
    ])

    for i in range(1, nr_recs):
        df.loc[i] = [
            fake.company(),  # name
            fake.street_address(),  # address
            fake.url(),  # website
            fake.random_int(min=1, max=1000000)  # credit_limit
        ]

    return df


def gen_data_fake_contacts(nr_recs: int, customer_ids: list[int]):
    fake = Faker()

    df = pd.DataFrame(columns=[
        'first_name',
        'last_name',
        'email',
        'phone',
        'customer_id'
    ])

    if customer_ids is None:
        customer_ids = [x for x in range(config.MAX)]

    for i in range(1, nr_recs):
        df.loc[i] = [
            fake.first_name(),  # first_name
            fake.last_name(),  # last_name
            fake.email(),  # email
            fake.phone_number()[:20],  # phone
            fake.random_element(elements=customer_ids)  # customer_id
        ]

    return df


def gen_data_fake_orders(nr_recs: int, customer_ids: list[int], salesman_ids: list[int], db: str):

    fake = Faker()

    df = pd.DataFrame(columns=[
            'customer_id',
            'status',
            'salesman_id',
            'order_date'
        ])

    if customer_ids is None:
        customer_ids = [x for x in range(config.MAX)]

    if salesman_ids is None:
        salesman_ids = [x for x in range(config.MAX)]

    for i in range(1, nr_recs):

        # Generate a random date within the past 3 years
        fake_date_this_year = fake.date_this_year()
        n = fake.random_int(min=0, max=2)
        order_date = fake_date_this_year - timedelta(days=365 * n)

        if db == 'ORACLE':
            # Convert the datetime.date to a string with the desired format
            order_date = order_date.strftime("%d-%b-%y").upper()

        df.loc[i] = [
            fake.random_element(elements=customer_ids),  # customer_id
            fake.random_element(elements=['Pending', 'Canceled', 'Shipped']),  # status
            fake.random_element(elements=salesman_ids),  # salesman_id
            order_date  # order_date

        ]

    return df


def gen_data_fake_order_items( order_items: list[int], products_ids: list[int]):

    fake = Faker()
    columns = [
        'order_id',
        'item_id',
        'product_id',
        'quantity',
        'unit_price'
    ]
    rows = []

    df = pd.DataFrame(columns=columns)

    if order_items is None:
        order_items = [x for x in range(1, config.MAX)]

    print( f'od : {order_items}')

    if products_ids is None:
        products_ids = [x for x in range(1, config.MAX)]

    print(f'prods : {order_items}')

    for i in order_items:

        nr_of_items = fake.random_int(min=1, max=5)

        for j in range(1, nr_of_items, 1):

            row_values = [
                i,  # order_id
                j,  # item_id
                fake.random_element(elements=products_ids),  # product_id
                fake.random_int(min=1, max=15),  # quantity
                fake.random_int(min=300, max=30000),  # unit_price
            ]

            # Append the row to the DataFrame
            rows.append(row_values)

    df = pd.DataFrame(rows, columns=columns)

    return df


if __name__ == '__main__':

    print('everything will be fine')
    # Create a Faker instance
    fake = Faker()

    # Generate a random date within the past 3 years
    fake_date_this_year = fake.date_this_year()
    n = fake.random_int(min=0, max=2)
    fake_date_last_3_years = fake_date_this_year - timedelta(days=365*n)

