from faker import Faker
import pandas as pd
import pycountry
import logging
import os

from scripts.init_config import config

config.setup_logging(logs='info')
log = logging.getLogger(os.path.basename(__file__))

def gen_data_fake_regions(nr_recs: int):

    fake = Faker()
    data_list = []

    # Initialize the DataFrame with the columns
    df = pd.DataFrame(columns=['region_name'])

    for i in range(1, nr_recs):
        data = {}  # Create a new dictionary in each iteration
        data['region_name'] = f'r_{fake.unique.word()}'
        data_list.append(data)

        df.loc[i] = [ f'r_{fake.unique.word()}']

    df = pd.DataFrame(data_list)
    return df, data_list


def gen_data_fake_countries(nr_recs: int, nr_of_regions: int):

    fake = Faker('en_US')

    if nr_recs > 249:
        log.info('Maximum nr of countries are 249')
        nr_recs = 249

    # Initialize the DataFrame with the columns
    df = pd.DataFrame(columns=['country_id', 'country_name', 'region_id'])

    # Use a set to keep track of generated country codes
    generated_country_codes = set()

    for i in range(1, nr_recs):
        # Select a random country
        country = fake.random_element(elements=pycountry.countries)
        country_code = country.alpha_2

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
            'region_id': fake.random_int(min=5, max=nr_of_regions+5)
        }

    return df


def gen_data_fake_locations(nr_recs: int):
    fake = Faker()

    # Initialize the DataFrame with the columns
    df = pd.DataFrame(columns=[
        'address',
        'postal_code',
        'city',
        'state',
        'country_id'
    ])

    for i in range(1, nr_recs):
        # Add a new row to the DataFrame in each iteration
        df.loc[i] = [
            fake.street_address(),
            str(fake.zipcode()),
            fake.city(),
            fake.state(),
            fake.country_code()
        ]

    return df

def gen_data_fake_warehouses(nr_recs: int,  nr_of_locations: int):
    fake = Faker()

    # Initialize the DataFrame with the columns
    df = pd.DataFrame(columns=[
        'warehouse_name',
        'location_id'
    ])

    for i in range(11, nr_recs):
        # Add a new row to the DataFrame in each iteration
        df.loc[i] = [
            fake.company(),
            fake.random_int(min=24, max=nr_of_locations+24)
        ]

    return df


def gen_data_fake_employees(nr_recs: int ):
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
        df.loc[i] = [
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            fake.phone_number(),
            fake.date_this_decade(),
            fake.random_int(min=108, max=108 + nr_recs - 1),
            fake.job()
        ]

    return df


def gen_data_fake_product_categories(nr_recs: int):
    fake = Faker()

    df = pd.DataFrame(columns=['category_id', 'category_name'])

    for i in range(6, 6 + nr_recs):
        df.loc[i] = [
            i,
            fake.word()
        ]

    return df


def gen_data_fake_products(nr_recs: int):
    fake = Faker()

    df = pd.DataFrame(columns=[
        'product_id',
        'product_name',
        'description',
        'standard_cost',
        'list_price',
        'category_id'
    ])

    for i in range(289, 289 + nr_recs):
        df.loc[i] = [
            i,  # product_id
            fake.word(),  # product_name
            fake.text(max_nb_chars=2000),  # description
            fake.random_int(min=1, max=1000),  # standard_cost
            fake.random_int(min=1001, max=2000),  # list_price
            fake.random_int(min=6, max=10)  # category_range
        ]

    return df


def gen_data_fake_customers(nr_recs: int):
    fake = Faker()

    df = pd.DataFrame(columns=[
        'customer_id',
        'name',
        'address',
        'website',
        'credit_limit'
    ])

    for i in range(320, 320 + nr_recs):
        df.loc[i] = [
            i,  # customer_id
            fake.company(),  # name
            fake.street_address(),  # address
            fake.url(),  # website
            fake.random_int(min=1, max=1000000)  # credit_limit
        ]

    return df

def gen_data_fake_contacts(nr_recs: int):
    fake = Faker()

    df = pd.DataFrame(columns=[
        'contact_id',
        'first_name',
        'last_name',
        'email',
        'phone',
        'customer_id'
    ])

    for i in range(320, 320 + nr_recs):
        df.loc[i] = [
            i,  # contact_id
            fake.first_name(),  # first_name
            fake.last_name(),  # last_name
            fake.email(),  # email
            fake.phone_number(),  # phone
            fake.random_int(min=320, max=320 + nr_recs - 1)  # customer_id
        ]

    return df




if __name__ == '__main__':

    print('everything will be fine')

    #df, data_list = gen_data_fake_regions(100)
    #print(df.head())
    #print(data_list)

    #countries = gen_data_fake_countries(20, 10 )
    #print(countries)

    #df = gen_data_fake_employees(10)
    #print(type(df['hire_date'].iloc[1]))
    #print(type(df['phone'].iloc[1]))
    #print(df['hire_date'].iloc[1])


    print(gen_data_fake_products(10))