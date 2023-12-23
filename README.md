Project Epic#5: Creating Python Data Migration project


-----------------
Task 1) To define a working space using VSCode to interact with your data from both Oracle and PostgreSQL.

* Create a well-organized folder structure for your data engineering project in VSCode based on below template.
* Consider using a version control (e.g., Git, GitHub) to track changes in your project and collaborate with others.

data-migration-project/
│
├── config/
│   ├── config.ini               # Configuration file for login credentials
│
├── scripts/
│   ├── datagen/                 # Data generator scripts
│   │   ├── postgres_data.py     # Generator script for PostgreSQL
│   │   ├── oracle_data.py       # Generator script for Oracle
│   │
│   ├── checks/                  # Check scripts
│   │   ├── migration_check.py   # Check script for Oracle to Postgres migration
│   │
│   ├── sqls/                    # SQL scripts
│   │   ├── big_sql_join.sql     # A query to return data from joins of: CUSTOMERS,ORDERS, ORDER_ITEMS, PRODUCTS and PRODUCT_CATEGORIES
│   │
│   ├── utils/                   # Utility scripts
│   │   ├── database_utils.py    # Helper functions for database connections
│   │   ├── file_utils.py        # Helper functions for file operations
│   │
│   └── main.py                  # Main script to run your project processes
│
├── output/
│   ├── csv/                     # CSV output files
│   │   ├── postgres_stat.csv    # CSV file for PostgreSQL DB model statistical data
│   │   ├── oracle_stat.csv      # CSV file for Oracle DB model statistical data
│   │
│   ├── logs/                    # Log files for your data processes
│
├── requirements.txt             # List of project dependencies
│
└── README.md                    # Project documentation

-----------------
Task 2) To develop database connectivity and file utility scripts
* Place your Oracle/Postgres DB login credentials in /config/config.ini file. 
* Define a function in /utils/file_utils.py to load credentials and return them as of dictionary type.
* Define a function in /utils/database_utils.py to return a Oracle connection based on provided credentials dictionary variable.
* Define a function in /utils/database_utils.py to return a PostgreSQL connection based on provided credentials dictionary variable.

+ Hint: use "from dotenv import load_dotenv" to load enviroment and ini files.

-----------------
Task 3) To develop fake data generator utility scripts
* Define one or several functions in /scripts/datagen/oracle_data.py to create-delete-update rows in project tables using provided Oracle connection.
* Define one or several functions in /scripts/datagen/postgres_data.py to create-delete-update rows in project tables using provided Postgres connection.


-----------------
Task 4) To develop data exportation to local csv file utility function
* Define the query script in /sqls/big_sql_join.sql to return the set of customers and the products bought for a particular ORDER_DATE. 
* Define a function in /utils/database_utils.py to return a pandas dataframe from a given database based on the /sqls/big_sql_join.sql script return.
* Define one function in /scripts/utils/file_utils.py to save the pandas dataframe to a specific local path.
