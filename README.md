Project Epic#5: Creating Python Data Migration project


-----------------
Task 1) To define a working space using VSCode to interact with your data from both Oracle and PostgreSQL.

* Create a well-organized folder structure for your data engineering project.
* Consider using a version control (e.g., Git, GitHub) to track changes in your project and collaborate with others.


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
