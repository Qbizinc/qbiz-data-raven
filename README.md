# qbiz-data-raven

## Description
A toolbox of flexible database connectors and test methods used to measure data integrity of datasets and database
tables.
* Build data quality tests which can be inserted into an existing Python script or run as a stand-alone script.
* Send outcome notifications to messaging and logging applications.
* Halt pipelines and raise exceptions when needed.

## Prerequisites
Python 3.6+

sqlalchemy>=1.3.19

psycopg2

pymysql

## Installing
`pip install qbiz-data-raven`


## A simple data quality test script

In this example we build a script to test the `name`, `price` and `product_id` columns from the Postgres table `Orders`.
This table has the following DDL:
```buildoutcfg
create table Orders (
id int,
name varchar(50),
order_ts varchar(26),
product_id int,
price float
);
```

Here's the test script.
```buildoutcfg
import os

from qbizdataraven.connections import PostgresConnector
from qbizdataraven.data_quality_operators import SQLNullCheckOperator


def main():
    # initialize logging
    lazy_logger = lambda msg: print(msg + '\n')

    # database connection credentials
    user = os.environ["user"]
    password = os.environ["password"]
    host = os.environ["host"]
    dbname = os.environ["dbname"]
    port = os.environ["port"]

    # postgres database connector
    conn = PostgresConnector(user, password, host, dbname, port, logger=lazy_logger)
    dialect = "postgres"

    # test thresholds
    threshold0 = 0
    threshold1 = 0.01
    threshold5 = 0.05

    ##### TEST ORDERS TABLE #####
    # Table to be tested
    from_clause = "test_schema.Orders"

    # Conditional logic to be applied to input data
    date = "2020-09-08"
    where_clause = [f"date(order_ts) = '{date}'"]

    # Columns to be tested in target table
    columns = ("name", "product_id", "price")

    # Threshold value to be applied to each column
    threhold = {"name": threshold1, "product_id": threshold0, "price": threshold5}

    # Hard fail condition set on specific columns
    hard_fail = {"product_id": True}

    # Execute the null check test on each column in columns, on the above table
    SQLNullCheckOperator(conn, dialect, from_clause, threhold, *columns, where=where_clause, logger=lazy_logger,
                         hard_fail=hard_fail)


if __name__ == "__main__":
    main()
```

# Documentation
## Database Support
* Postgres
* MySQL

## Data Quality Tests
Data quality tests are used to measure the integrity of specified columns within a table or document. Every data
quality test will return `'test_pass'` or `'test_fail'` depending on the given measure and threshold.

### Data Quality Operators
Each operator will log the test results using the function passed in the `logger` parameter. If no logger is found then
these log messages will be swallowed.

Each operator has a `test_results` attribute which exposes the results from the underlying test. `test_results` is a
`dict` object with the following structure:
```buildoutcfg
{
    COLUMN NAME: {
        "result": 'test_pass' or 'test_fail',
        "measure": THE MEASURED VALUE OF COLUMN NAME,
        "threshold": THE THRESHOLD VALUE SPECIFIED FOR TEST,
        "result_msg": TEST RESULT MESSAGE
    }
}   
```

#### SQL Operators
All SQL operators have the following required parameters:
* `conn` - The database connection object.
* `dialect` - The SQL dialect for the given database. Accepted values are `postgres` or `mysql`.
* `from_` - The schema and table name of table to be tested.
* `threshold` - The threshold specified for a given test or collection of tests. This parameter can be numeric or a
`dict` object. If `threshold` is numeric then this value will be applied to all columns being tested by the operator.
If `threshold` is a `dict` then each `threshold` value will be referenced by column name. All columns being passed to the
operator must have a specified threshold value. If `threshold` is a `dict` it must have the following structure:
```buildoutcfg
{
    COLUMN NAME: NUMERIC VALUE
}
```
* `columns` - The column names entered as comma separated positional arguments.

All SQL operators have the following optional parameters:
* `logger` - The logging function. If None is passed then logged messages will be swallowed.
* `where` - Conditional logic to be applied to table specified in `from_`.
* `hard_fail` - Specifies if an operator which has a test which results in `'test_fail'` should terminate the current
process. This parameter
can be passed as a literal or a `dict` object. If `hard_fail` is set to `True` then every test being performed by the
given operator which results in `'test_fail'` will terminate the current process. If `hard_fail` is a `dict` object then
each `hard_fail` value will be referenced by column name. Only those columns with a `hard_fail` value of `True` will
terminate the process upon test failure. If `hard_fail` is a `dict` it must have the following structure:
```buildoutcfg
{
    COLUMN NAME: BOOLEAN VALUE
}
```
* `use_ansi` - If true then compile measure query to ANSI standards.

`SQLNullCheckOperator` - Test the proportion of null values for each column contained in `columns`.

`SQLDuplicateCheckOperator` - Test the proportion of duplicate values for each column contained in `columns`.

`SQLSetDuplicateCheckOperator` - Test the number of duplicate values across all columns passed to the `columns`
parameter simultaniously. This measure is equivalent to counting the number of rows returned from a `SELECT DISTINCT` on
all columns and dividing by the total number of rows.

#### CSV Operators
All CSV operators have the following required parameters:
* `from_` - The path to CSV file to be tested.
* `threshold` - Same as defined above for SQL operators.
* `columns` - the column names entered as comma separated positional arguments.

All CSV operators have the following optional parameters:
* `delimiter` -  The delimiter used to separate values specified in the file refeneced by the `from_` parameter.
* `hard_fail` - Same as defined above for SQL operators.
* `fieldnames` - A sequence of all column names for CSV file specified in `from_` parameter. To be used if the specified
file does not have column headers.
* `reducer_kwargs` - Key word arguments passed to the measure reducer function.

`CSVNullCheckOperator` - Test the proportion of `NULL` values for each column contained in `columns`.

`CSVDuplicateCheckOperator` - Test the proportion of duplicate values for each column contained in `columns`.

`CSVSetDuplicateCheckOperator` - Test the number of duplicate values across all columns passed to the `columns`
parameter simultaniously.

#### Custom Operators
`CustomSQLDQOperator` - Executes the test passed by the `custom_test` parameter on each column contained in `columns`.
The `CustomSQLDQOperator` class has the following required parameters:
* `conn` - The database connection object.
* `custom_test` - The SQL query to be executed. The `custom_test` query is required to return a column labeled `result`
which takes value `'test_pass'` or `'test_fail'`. The `custom_test` query should also return columns `measure`, which
provides the measured column value, and `threshold`, which gives the threshold used in the test. If these columns are
present then these values will be logged and returned in the `test_results` attribute. If `measure` and `threshold` are
not returned by the `custom_test` query then these values will be logged as `None`, and will be given in the
`test_results` attribute as `None`. `custom_test` can also be a query template with placeholders `{column}` and
`{threshold}` for variable column names and threshold values.  
* `description` - The description of the data quality test being performed. The description is may contain
placeholders `{column}` and `{threshold}` for the optional parameters `columns` and `threshold`, if they are passed
to the `CustomSQLDQOperator`. In this case then a test description will be generated for each `column` in `columns` and
for each value of `threshold`.

The `CustomSQLDQOperator` class has the following optional parameters:
* `columns` - a comma separated list of column arguments.
* `threhsold` - Same as defined above for SQL operators.
* `hard_fail` - Same as defined above for SQL operators.
* `test_desc_kwargs` - Key word arguments for formatting the test description.
