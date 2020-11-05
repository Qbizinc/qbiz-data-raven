import os
import logging

from dataraven.connections import PostgresConnector
from dataraven.data_quality_operators import SQLNullCheckOperator, SQLDuplicateCheckOperator, CustomSQLDQOperator,\
    CSVNullCheckOperator, CSVSetDuplicateCheckOperator


def init_logging(logfile="example_test.log"):
    # remove previous run log file
    if os.path.exists(logfile):
        os.remove(logfile)

    # create log message formatting
    format = "%(asctime)s | %(name)s | %(levelname)s | \n%(message)s\n"
    formatter = logging.Formatter(format)

    # create log level
    level = logging.DEBUG

    # create file handler
    handler = logging.FileHandler(logfile)

    # set log formatter and level
    handler.setFormatter(formatter)
    handler.setLevel(level)

    # create logger function
    logger = logging.getLogger(__name__)

    # set logger level and handler
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def main():
    # initialize logging
    logger = init_logging().info

    # database connection credentials
    user = os.environ["user"]
    password = os.environ["password"]
    host = os.environ["host"]
    dbname = os.environ["dbname"]
    port = os.environ["port"]

    # postgres database connector
    conn = PostgresConnector(user, password, host, dbname, port, logger=logger)

    # test thresholds
    threshold0 = 0
    threshold1 = 0.01
    threshold5 = 0.05
    threshold10 = 0.1

    ##### TEST ORDERS TABLE #####
    orders_from_clause = "test_schema.Orders"
    orders_where_clause = ["date(order_ts) = '2020-09-08'"]

    # test for duplicates
    orders_duplicates_test_column = "id"
    SQLDuplicateCheckOperator(conn, orders_from_clause, threshold0, orders_duplicates_test_column,
                              where=orders_where_clause, logger=logger)

    # test multiple columns using one threshold
    orders_null_test_columns = ("name", "product_id", "price")
    SQLNullCheckOperator(conn, orders_from_clause, threshold0, *orders_null_test_columns,
                         where=orders_where_clause, logger=logger)

    ##### TEST Contacts_table.csv #####

    #contacts_path = "../test_data/Contacts_table.csv"
    contacts_path = "test_data/Contacts_table.csv"

    # test first_name-last_name for duplicates
    contacts_duplicats_test_columns = ("first_name", "last_name")
    CSVSetDuplicateCheckOperator(contacts_path, threshold0, *contacts_duplicats_test_columns, logger=logger)

    # test email, state for null values
    contacts_null_columns = ("email", "country")
    contacts_null_threshold = {"email": threshold10, "country": 0.5}
    CSVNullCheckOperator(contacts_path, contacts_null_threshold, *contacts_null_columns, logger=logger)

    ##### TEST EARTHQUAKES TABLE #####
    # test magnitude is bounded above at 10
    magnitude_bounds_test_description = "Earthquakes.magnitude should be less than 10"
    magnitude_bounds_test_query = """
        select
            case
                when measure > 0 then 'test_fail'
                else 'test_pass'
            end as result,
            measure,
            0 as threshold
        from
        (select count(1) as measure
        from test_schema.Earthquakes
        where magnitude > 10)t
        """
    CustomSQLDQOperator(conn, magnitude_bounds_test_query, magnitude_bounds_test_description, logger=logger)

    # test columns for blank values
    earthquakes_columns = ("state", "epicenter", "date", "magnitude")
    earthquake_null_thresholds = {"state": threshold0, "epicenter": threshold5, "date": threshold1,
                                  "magnitude": threshold0}
    earthquake_col_not_blank_description = """{column} in table test_schema.Earthquakes should have fewer than 
    {threshold} BLANK values."""
    earthquake_col_not_blank_query = """
    select      
    case
        when measure is NULL then 'test_fail'
        when measure > {threshold} then 'test_fail'
        else 'test_pass'
    end as result,
    measure,
    {threshold} as threshold
    from
    (select 
    case when rows_ > 0 then cast(blank_cnt as float) / rows_ end as measure
    from
    (select 
    count(1) as rows_,
    sum(case when cast({column} as varchar) = '' then 1 else 0 end) as blank_cnt
    from test_schema.Earthquakes)t)tt
    """
    CustomSQLDQOperator(
        conn,
        earthquake_col_not_blank_query,
        earthquake_col_not_blank_description,
        *earthquakes_columns,
        threshold=earthquake_null_thresholds,
        logger=logger
    )


if __name__ == "__main__":
    main()
