import unittest
import os
import logging

import sqlalchemy as db
import sqlalchemy.sql.expression as sql
from sqlalchemy import column, text
from sqlalchemy.sql import func

from dataraven.connections import PostgresConnector, MySQLConnector

from dataraven.sql.core import build_select_query, build_aggregate_query
from dataraven.sql.helpers import apply_where_clause, format_from_clause, format_select_columns, compile_to_dialect
from dataraven.sql.measure_logic import measure_proportion_each_column, measure_set_duplication

from dataraven.data_quality_operators import SQLNullCheckOperator, SQLDuplicateCheckOperator, \
    SQLSetDuplicateCheckOperator, CSVNullCheckOperator, CSVDuplicateCheckOperator, CSVSetDuplicateCheckOperator, \
    CustomSQLDQOperator


class TestHarness(object):
    def __init__(self):
        # database credentials
        self.user = os.environ["user"]
        self.password = os.environ["password"]
        self.host = os.environ["host"]
        self.dbname = os.environ["dbname"]
        self.port = os.environ["port"]
        self.dbms = os.environ["dbms"]

        # sql test parameters
        self.from_clause = "test_schema.Orders"
        self.conn = self.connect_database()

        # csv test parameters
        self.path = "test_data/Orders_table.csv"

        # unittest paramters
        self.threshold = {"order_ts": 0, "product_id": 1, "price": 0.1}
        self.columns = ("order_ts", "product_id", "price")

        # logger function
        self.logger = self.init_logging().info

    def connect_database(self):
        if self.dbms.upper() == "POSTGRES":
            return PostgresConnector(self.user, self.password, self.host, self.dbname, self.port)

        elif self.dbms.upper() == "MYSQL":
            return MySQLConnector(self.user, self.password, self.host, self.dbname, self.port)

    @staticmethod
    def init_logging(logfile="unittests.log"):
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

    @staticmethod
    def strip_all_space(target):
        if not isinstance(target, str):
            target = str(target)
        target_ = target.replace('\n', '').replace('\t', '').replace(' ', '')
        return target_


th = TestHarness()


class TestDBConnector(unittest.TestCase):
    def test_execute(self):
        if th.dbms.upper() == "POSTGRES":
            cred = f"postgresql://{th.user}:{th.password}@{th.host}:{th.port}/{th.dbname}"
        elif th.dbms.upper() == "MYSQL":
            cred = f"mysql+pymysql://{th.user}:{th.password}@{th.host}:{th.port}/{th.dbname}"
        else:
            raise ValueError(f"expected dbms to be postgres or mysql but found {th.dbms}")

        engine = db.create_engine(cred)
        sa_conn = engine.connect()
        query = f"select * from {th.from_clause} order by 1 limit 1"
        target = sa_conn.execute(query).fetchall()[0]
        result = th.conn.execute(query).fetchall()[0]
        self.assertEqual(result, target)

    def test_fetch(self):
        if th.dbms.upper() == "POSTGRES":
            cred = f"postgresql://{th.user}:{th.password}@{th.host}:{th.port}/{th.dbname}"
        elif th.dbms.upper() == "MYSQL":
            cred = f"mysql+pymysql://{th.user}:{th.password}@{th.host}:{th.port}/{th.dbname}"
        else:
            raise ValueError(f"expected dbms to be postgres or mysql but found {th.dbms}")

        engine = db.create_engine(cred)
        sa_conn = engine.connect()
        query = f"select * from {th.from_clause} order by 1 limit 1"
        response = th.conn.execute(query)
        result = th.conn.fetch(response)[0]
        target = sa_conn.execute(query).fetchall()[0]
        self.assertEqual(result, target)


class TestSQLCore(unittest.TestCase):
    def test_build_sql_query(self):
        columns = ("col1", "col2")

        query1 = build_select_query(th.from_clause, *columns)
        query1_ = th.strip_all_space(query1).lower()
        query1_target = f"""
        SELECT col1, col2
        FROM {th.from_clause}
        """.lower()
        query1_target_ = th.strip_all_space(query1_target)
        self.assertEqual(query1_, query1_target_)

        where_clause2 = column("col3") > text("0")
        query2 = build_select_query(th.from_clause, *columns, where_clause=where_clause2)
        query2_ = th.strip_all_space(query2).lower()
        query2_target = f"""
        SELECT col1, col2
        FROM {th.from_clause}
        WHERE col3 > 0
        """.lower()
        query2_target_ = th.strip_all_space(query2_target)
        self.assertEqual(query2_, query2_target_)

        where_clause3 = [where_clause2, "col4 like 'A%'"]
        query3 = build_select_query(th.from_clause, *columns, where_clause=where_clause3)
        query3_ = th.strip_all_space(query3).lower()
        query3_target = f"""
        SELECT col1, col2
        FROM {th.from_clause}
        WHERE col3 > 0 AND col4 like 'A%'
        """.lower()
        query3_target_ = th.strip_all_space(query3_target)
        self.assertEqual(query3_, query3_target_)

    def test_build_aggregate_query(self):
        columns = ("col1", "col2")
        query1 = build_aggregate_query(th.from_clause, *columns, col3=func.count)
        query1_ = th.strip_all_space(query1).lower()
        query1_target = f"""
        SELECT col1, col2, count(col3) AS col3
        FROM {th.from_clause}
        GROUP BY col1, col2
        """.lower()
        query1_target_ = th.strip_all_space(query1_target)
        self.assertEqual(query1_, query1_target_)


class TestSQLHelpers(unittest.TestCase):
    def test_apply_where_clause(self):
        columns = ("col1", "col2")
        columns_ = list(map(lambda col: text(col), columns))
        query = sql.select(columns_).select_from(text(th.from_clause))

        query_target_ = f"""
        SELECT col1, col2
        FROM {th.from_clause}
        WHERE col2 > 0
        """.lower()
        query_target = th.strip_all_space(query_target_)

        where_clause1 = "col2 > 0"
        query1 = apply_where_clause(query, where_clause1)
        query1_ = th.strip_all_space(query1).lower()
        self.assertEqual(query1_, query_target)

        where_clause2 = text(where_clause1)
        query2 = apply_where_clause(query, where_clause2)
        query2_ = th.strip_all_space(query2).lower()
        self.assertEqual(query2_, query_target)

        where_clause3 = column("col2") > text("0")
        query3 = apply_where_clause(query, where_clause3)
        query3_ = th.strip_all_space(query3).lower()
        self.assertEqual(query3_, query_target)

    def test_format_from_clause(self):
        from_clause1 = th.from_clause
        from_clause1_ = format_from_clause(from_clause1)
        self.assertEqual(from_clause1_.__class__.__name__, "TextClause")
        self.assertEqual(str(from_clause1_), th.from_clause)

        from_clause2 = text(th.from_clause)
        from_clause2_ = format_from_clause(from_clause2)
        self.assertEqual(from_clause2_.__class__.__name__, "TextClause")
        self.assertEqual(str(from_clause2_), th.from_clause)

    def test_format_select_columns(self):
        columns1 = ("col1", "col2")
        columns1_ = format_select_columns(*columns1)
        self.assertTrue(all(map(lambda col: type(col).__name__ == "ColumnClause", columns1_)))

        columns2 = (text("col1"), text("col2"))
        columns2_ = format_select_columns(*columns2)
        self.assertTrue(all(map(lambda col: type(col).__name__ == "ColumnClause", columns2_)))

        columns3 = (column("col1"), column("col2"))
        columns3_ = format_select_columns(*columns3)
        self.assertTrue(all(map(lambda col: type(col).__name__ == "ColumnClause", columns3_)))

    def test_compile_to_dialect(self):
        columns = (text("col1"), text("col2"))
        query = sql.select(columns).select_from(text(th.from_clause))
        postgres_query = compile_to_dialect(query, "postgres")
        postgres_query_ = th.strip_all_space(postgres_query).lower()
        query_target_ = f"""
        SELECT col1, col2
        FROM {th.from_clause}
        """.lower()
        query_target = th.strip_all_space(query_target_)
        self.assertEqual(postgres_query_, query_target)


class TestSQLMeasureLogic(unittest.TestCase):
    maxDiff = None
    def test_measure_proportion_each_column(self):
        aggregate_func = func.count
        columns = ("col1", "col2")
        query = measure_proportion_each_column(th.from_clause, aggregate_func, *columns)
        query_ = th.strip_all_space(query).lower()
        query1_target = f"""
        SELECT 
        CASE 
            WHEN (t."1" > 0) THEN 1 - CAST(t.col1 AS FLOAT) / t."1" 
        END AS col1, 
        CASE 
            WHEN (t."1" > 0) THEN 1 - CAST(t.col2 AS FLOAT) / t."1" 
        END AS col2
        FROM (SELECT count(1) AS "1", count(col1) AS col1, count(col2) AS col2
        FROM {th.from_clause}) AS t
        """.lower()
        query1_target_ = th.strip_all_space(query1_target)
        self.assertEqual(query_, query1_target_)

    def test_measure_set_duplication(self):
        from_clause = th.from_clause
        columns = ("col1", "col2")
        query = measure_set_duplication(from_clause, *columns)
        query_ = th.strip_all_space(query).lower()
        query_target = f"""
        SELECT CASE WHEN (r."1" > 0) THEN 1 - CAST(u."1" AS FLOAT) / r."1" END AS "col1,col2"
        FROM (SELECT count(1) AS "1"
        FROM {th.from_clause}) AS r JOIN (SELECT count(1) AS "1"
        FROM (SELECT DISTINCT col1, col2
        FROM {th.from_clause}) AS t) AS u ON r."1" IS NOT NULL
        """.lower()
        query_target_ = th.strip_all_space(query_target)
        self.assertEqual(query_, query_target_)


class TestFetchQueryResults(unittest.TestCase):
    def test_execute_query(self):
        pass

    def test_fetch_results(self):
        pass


class TestOperations(unittest.TestCase):
    def test_parse_dict_param(self):
        pass

    def test_format_test_result_msgs(self):
        pass

    def test_build_test_outcomes(self):
        pass

    def test_log_test_results(self):
        pass

    def test_raise_execpetion_if_fail(self):
        pass

    def test_execute(self):
        pass


class TestSQLOperations(unittest.TestCase):
    def test_format_test_description(self):
        pass

    def test_calculate_measure_values(self):
        pass


class TestSQLSetOperations(unittest.TestCase):
    def test_format_test_description(self):
        pass


class TestCSVOperations(unittest.TestCase):
    def test_format_test_description(self):
        pass

    def test_calculate_measure_values(self):
        pass

    def test_build_measure_proportion_values(self):
        pass


class TestCSVSetOperations(unittest.TestCase):
    def test_format_test_description(self):
        pass


class TestCustomSQLOperations(unittest.TestCase):
    def test_format_test_description(self):
        pass

    def test_calcualte_test_results(self):
        pass

    def test_execute(self):
        pass


class TestSQLNullMeasure(unittest.TestCase):
    def test_build_measure_query(self):
        pass

    def test_factory(self):
        pass


class TestSQLNullTest(unittest.TestCase):
    def test_build_measure(self):
        pass

    def test_factory(self):
        pass


class TestCSVNullTest(unittest.TestCase):
    def test_build_measure(self):
        pass

    def test_factory(self):
        pass


class TestCustomTestFactory(unittest.TestCase):
    def test_factory(self):
        pass


class TestDataQualityOperators(unittest.TestCase):
    def test_SQLNullCheckOperator(self):
        results = SQLNullCheckOperator(th.conn, th.from_clause, th.threshold, *th.columns, logger=th.logger)\
            .test_results

        orders_ts_result = results["order_ts"]

        result = orders_ts_result["result"]
        measure = orders_ts_result["measure"]
        threshold = orders_ts_result["threshold"]
        self.assertEqual(result, "test_pass")
        self.assertEqual(measure, 0)
        self.assertEqual(threshold, th.threshold["order_ts"])

        num_outcomes = len(results)
        self.assertEqual(num_outcomes, 3)

        outcome_columns = list(results.keys()).sort()
        target_outcome_columns = list(th.columns).sort()
        self.assertEqual(outcome_columns, target_outcome_columns)

    def test_SQLDuplicateCheckOperator(self):
        results = SQLDuplicateCheckOperator(th.conn, th.from_clause, th.threshold, *th.columns, logger=th.logger)\
            .test_results

        orders_ts_result = results["order_ts"]

        result = orders_ts_result["result"]
        measure = orders_ts_result["measure"]
        threshold = orders_ts_result["threshold"]
        self.assertEqual(result, "test_fail")
        self.assertEqual(measure, 0.366)
        self.assertEqual(threshold, th.threshold["order_ts"])

        num_outcomes = len(results)
        self.assertEqual(num_outcomes, 3)

        outcome_columns = list(results.keys()).sort()
        target_outcome_columns = list(th.columns).sort()
        self.assertEqual(outcome_columns, target_outcome_columns)

    def test_SQLSetDuplicateCheckOperator(self):
        threshold_ = 0.1
        results = SQLSetDuplicateCheckOperator(th.conn, th.from_clause, threshold_, *th.columns, logger=th.logger)\
            .test_results

        column_label = ",".join(th.columns)
        test_result = results[column_label]

        result = test_result["result"]
        measure = test_result["measure"]
        threshold = test_result["threshold"]
        self.assertEqual(result, "test_pass")
        self.assertEqual(measure, 0.05800000000000005)
        self.assertEqual(threshold, threshold_)

        num_outcomes = len(results)
        self.assertEqual(num_outcomes, 1)

    def test_CSVNullCheckOperator(self):
        results = CSVNullCheckOperator(th.path, th.threshold, *th.columns, logger=th.logger).test_results

        orders_ts_result = results["order_ts"]

        result = orders_ts_result["result"]
        measure = orders_ts_result["measure"]
        threshold = orders_ts_result["threshold"]
        self.assertEqual(result, "test_pass")
        self.assertEqual(measure, 0)
        self.assertEqual(threshold, th.threshold["order_ts"])

        num_outcomes = len(results)
        self.assertEqual(num_outcomes, 3)

        outcome_columns = list(results.keys()).sort()
        target_outcome_columns = list(th.columns).sort()
        self.assertEqual(outcome_columns, target_outcome_columns)

    def test_CSVDuplicateCheckOperator(self):
        results = CSVDuplicateCheckOperator(th.path, th.threshold, *th.columns, logger=th.logger).test_results

        orders_ts_result = results["order_ts"]

        result = orders_ts_result["result"]
        measure = orders_ts_result["measure"]
        threshold = orders_ts_result["threshold"]

        self.assertEqual(result, "test_fail")
        self.assertEqual(measure, 0.362)
        self.assertEqual(threshold, th.threshold["order_ts"])

        num_outcomes = len(results)
        self.assertEqual(num_outcomes, 3)

        outcome_columns = list(results.keys()).sort()
        target_outcome_columns = list(th.columns).sort()
        self.assertEqual(outcome_columns, target_outcome_columns)

    def test_CSVSetDuplicateCheckOperator(self):
        threshold_ = 0.1
        results = CSVSetDuplicateCheckOperator(th.path, threshold_, *th.columns, logger=th.logger).test_results

        column_label = ",".join(th.columns)
        test_result = results[column_label]

        result = test_result["result"]
        measure = test_result["measure"]
        threshold = test_result["threshold"]
        self.assertEqual(result, "test_pass")
        self.assertEqual(measure, 0.058)
        self.assertEqual(threshold, threshold_)

        num_outcomes = len(results)
        self.assertEqual(num_outcomes, 1)

    def test_CustomSQLDQOperator(self):
        description1 = "{column} in table test_schema.Orders should not have more than 0.1 duplicate values"
        query1 = """
        select
        case
            when measure < {threshold} then 'test_pass'
            else 'test_fail'
        end as result,
        measure,
        '{column}' as column,
        {threshold} as threshold
        from
        (select
        case 
        when rows = 0 then NULL
        else 1 - cast(uniques as float)/rows
        end as measure
        from
        (select count(1) as rows, count(distinct {column}) as uniques
        from test_schema.Orders)t)tt
        """
        results = CustomSQLDQOperator(th.conn, query1, description1, *th.columns, threshold=th.threshold,
                                      logger=th.logger).test_results
        orders_ts_result = results["order_ts"]

        result = orders_ts_result["result"]
        measure = orders_ts_result["measure"]
        threshold = orders_ts_result["threshold"]
        self.assertEqual(result, "test_fail")
        self.assertEqual(measure, 0.366)
        self.assertEqual(float(threshold), th.threshold["order_ts"])

        num_outcomes = len(results)
        self.assertEqual(num_outcomes, 3)

        outcome_columns = list(results.keys()).sort()
        target_outcome_columns = list(th.columns).sort()
        self.assertEqual(outcome_columns, target_outcome_columns)

    def test_CustomSQLDQOperator_no_columns(self):
        description = "product_id in table test_schema.Orders should not have more than 0.1 duplicate values"
        query = """
        select
        case
            when measure < 0.1 then 'test_pass'
            else 'test_fail'
        end as result,
        measure,
        'product_id' as column,
        0.1 as threshold
        from
        (select
        case 
        when rows = 0 then NULL
        else 1 - cast(uniques as float)/rows
        end as measure
        from
        (select count(1) as rows, count(distinct product_id) as uniques
        from test_schema.Orders)t)tt
        """
        results = CustomSQLDQOperator(th.conn, query, description, logger=th.logger).test_results

        orders_ts_result = results["product_id"]

        result = orders_ts_result["result"]
        measure = orders_ts_result["measure"]
        threshold = orders_ts_result["threshold"]
        self.assertEqual(result, "test_fail")
        self.assertEqual(measure, 0.99)
        self.assertEqual(float(threshold), 0.1)

        num_outcomes = len(results)
        self.assertEqual(num_outcomes, 1)


if __name__ == "__main__":
    unittest.main()
