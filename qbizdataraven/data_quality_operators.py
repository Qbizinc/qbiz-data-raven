import abc

from .log import get_null_logger
from .tests import CustomTestFactory, SQLNullTest, SQLDuplicateTest, SQLSetDuplicateTest, CSVNullTest, \
    CSVDuplicateTest, CSVSetDuplicateTest

from .operations import SQLOperations, SQLSetOperations, CSVOperations, CSVSetOperations, CustomSQLOperations


class DQOperator(object):
    def __init__(self, logger=None):
        self.logger = logger if logger is not None else get_null_logger().info

    @abc.abstractmethod
    def build_test(self): pass


class SQLDQOperator(DQOperator):
    def __init__(
            self,
            conn,
            from_,
            threshold,
            *columns,
            logger=None,
            where=None,
            hard_fail=None,
            use_ansi=True
    ):
        """
        :param conn: Database connection object
        :param from_: Schema and table name of table to be tested
        :param threshold: Numeric or dict to specify the threshold for a given test or collection of tests
        :param columns: The column names entered as comma separated positional arguments
        :param logger: Optional logging function. If None is passed then logged messages will be swallowed
        :param where: Conditional logic to be applied to table specified in `from_`
        :param hard_fail: Boolean or dict to specify if test failure should result in terminating the current process
        :param use_ansi: Boolean to specify if SQL query should be complied to ANSI standards
        """
        super().__init__(logger=logger)
        self.conn = conn
        self.dialect = conn.dialect
        self.threshold = threshold
        self.from_ = from_
        self.columns = columns
        self.where = where
        self.hard_fail = hard_fail
        self.use_ansi = use_ansi

        self.test_results = self.execute()

    @abc.abstractmethod
    def build_test(self): pass

    def execute(self):
        test = self.build_test()
        operator = SQLOperations(self.conn, self.logger, test)
        test_results = operator.execute()
        return test_results


class SQLNullCheckOperator(SQLDQOperator):
    def __init__(
            self,
            conn,
            from_,
            threshold,
            *columns,
            logger=None,
            where=None,
            hard_fail=None,
            use_ansi=True
    ):
        """
        :param conn: Database connection object
        :param from_: Schema and table name of table to be tested
        :param threshold: Numeric or dict to specify the threshold for a given test or collection of tests
        :param columns: The column names entered as comma separated positional arguments
        :param logger: Optional logging function. If None is passed then logged messages will be swallowed
        :param where: Conditional logic to be applied to table specified in `from_`
        :param hard_fail: Boolean or dict to specify if test failure should result in terminating the current process
        :param use_ansi: Boolean to specify if SQL query should be complied to ANSI standards
        """
        super().__init__(conn, from_, threshold, *columns, logger=logger, where=where, hard_fail=hard_fail,
                         use_ansi=use_ansi)

    def build_test(self):
        test = SQLNullTest(self.dialect, self.from_, self.threshold, *self.columns, where=self.where,
                           hard_fail=self.hard_fail, use_ansi=self.use_ansi).factory()
        return test


class SQLDuplicateCheckOperator(SQLDQOperator):
    def __init__(
            self,
            conn,
            from_,
            threshold,
            *columns,
            logger=None,
            where=None,
            hard_fail=None,
            use_ansi=True
    ):
        """
        :param conn: Database connection object
        :param from_: Schema and table name of table to be tested
        :param threshold: Numeric or dict to specify the threshold for a given test or collection of tests
        :param columns: The column names entered as comma separated positional arguments
        :param logger: Optional logging function. If None is passed then logged messages will be swallowed
        :param where: Conditional logic to be applied to table specified in `from_`
        :param hard_fail: Boolean or dict to specify if test failure should result in terminating the current process
        :param use_ansi: Boolean to specify if SQL query should be complied to ANSI standards
        """
        super().__init__(conn, from_, threshold, *columns, logger=logger, where=where, hard_fail=hard_fail,
                         use_ansi=use_ansi)

    def build_test(self):
        test = SQLDuplicateTest(self.dialect, self.from_, self.threshold, *self.columns, where=self.where,
                                hard_fail=self.hard_fail, use_ansi=self.use_ansi).factory()
        return test


class SQLSetDuplicateCheckOperator(SQLDQOperator):
    def __init__(
            self,
            conn,
            from_,
            threshold,
            *columns,
            logger=None,
            where=None,
            hard_fail=None,
            use_ansi=True
    ):
        """
        :param conn: Database connection object
        :param from_: Schema and table name of table to be tested
        :param threshold: Numeric or dict to specify the threshold for a given test or collection of tests
        :param columns: The column names entered as comma separated positional arguments
        :param logger: Optional logging function. If None is passed then logged messages will be swallowed
        :param where: Conditional logic to be applied to table specified in `from_`
        :param hard_fail: Boolean or dict to specify if test failure should result in terminating the current process
        :param use_ansi: Boolean to specify if SQL query should be complied to ANSI standards
        """
        super().__init__(conn, from_, threshold, *columns, logger=logger, where=where, hard_fail=hard_fail,
                         use_ansi=use_ansi)

    def build_test(self):
        test = SQLSetDuplicateTest(self.dialect, self.from_, self.threshold, *self.columns, where=self.where,
                                   hard_fail=self.hard_fail, use_ansi=self.use_ansi).factory()
        return test

    def execute(self):
        test = self.build_test()
        operator = SQLSetOperations(self.conn, self.logger, test)
        test_results = operator.execute()
        return test_results


class CSVDQOperator(DQOperator):
    def __init__(
            self,
            from_,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None,
            logger=None,
            **reducer_kwargs
    ):
        """
        :param from_: Path to CSV file to be tested
        :param threshold: Numeric or dict to specify the threshold for a given test or collection of tests
        :param columns: Column names entered as comma separated positional arguments
        :param delimiter: Separator used in file specified by the `from_` parameter
        :param hard_fail: Boolean or dict to specify if test failure should result in terminating the current process
        :param fieldnames: Sequence of all columns in CSV specified by `from_`. To be used when no headers exist in file
        :param logger: Optional logging function. If None is passed then logged messages will be swallowed
        :param reducer_kwargs: Key word arguments passed to the measure reducer function
        """
        super().__init__(logger=logger)
        self.from_ = from_
        self.threshold = threshold
        self.columns = columns
        self.delimiter = delimiter
        self.hard_fail = hard_fail
        self.fieldnames = fieldnames
        self.reducer_kwargs = reducer_kwargs

        self.test_results = self.execute()

    @abc.abstractmethod
    def build_test(self): pass

    def execute(self):
        test = self.build_test()
        operator = CSVOperations(self.logger, test, fieldnames=self.fieldnames, **self.reducer_kwargs)
        test_results = operator.execute()
        return test_results


class CSVNullCheckOperator(CSVDQOperator):
    def __init__(
            self,
            from_,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None,
            logger=None

    ):
        """
        :param from_: Path to CSV file to be tested
        :param threshold: Numeric or dict to specify the threshold for a given test or collection of tests
        :param columns: Column names entered as comma separated positional arguments
        :param delimiter: Separator used in file specified by the `from_` parameter
        :param hard_fail: Boolean or dict to specify if test failure should result in terminating the current process
        :param fieldnames: Sequence of all columns in CSV specified by `from_`. To be used when no headers exist in file
        :param logger: Optional logging function. If None is passed then logged messages will be swallowed
        """
        super().__init__(from_, threshold, *columns, delimiter=delimiter, hard_fail=hard_fail, fieldnames=fieldnames,
                         logger=logger)

    def build_test(self):
        test = CSVNullTest(self.from_, self.threshold, *self.columns, delimiter=self.delimiter,
                           hard_fail=self.hard_fail).factory()
        return test


class CSVDuplicateCheckOperator(CSVDQOperator):
    def __init__(
            self,
            from_,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None,
            logger=None
    ):
        """
        :param from_: Path to CSV file to be tested
        :param threshold: Numeric or dict to specify the threshold for a given test or collection of tests
        :param columns: Column names entered as comma separated positional arguments
        :param delimiter: Separator used in file specified by the `from_` parameter
        :param hard_fail: Boolean or dict to specify if test failure should result in terminating the current process
        :param fieldnames: Sequence of all columns in CSV specified by `from_`. To be used when no headers exist in file
        :param logger: Optional logging function. If None is passed then logged messages will be swallowed
        """
        super().__init__(from_, threshold, *columns, delimiter=delimiter, hard_fail=hard_fail, fieldnames=fieldnames,
                         logger=logger)

    def build_test(self):
        test = CSVDuplicateTest(self.from_, self.threshold, *self.columns, delimiter=self.delimiter,
                                hard_fail=self.hard_fail).factory()
        return test


class CSVSetDuplicateCheckOperator(CSVDQOperator):
    def __init__(
            self,
            from_,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None,
            logger=None
    ):
        """
        :param from_: Path to CSV file to be tested
        :param threshold: Numeric or dict to specify the threshold for a given test or collection of tests
        :param columns: Column names entered as comma separated positional arguments
        :param delimiter: Separator used in file specified by the `from_` parameter
        :param hard_fail: Boolean or dict to specify if test failure should result in terminating the current process
        :param fieldnames: Sequence of all columns in CSV specified by `from_`. To be used when no headers exist in file
        :param logger: Optional logging function. If None is passed then logged messages will be swallowed
        """
        super().__init__(from_, threshold, *columns, delimiter=delimiter, hard_fail=hard_fail, fieldnames=fieldnames,
                         logger=logger)

    def build_test(self):
        test = CSVSetDuplicateTest(self.from_, self.threshold, *self.columns, delimiter=self.delimiter,
                                   hard_fail=self.hard_fail).factory()
        return test

    def execute(self):
        test = self.build_test()
        operator = CSVSetOperations(self.logger, test, fieldnames=self.fieldnames, **self.reducer_kwargs)
        test_results = operator.execute()
        return test_results


class CustomSQLDQOperator(DQOperator):
    def __init__(
            self,
            conn,
            custom_test,
            description,
            *columns,
            threshold=None,
            hard_fail=None,
            logger=None,
            **test_desc_kwargs
    ):
        """
        :param conn: Database connection object
        :param custom_test: SQL query used to execute the data quality test
        :param description: Description of the test given in `custom_test`
        :param columns: Column names entered as comma separated positional arguments
        :param threshold: Numeric or dict to specify the threshold for a given test or collection of tests
        :param hard_fail: Boolean or dict to specify if test failure should result in terminating the current process
        :param logger: Optional logging function. If None is passed then logged messages will be swallowed
        :param test_desc_kwargs: Key word arguments passed to the test description formatter
        """
        super().__init__(logger=logger)
        self.conn = conn
        self.description = description
        self.custom_test = custom_test
        self.columns = columns
        self.threshold = threshold
        self.hard_fail = hard_fail
        self.test_desc_kwargs = test_desc_kwargs

        self.test_results = self.execute()

    def build_test(self):
        test = CustomTestFactory(self.description, self.custom_test, *self.columns, threshold=self.threshold,
                                 hard_fail=self.hard_fail).factory()
        return test

    def execute(self):
        test = self.build_test()
        operator = CustomSQLOperations(self.conn, self.logger, test, **self.test_desc_kwargs)
        test_results = operator.execute()
        return test_results
