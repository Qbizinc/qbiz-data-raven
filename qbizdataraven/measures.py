import abc

from sqlalchemy.sql import func, distinct

from .sql.helpers import compile_to_dialect
from .sql.measure_logic import measure_proportion_each_column, measure_set_duplication

from .csv.reducers import measure_null, measure_duplicates, measure_set_duplicates


class Measure(object):
    pass


class SQLMeasure(Measure):
    def __init__(self, dialect, from_, query, *columns):
        """
        :param dialect: The SQL dialect for the given database
        :param from_: Schema and table name of table to be tested
        :param query: The SQL query used to calculate measure value
        :param columns: The column names entered as comma separated positional arguments
        """
        self.columns = columns
        self.query = query
        self.from_ = from_
        self.dialect = dialect


class CSVMeasure(Measure):
    def __init__(self, delimiter, from_, reducer, *columns):
        """
        :param delimiter: Separator used in file specified by the `from_` parameter
        :param from_: Path to CSV file
        :param reducer: Function used to calculate measure value
        :param columns: The column names entered as comma separated positional arguments
        """
        self.columns = columns
        self.from_ = from_
        self.reducer = reducer
        self.delimiter = delimiter


class MeasureFactory(object):
    @abc.abstractmethod
    def factory(self): pass


class SQLMeasureFactory(MeasureFactory):
    def __init__(
            self,
            dialect,
            from_,
            *columns,
            where=None,
            use_ansi=True
    ):
        """
        :param dialect: The SQL dialect for the given database
        :param from_: Schema and table name of table to be tested
        :param columns: The column names entered as comma separated positional arguments
        :param where: Conditional logic to be applied to table specified by from_
        :param use_ansi: Boolean to specify if SQL query should be complied to ANSI standards
        """
        self.dialect = dialect
        self.from_ = from_
        self.columns = columns
        self.where = where
        self.use_ansi = use_ansi

    @staticmethod
    def compile_dialect(query, dialect, use_ansi):
        query_ = compile_to_dialect(query, dialect, use_ansi=use_ansi)
        return query_

    @abc.abstractmethod
    def build_measure_query(self):
        pass

    def factory(self):
        query = self.build_measure_query()
        query_ = self.compile_dialect(query, self.dialect, self.use_ansi)
        return SQLMeasure(self.dialect, self.from_, query_, *self.columns)


class SQLNullMeasure(SQLMeasureFactory):
    def __init__(self, dialect, from_, *columns, where=None, use_ansi=True):
        super().__init__(dialect, from_, *columns, where=where, use_ansi=use_ansi)

    def build_measure_query(self):
        aggregate_func = func.count
        measure_query = measure_proportion_each_column(
            self.from_,
            aggregate_func,
            *self.columns,
            where_clause=self.where
        )
        return measure_query


class SQLDuplicateMeasure(SQLMeasureFactory):
    def __init__(self, dialect, from_, *columns, where=None, use_ansi=True):
        super().__init__(dialect, from_, *columns, where=where, use_ansi=use_ansi)

    def build_measure_query(self):
        def aggregate_func(column):
            return func.count(distinct(column))

        measure_query = measure_proportion_each_column(
            self.from_,
            aggregate_func,
            *self.columns,
            where_clause=self.where
        )
        return measure_query


class SQLSetDuplicateMeasure(SQLMeasureFactory):
    def __init__(self, dialect, from_, *columns, where=None, use_ansi=True):
        super().__init__(dialect, from_, *columns, where=where, use_ansi=use_ansi)

    def build_measure_query(self):
        measure_query = measure_set_duplication(self.from_, *self.columns, where_clause=self.where)
        return measure_query


class CSVMeasureFactory(MeasureFactory):
    def __init__(self, from_, *columns, delimiter=','):
        """
        :param from_: Path to CSV file
        :param columns: The column names entered as comma separated positional arguments
        :param delimiter: Separator used in file specified by the `from_` parameter
        """
        self.from_ = from_
        self.columns = columns
        self.delimiter = delimiter

    @abc.abstractmethod
    def build_reducer(self):
        pass

    def factory(self):
        reducer = self.build_reducer()
        return CSVMeasure(self.delimiter, self.from_, reducer, *self.columns)


class CSVNullMeasure(CSVMeasureFactory):
    def __init__(self, from_, *columns, delimiter=','):
        super().__init__(from_, *columns, delimiter=delimiter)

    def build_reducer(self):
        return measure_null


class CSVDuplicateMeasure(CSVMeasureFactory):
    def __init__(self, from_, *columns, delimiter=','):
        super().__init__(from_, *columns, delimiter=delimiter)

    def build_reducer(self):
        return measure_duplicates


class CSVSetDuplicateMeasure(CSVMeasureFactory):
    def __init__(self, from_, *columns, delimiter=','):
        super().__init__(from_, *columns, delimiter=delimiter)

    def build_reducer(self):
        return measure_set_duplicates
