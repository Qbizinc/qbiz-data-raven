import sqlalchemy as db
from sqlalchemy import text

from .exception_handling import try_except
from .log import get_null_logger


class DBConnector(object):
    def __init__(self, user, password, host, dbname, port, logger=None):
        """
        :param user: user name
        :param password: user password
        :param host: database host
        :param dbname: database name
        :param port: database port
        :param logger: optional logging function
        """
        self.user = user
        self.password = password
        self.host = host
        self.dbname = dbname
        self.port = port
        self.conn = None
        self.logger = logger if logger is not None else get_null_logger().debug

    def __get_credential(self):
        pass

    def __get_engine(self, credential):
        """
        :param credential: database login crediential
        :return: sqlalchemy.engine.base.Engine object
        """
        engine = db.create_engine(credential)
        return engine

    def get_conn(self, credential):
        """
        :param credential: database login crediential
        :return: sqlalchemy.engine.base.Connection object
        """
        @try_except(self.logger)
        def apply():
            engine = self.__get_engine(credential)
            conn = engine.connect()
            return conn
        return apply()

    def execute(self, query):
        """
        :param query: SQL query to be executed
        :return: sqlalchemy.engine.result.ResultProxy object
        """
        @try_except(self.logger)
        def apply():
            if isinstance(query, str):
                query_ = text(query)
            else:
                query_ = query
            response = self.conn.execute(query_)
            return response

        if self.conn is not None:
            return apply()
        else:
            raise AttributeError("expected sqlalchemy.engine.connect object but found None")

    def fetch(self, response):
        """
        :param response: sqlalchemy.engine.result.ResultProxy object
        :return: list of tuples where each tuple is a row from the query result
        """
        query_result = response.fetchall()
        return query_result


class PostgresConnector(DBConnector):
    def __init__(self, user, password, host, dbname, port, logger=None):
        super().__init__(user, password, host, dbname, port, logger=logger)
        credential = self.__get_credential()
        self.conn = self.get_conn(credential)
        self.dialect = "POSTGRES"

    def __get_credential(self):
        cred = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        return cred


class MySQLConnector(DBConnector):
    def __init__(self, user, password, host, dbname, port, logger=None):
        super().__init__(user, password, host, dbname, port, logger=logger)
        credential = self.__get_credential()
        self.conn = self.get_conn(credential)
        self.dialect = "MYSQL"

    def __get_credential(self):
        cred = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        return cred