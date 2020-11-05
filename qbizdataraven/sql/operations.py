

class FetchQueryResults(object):
    def __init__(self, conn, query):
        """
        :param conn:
        :param query:
        """
        self.conn = conn
        self.query = query

        response = self.execute_query()
        self.results = self.fetch_results(response)

    def execute_query(self):
        resposne = self.conn.execute(self.query)
        return resposne

    def fetch_results(self, response):
        result = self.conn.fetch(response)[0]
        result_columns = response.keys()
        query_results = dict(zip(result_columns, result))
        return query_results

    def get_results(self):
        return self.results