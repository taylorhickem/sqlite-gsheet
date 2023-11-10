""" this module provides template classes that conform to the interface patterns
"""
from pandas import DataFrame

class DBConnection(object):
    _connection = {'engine': None, 'con': None}
    config = {}

    def __init__(self, config={}):
        """ class constructor. after running this, should be able to run .connect()
        without passing any further specifications.
        """
        self.config = config

    def __on_error__(self, error='', exception=None):
        pass

    def connect(self):
        """ creates a connection to the database. After running this
        the self._connection dictionary should be updated and
        should be able to directly perform CRUD operations on tables
        """
        pass

    def disconnect(self):
        pass

    def is_connected(self):
        """ returns the status of the database connection
        """
        pass
        return False

    def connection(self, **kwargs):
        return self._connection

    def table_exists(self, table_name: str) -> bool:
        pass
        return False

    def get_table_names(self) -> list:
        """ returns the available tables as a list of strings
        """
        table_names = []
        return table_names

    def rows_insert(rows: DataFrame, table_name: str):
        """ CREATE: inserts rows as DataFrame into table table_name
        """
        pass

    def get_table(self, table_name: str) -> DataFrame:
        """ READ: returns the table: table_name as a DataFrame.
        equivalent to SQL: SELECT * FROM table_name;
        """
        df = DataFrame([])
        return df

    def rows_update(rows: DataFrame, table_name: str, key: str):
        """ UPDATE: takes input pandas DataFrame rows and updates the rows from the database
             by the primary key specified
        """
        pass

    def delete_all(table_name: str):
        """ DELETE: deletes all rows in the table
        """
        pass

    def rows_delete(rows: DataFrame, table_name: str, key: str):
        """ DELETE: takes input pandas DataFrame rows and deletes the rows from the database
             by the primary key specified
        """
        pass
