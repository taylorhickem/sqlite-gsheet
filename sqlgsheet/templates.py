""" this module provides template classes that conform to the interface patterns
"""
from pandas import DataFrame

class DBConnection(object):
    _connected = False
    config = {}

    def __init__(self, config={}):
        """ class constructor. after running this, should be able to run .connect()
        without passing any further specifications.
        """
        self.config = config

    def __on_error__(self, error='', exception=None):
        pass

    def connect(self, **kwargs):
        """ creates a connection to the database. After running this
        the self._connected should be updated and if successful,
        should be able to directly perform CRUD operations on tables
        """
        self._connected = True

    def disconnect(self):
        pass

    def is_connected(self):
        """ returns the status of the database connection
        """
        return self._connected

    def connection(self, **kwargs) -> dict:
        """ returns a connection dictionary with a copy of self for 'eng' and 'con'
            first check if connected, if not then attempt to open a connection
        """
        if not self.is_connected():
            self.connect(**kwargs)
        con_dict = {'engine': self, 'con': self}
        return con_dict

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
