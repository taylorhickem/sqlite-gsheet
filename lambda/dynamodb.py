""" this module provides template classes that conform to the interface patterns
"""
import re
from decimal import Decimal
from typing import Tuple
import datetime as dt
import pandas as pd
from boto3.dynamodb.conditions import Key, Attr
import boto3


DEFAULT_BATCH_MAX = 100
DIRECT_MAP = ['str', 'int']
REF_DATE = dt.datetime(1900, 1, 1)
DATETIME_FORMATS = {
    'datetime': '%Y-%m-%d %H:%M:%S',
    'date': '%Y-%m-%d',
    'time': '%H:%M:%S'
}


class DynamoDBTable(object):
    schema = {}
    columns = []
    table = None
    name = ''
    batch_max = DEFAULT_BATCH_MAX
    keys = {'partition_key': ''}

    def __repr__(self):
        return f'<DynamoDBTable:{self.name}>'

    def __init__(self, table_obj, name, partition_key, sort_key='', schema={}):
        self.table = table_obj
        self.name = name
        self.schema = schema
        self.columns = list(self.schema.keys())
        self.keys['partition_key'] = partition_key
        if sort_key != '':
            self.keys['sort_key'] = sort_key

    def _on_error(self, error='', exception=None):
        exception_message = ''
        if exception:
            exception_message = str(exception)
        if error != '':
            exception_message = error + ':' + exception_message
        print(exception_message)
        self.unload()

    def unload(self):
        self.table = None

    def is_loaded(self):
        return self.table is not None

    def as_items(self, df: pd.DataFrame) -> list:
        items = [dict(zip(self.columns, [None for i in range(len(self.columns))]))]
        if len(df) > 0:
            row_count = len(df)
            columns = df.to_dict(orient='series')
            for c in self.columns:
                columns[c] = dynamodb_format(self.schema[c], columns[c])
            items = [dict(zip(self.columns, [columns[c][i] for c in self.columns]))
                     for i in range(row_count)]
        return items

    def as_dataframe(self, items: list) -> pd.DataFrame:
        df = pd.DataFrame([], columns=self.columns)
        if items > 0:
            df = pd.DataFrame.from_records(items)
            for c in df.columns:
                df[c] = pandas_format(self.schema[c], df[c])
        return df

    def _items_insert(self, items: list):
        """ CREATE: inserts rows as Items into table.
            items should already be in standard format.
        """
        if len(items) > 0:
            try:
                with self.table.batch_writer() as batch:
                    for item in items:
                        batch.put_item(Item=item)
            except Exception as e:
                self._on_error(str(e))

    def rows_insert(self, rows: pd.DataFrame):
        """ CREATE: inserts rows as DataFrame into table
        """
        items = self.as_items(rows)
        self._items_insert(items)

    def query_by_decimal_value_eq(self, column_name, decimal_value, is_key=True, batch_max=None):
        if is_key:
            self.query_by_key_decimal_value_eq(column_name, decimal_value, batch_max=batch_max)
        else:
            self.query_by_attr_decimal_value_eq(column_name, decimal_value, batch_max=batch_max)

    def query_by_attr_decimal_value_eq(self, column_name, decimal_value, batch_max=None):
        filter_exp = Attr(column_name).eq(decimal_value)
        return self._query(filter_expression=filter_exp, batch_max=batch_max)

    def query_by_key_decimal_value_eq(self, column_name, decimal_value, batch_max=None):
        key_condition = Key(column_name).eq(decimal_value)
        return self._query(key_condition=key_condition, batch_max=batch_max)

    def _query(self, key_condition=None, filter_expression=None, batch_max=None) -> Tuple[list, list]:
        items = []
        keys = []
        response = {}
        start_key = 'start'
        batch = 0
        if batch_max is None:
            batch_max = self.batch_max
        while start_key != {} and batch <= batch_max:
            if start_key == 'start':
                if key_condition is not None and filter_expression is not None:
                    response = self.table.query(
                        KeyConditionExpression=key_condition,
                        FilterExpression=filter_expression
                    )
                elif key_condition is not None and filter_expression is None:
                    response = self.table.query(KeyConditionExpression=key_condition)
                elif key_condition is None and filter_expression is not None:
                    response = self.table.query(FilterExpression=filter_expression)
                else:
                    error_message = 'no query condition specified. Include argument for either KeyConditionExpression or FilterExpression'
                    self._on_error(error_message)
            else:
                if key_condition is not None and filter_expression is not None:
                    response = self.table.query(
                        KeyConditionExpression=key_condition,
                        FilterExpression=filter_expression,
                        ExclusiveStartKey=start_key
                    )
                elif key_condition is not None and filter_expression is None:
                    response = self.table.query(KeyConditionExpression=key_condition, ExclusiveStartKey=start_key)
                elif key_condition is None and filter_expression is not None:
                    response = self.table.query(FilterExpression=filter_expression, ExclusiveStartKey=start_key)
                else:
                    error_message = 'no query condition specified. Include argument for either KeyConditionExpression or FilterExpression'
                    self._on_error(error_message)
            if 'Items' in response:
                items = items + response['Items']
                if 'sort_key' in self.keys:
                    keys = keys + [{
                        self.keys['partition_key']: i[self.keys['partition_key']],
                        self.keys['sort_key']: i[self.keys['sort_key']]
                    } for i in response['Items']
                    ]
                else:
                    keys = keys + [{
                        self.keys['partition_key']: i[self.keys['partition_key']]
                    } for i in response['Items']
                    ]

            start_key = {}
            batch = batch + 1
            if 'LastEvaluatedKey' in response:
                start_key = response['LastEvaluatedKey']
        print(f'scan completed in {batch} batches out of max {batch_max}')
        return keys, items

    def scan(self) -> Tuple[list, list]:
        """ READ: returns the table as a list of Items.
        equivalent to SQL: SELECT * FROM name;
        """
        items = []
        keys = []
        start_key = 'start'
        batch_max = 50
        batch = 0
        while start_key != {} and batch <= batch_max:
            if start_key == 'start':
                response = self.table.scan()
            else:
                response = self.table.scan(ExclusiveStartKey=start_key)
            if 'Items' in response:
                items = items + response['Items']
                if 'sort_key' in self.keys:
                    keys = keys + [{
                        self.keys['partition_key']: i[self.keys['partition_key']],
                        self.keys['sort_key']: i[self.keys['sort_key']]
                    } for i in response['Items']
                    ]
                else:
                    keys = keys + [{
                        self.keys['partition_key']: i[self.keys['partition_key']]
                    } for i in response['Items']
                    ]

            start_key = {}
            batch = batch + 1
            if 'LastEvaluatedKey' in response:
                start_key = response['LastEvaluatedKey']
        print(f'scan completed in {batch} batches out of max {batch_max}')
        return keys, items

    def get_all_keys(self) -> list:
        keys, items = self.scan()
        return keys

    def get_table(self) -> pd.DataFrame:
        """ READ: returns the table as a DataFrame.
        equivalent to SQL: SELECT * FROM name;
        """
        keys, items = self.scan()
        df = self.as_dataframe(items)
        return df

    def _items_update(self, items: list, keys: list):
        """ UPDATE: takes input Item list rows and updates the rows from the database
             by the primary key specified by a sequential delete and insert.
        """
        self._items_delete(keys)
        self._items_insert(items)

    def rows_update(self, rows: pd.DataFrame, key: str):
        """ UPDATE: takes input pandas DataFrame rows and updates the rows from the database
             by the primary key specified
        """
        items = self.as_items(rows)
        keys = self.keys_from_rows(rows, key)
        self._items_update(items, keys)

    def delete_all(self):
        """ DELETE: deletes all rows in the table
        """
        keys = self.get_all_keys()
        self._items_delete(keys)

    def _items_delete(self, keys: list):
        """ DELETE: takes input list of keys and deletes the rows from the database
             by the primary keys specified. The keys is a list of dictionary that
             includes the partition and sort key reference.
        """
        if len(keys) > 0:
            try:
                with self.table.batch_writer() as batch:
                    for key in keys:
                        batch.delete_item(Key=key)
            except Exception as e:
                self._on_error(str(e))

    def keys_from_rows(self, rows: pd.DataFrame, partition_key: str) -> list:
        p_keys = rows[partition_key].to_list()
        if 'sort_key' in self.keys:
            sort_key = self.keys['sort_key']
            s_keys = rows[sort_key].to_list()
            keys = [{partition_key: p_keys[i], sort_key: s_keys[i]}
                    for i in range(len(p_keys))]
        else:
            keys = [{partition_key: pk} for pk in p_keys]
        return keys

    def rows_delete(self, rows: pd.DataFrame, key: str):
        """ DELETE: takes input pandas DataFrame rows and deletes the rows from the database
             by the primary key specified
        """
        keys = self.keys_from_rows(rows, key)
        self._items_delete(keys)


class DynamoDBAPI(object):
    client = None
    tables = {}
    table_names = []
    _connected = False
    config = {}

    def __init__(self, config={}):
        """ class constructor. after running this, should be able to run .connect()
        without passing any further specifications.
        """
        self.config = config

    def __on_error__(self, error='', exception=None):
        exception_message = ''
        if exception:
            exception_message = str(exception)
        if error != '':
            exception_message = error + ':' + exception_message
        print(exception_message)
        self.disconnect()

    def connect(self, **kwargs):
        """ creates a connection to the database. After running this
        the self._connected should be updated and if successful,
        should be able to directly perform CRUD operations on tables
        """
        success = self._client_load()
        if success:
            self._create_tables()
            self._connected = True

    def disconnect(self):
        self._unload_tables()
        self.client = None
        self._connected = False

    def _client_load(self):
        try:
            self.client = boto3.resource('dynamodb')
            return True
        except Exception as e:
            self.__on_error__(exception=e)
            return False

    def _unload_tables(self):
        for t in self.tables:
            self.tables[t].unload()
            self.tables[t] = None

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
        return table_name in self.tables

    def get_table_names(self) -> list:
        """ returns the available tables as a list of strings
        """
        return self.table_names

    def _create_tables(self):
        config = self.config['tables']
        self.table_names = config.keys()
        for t in self.table_names:
            table_config = config[t]
            table = self._create_table(t, table_config)
            self.tables[t] = table

    def _create_table(self, table_name: str, table_config={}) -> DynamoDBTable:
        columns = table_config['columns']
        sort_key = ''
        partition_key = table_config['partition_key']
        if 'sort_key' in table_config:
            sort_key = table_config['sort_key']

        ddb_table = self.client.Table(table_name)
        table = DynamoDBTable(ddb_table, table_name,
                              partition_key, sort_key=sort_key, schema=columns)
        return table

    def _get_table_obj(self, table_name: str) -> DynamoDBTable:
        if table_name in self.tables:
            return self.tables[table_name]

    def get_table(self, table_name: str) -> pd.DataFrame:
        """ READ: returns the table: table_name as a DataFrame.
        equivalent to SQL: SELECT * FROM table_name;
        """
        if table_name in self.tables:
            table = self._get_table_obj(table_name)
            df = table.get_table()
            return df

    def rows_insert(self, rows: pd.DataFrame, table_name: str):
        """ CREATE: inserts rows as DataFrame into table table_name
        """
        if self.table_exists(table_name):
            table = self._get_table_obj(table_name)
            table.rows_insert(rows)

    def rows_update(self, rows: pd.DataFrame, table_name: str, key: str):
        """ UPDATE: takes input pandas DataFrame rows and updates the rows from the database
             by the primary key specified
        """
        if self.table_exists(table_name):
            table = self._get_table_obj(table_name)
            table.rows_update(rows, key)

    def delete_all(self, table_name: str):
        """ DELETE: deletes all rows in the table
        """
        if self.table_exists(table_name):
            table = self._get_table_obj(table_name)
            table.delete_all()

    def rows_delete(self, rows: pd.DataFrame, table_name: str, key: str):
        """ DELETE: takes input pandas DataFrame rows and deletes the rows from the database
             by the primary key specified
        """
        if self.table_exists(table_name):
            table = self._get_table_obj(table_name)
            table.rows_delete(rows, key)


def pandas_format(column_spec: str, df_series: pd.Series) -> pd.Series:
    formatted = df_series.copy()
    if len(formatted) > 0:
        data_type, format_spec = extract_parenthesis(column_spec)
        if data_type not in DIRECT_MAP:
            if data_type in DATETIME_FORMATS:
                if format_spec == '':
                    format_spec = DATETIME_FORMATS[data_type]
                if data_type == 'time': # -> dt.timedelta
                    formatted = formatted.apply(
                        lambda x: dt.datetime.strptime(x, format_spec) - REF_DATE)
                else: # -> dt.datetime
                    formatted = formatted.apply(lambda x: dt.datetime.strptime(x, format_spec))

            elif data_type == 'decimal':
                    formatted = formatted.astype(float)

    return formatted


def dynamodb_format(column_spec: str, column_list: list) -> list:
    formatted = []
    if len(column_list) > 0:
        formatted = [dynamodb_format_value(column_spec, v) for v in column_list]
    return formatted


def dynamodb_format_value(column_spec: str, raw):
    formatted = raw.copy()
    data_type, format_spec = extract_parenthesis(column_spec)
    if data_type not in DIRECT_MAP:
        if data_type in DATETIME_FORMATS:
            if format_spec == '':
                format_spec = DATETIME_FORMATS[data_type]
            if isinstance(formatted, dt.timedelta):
                formatted = REF_DATE + formatted
            formatted = formatted.strftime(format_spec)

        elif data_type == 'decimal':
            if format_spec:
                formatted = round(int(format_spec))
            formatted_str = str(formatted)
            formatted = Decimal(formatted_str)

    return formatted


def extract_parenthesis(test_str:str):
    inner = ''
    regex_outer = r'(.*?)\((.*?)\)?'
    regex_inner = r'\(.*?\)'
    match_inner = re.findall(regex_inner, test_str)
    if match_inner:
        inner = match_inner[0][1:-1]
        match_outer = re.match(regex_outer, test_str)
        outer = match_outer.group(1)
    else:
        outer = test_str
    return outer, inner
