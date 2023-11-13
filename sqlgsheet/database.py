''' interface with data sources
'''
# -----------------------------------------------------
# Import
# -----------------------------------------------------
import os
import sys
import shutil
import json
import pandas as pd
import datetime as dt
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import MetaData
from sqlalchemy.sql.expression import bindparam
from sqlalchemy import delete
from sqlalchemy import update
from sqlgsheet import gsheet as gs
from sqlgsheet import gdrive as gd
from sqlgsheet import fso
from sqlgsheet import mysql

##-----------------------------------------------------
# Module variables
##-----------------------------------------------------
# constants
DB_SOURCE = 'local'    # remote=MySQL, local=sqlite, generic=templates.DBConnection
PATH_GSHEET_CONFIG = 'gsheet_config.json'
PATH_DB_CONFIG = 'db_config.json'
NUMERIC_TYPES = ['int', 'float']
SQL_DB_NAME = 'sqlite:///myapp.db'
SQL_DATA_TYPES = {'INTEGER()':'int',
                  'REAL()':'float',
                  'DATE()':'date',
                  'TEXT()':'str',
                  'CHAR(':'str',
                  'DATE()': 'date',
                  'DATETIME()': 'datetime'
                  }


# dynamic : config
GSHEET_CONFIG = {}
DB_CONFIG = {}
LOCAL_DIR = os.path.abspath(os.path.dirname(__file__))

# custom class objects from other modules
engine = None
gs_engine = None
con = None


# -----------------------------------------------------
# Setup
# -----------------------------------------------------
def load(generic_con_class=None, db_config={}):
    load_config(db_config=db_config)
    load_sql(generic_con_class=generic_con_class)
    load_gsheet()


def load_client_secret(client_secret):
    file_path = os.path.join(LOCAL_DIR, gs.CLIENT_SECRET_FILE)
    with open(file_path, 'w') as f:
        json.dump(client_secret, f)
    f.close()


def load_config(db_config={}):
    global CONFIG, GSHEET_CONFIG, DB_CONFIG
    GSHEET_CONFIG = json.load(open(PATH_GSHEET_CONFIG))
    if not db_config:
        db_config = json.load(open(PATH_DB_CONFIG))
    DB_CONFIG = db_config


def load_gsheet():
    global gs_engine
    if gs_engine is None:
        gs_engine = gs.SheetsEngine()


def set_user_data(gsheet_config: Optional[str] = None,
                  client_secret: Optional[str] = None,
                  mysql_credentials: Optional[str] = None,
                  db_config: Optional[str] = None) -> None:
    global PATH_GSHEET_CONFIG, PATH_DB_CONFIG
    if client_secret:
        gs.CLIENT_SECRET_FILE = client_secret
        gd.CLIENT_SECRET_FILE = client_secret
    if mysql_credentials:
        mysql.PATH_MYSQL_CRED = mysql_credentials
    if gsheet_config:
        PATH_GSHEET_CONFIG = gsheet_config
    if db_config:
        PATH_DB_CONFIG = db_config


# -----------------------------------------------------
# SQL
# -----------------------------------------------------
def unload_sql():
    global engine, con, inspector, table_names
    table_names = []
    inspector = None
    engine = None
    con = None


def sql_config(db_source, sqlite_db_name=None, db_config={}):
    global DB_SOURCE, SQL_DB_NAME, DB_CONFIG
    DB_SOURCE = db_source
    if db_config:
        DB_CONFIG = db_config
    if sqlite_db_name:
        SQL_DB_NAME = sqlite_db_name


def load_sql(generic_con_class=None):
    global engine, inspector, table_names, con
    if engine is None:
        if DB_SOURCE == 'remote': # MySQL
            mysql.load()
            engine = mysql.engine
            con = mysql.con

        elif DB_SOURCE == 'local': # sqlite
            connect = _sqlite_connection(database=SQL_DB_NAME)
            engine = connect['engine']
            con = connect['con']

        inspector = Inspector.from_engine(engine)
        table_names = inspector.get_table_names()

        if DB_SOURCE == 'generic': #templates.DBConnection
            con = generic_con_class(DB_CONFIG)
            con.connect()
            engine = con
            table_names = con.get_table_names()


def table_exists(tableName):
    return tableName in table_names


def is_sqlalchemy_con(con_obj):
    is_class = isinstance(con_obj, Engine) or isinstance(con_obj, Connection)
    return is_class


def get_table(table_name, con=None):
    check_exists = True
    if not con:
        check_exists = table_exists(table_name)
        con=engine
    if check_exists:
        if is_sqlalchemy_con(con):
            tbl = pd.read_sql_table(table_name, con=con)
        else:
            tbl = con.get_table(table_name)
    else:
        tbl = None
    return tbl


def update_table(tbl, tblname, append=True):
    if is_sqlalchemy_con(engine):
        if append:
            ifex = 'append'
        else:
            ifex = 'replace'
        tbl.to_sql(tblname, con=engine, if_exists=ifex, index=False)
    else:
        if not append:
            engine.delete_all(tblname)
        engine.rows_insert(tbl, tblname)


def db_connection(db_type, con_obj=None, spec={}) -> dict:
    connect = {}
    if db_type == 'sqlite':
        connect = _sqlite_connection(**spec)
    elif db_type == 'mysql':
        connect = mysql.get_connection(**spec)
    elif db_type == 'generic':
        if con_obj is None:
            con_obj = con
        connect = con_obj.connection(**spec)
    else:
        raise ValueError(f'unrecognized db_type:{db_type}. Allowed  [sqlite, mysql, generic]')
    return connect


def _sqlite_connection(database='') -> dict:
    connect = {}
    try:
        connect['engine'] = create_engine(database, echo=False)
        connect['con'] = connect['engine'].connect()
    except Exception as e:
        print(f'ERROR: unable to connect to database {e}')
    return connect


def rows_insert(rows, table_name, con=None):
    if con is None:
        con = con
    if is_sqlalchemy_con(con):
        rows.to_sql(table_name, con=con, if_exists='append', index=False)
    else:
        con.rows_insert(rows, table_name)


def rows_delete(rows, table_name, key='index', eng=None):
    if eng is None:
        eng = engine
    if is_sqlalchemy_con(eng):
        md = MetaData(bind=eng)
        md.reflect()
        if table_name in md.tables:
            if key == 'index':
                keys = list(rows.index)
            else:
                keys = list(rows[key])

            table = md.tables[table_name]
            stmt = delete(table).\
                where(table.c[key].in_(keys))
            eng.connect().execute(stmt)
    else:
        eng.rows_delete(rows, table_name, key)


def rows_update(rows, table_name, key='index', eng=None):
    if eng is None:
        eng = engine
    if is_sqlalchemy_con(eng):
        u_rows = rows.copy()
        md = MetaData(bind=eng)
        md.reflect()
        if table_name in md.tables:
            if key == 'index':
                u_rows.reset_index(inplace=True)
            u_rows.rename(columns={key: '_' + key}, inplace=True)
            row_values = u_rows.to_dict(orient='records')

            table = md.tables[table_name]
            stmt = update(table).\
                where(table.c[key] == bindparam('_' + key))
            eng.connect().execute(stmt, row_values)
    else:
        eng.rows_update(rows, table_name, key)

# -----------------------------------------------------
# Google spreadsheet
# -----------------------------------------------------

def get_sheet(wkb_name, rng_code, include_values=True):
    ''' get a table from a range in a gsheet as a pandas DataFrame

    :param wkb_name: spreadsheet label
    :param rng_code: table range label
    :param include_values: (optional) set to False to return an empty table with just the header
    :type wkb_name: str
    :type rng_code: str
    :type include_values: bool
    :return: table with a header and values
    :rtype: pd.DataFrame
    '''
    WKB_CONFIG = GSHEET_CONFIG[wkb_name]
    wkbid = WKB_CONFIG['wkbid']
    rng_config = WKB_CONFIG['sheets'][rng_code]
    rngid = rng_config['data']
    hdrid = rng_config['header']
    valueList = gs_engine.get_rangevalues(wkbid, rngid)
    header = gs_engine.get_rangevalues(wkbid, hdrid)[0]
    if include_values:
        rng = pd.DataFrame(valueList, columns=header)
        if 'data_types' in rng_config:
            data_types = rng_config['data_types']
            for field in data_types:
                typeId = data_types[field]
                if not typeId in ['str', 'date']:
                    if typeId in NUMERIC_TYPES:
                        # to deal with conversion from '' to nan
                        if typeId in ['float']:  # nan compatible
                            rng[field] = pd.to_numeric(rng[field]).astype(typeId)
                        else:  # nan incompatible types
                            rng[field] = pd.to_numeric(rng[field]).fillna(0).astype(typeId)
                    else:
                        rng[field] = rng[field].astype(typeId)
                if typeId == 'date':
                    if 'date_format' in rng_config:
                        rng[field] = rng[field].apply(
                            lambda x: dt.datetime.strptime(x, rng_config['date_format']))
    else:
        rng = pd.DataFrame([], columns=header)

    return rng


def post_to_gsheet(df, wkb_name, rng_code, input_option='RAW'):
    ''' post pandas DataFrame table to a range in a gsheet

    :param df: table composed of a header and values
    :param wkb_name: spreadsheet label
    :param rng_code: table range label
    :param input_option: post all fields as str or in the type passsed by the user
    :type df: pd.DataFrame
    :type wkb_name: str
    :type rng_code: str
    :type input_option: str

    to specify datatype set input_option = 'USER_ENTERED'
    '''
    WKB_CONFIG = GSHEET_CONFIG[wkb_name]
    wkbid = WKB_CONFIG['wkbid']
    rng_config = WKB_CONFIG['sheets'][rng_code]
    if 'post' in rng_config:
        post_config = rng_config['post']
        rngid = post_config['data']
        fields = post_config['fields']
        df = df[fields]
    else:
        rngid = rng_config['data']

    # clear the range
    gs_engine.clear_rangevalues(wkbid, rngid)

    # post new values
    # DataFrame values must be converted to a 2D list [[]]
    if len(df) > 0:
        if input_option == 'RAW':  # write everything as a string
            values = df.values.astype('str').tolist()
        else:  # write as type passed by user
            values = df.values.tolist()
        gs_engine.set_rangevalues(wkbid, rngid, values, input_option)


# -----------------------------------------------------
# CSV file directory
# -----------------------------------------------------


class CSVDirectory(object):
    files = []
    csv_files = []
    xls_files = []

    def __init__(self, directory_path, filetype='csv'):
        self.path = directory_path
        self.filetype = filetype
        self.load_files()

    def load_files(self):
        self.files = fso.getFilesInFolder(self.path)
        if len(self.files) > 0:
            if self.filetype in ['csv', 'tsv']:
                self.csv_files = [f for f in self.files if '.'+self.filetype in f]
            elif self.filetype == 'xls':
                self.xls_files = [f for f in self.files if '.'+self.filetype in f]

    def get_tables(self):
        tbls = {}
        files = []
        if self.filetype in ['csv', 'tsv']:
            files = self.csv_files
        elif self.filetype == 'xls':
            files = self.xls_files
        if len(files) > 0:
            for f in files:
                try:
                    df = []
                    if self.filetype == 'csv':
                        df = pd.read_csv(self.path + '\\' + f)
                    elif self.filetype == 'xls':
                        df = pd.read_excel(self.path + '\\' + f)
                    elif self.filetype == 'tsv':
                        df = pd.read_csv(self.path + '\\' + f, sep='\t')
                    tbls[f] = df
                except:
                    pass
        return tbls

    def has_files(self):
        return len(self.files) > 0

    def has_csv(self):
        csv_check = False
        if self.has_files():
            if self.filetype in ['csv', 'tsv']:
                csv_check = len(self.csv_files) > 0
            elif self.filetype == 'xls':
                csv_check = len(self.xls_files) > 0
        return csv_check

    def flush(self, new_directory=None):
        if self.has_files():
            for f in self.files:
                src = self.path + '\\' + f
                if new_directory is None:
                    os.remove(src)
                else:
                    if new_directory == '':
                        dest = f
                    else:
                        dest = new_directory + '\\' + f
                    shutil.move(src, dest)
            self.__init__(self.path)

# -----------------------------------------------------
# CLI
# -----------------------------------------------------


if __name__ == "__main__":
    if len(sys.argv) > 1:
        function_name = sys.argv[1]
        if function_name == 'load_client_secret':
            file_path = gs.CLIENT_SECRET_FILE
            if len(sys.argv) > 2:
                file_path = sys.argv[2]
            with open(file_path, 'r') as f:
                client_secret = json.load(f)
            load_client_secret(client_secret)
            f.close()
