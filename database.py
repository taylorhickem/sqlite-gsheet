''' interface with data sources
'''
# -----------------------------------------------------
# Import
# -----------------------------------------------------
import os
import shutil
import utilities.fso as fso
import json
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector
import gsheet as gs
#import sqlite-gsheet.gsheet as gs

##-----------------------------------------------------
# Module variables
##-----------------------------------------------------
# constants
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
CONFIG = {}
GSHEET_CONFIG = {}


# custom class objects from other modules
engine = None
gs_engine = None


# -----------------------------------------------------
# Setup
# -----------------------------------------------------
def load():
    load_config()
    load_sql()
    load_gsheet()


def load_config():
    global CONFIG, GSHEET_CONFIG
    CONFIG = json.load(open('config.json'))
    GSHEET_CONFIG = json.load(open(CONFIG['gsheet_config_file']))


def load_gsheet():
    global gs_engine
    if gs_engine is None:
        gs_engine = gs.SheetsEngine()


# -----------------------------------------------------
# Sqlite
# -----------------------------------------------------

def load_sql():
    global engine, inspector, table_names
    if engine is None:
        engine = create_engine(SQL_DB_NAME, echo=False)
        inspector = Inspector.from_engine(engine)
        table_names = inspector.get_table_names()


def table_exists(tableName):
    return tableName in table_names


def get_table(table_name):
    if table_exists(table_name):
        tbl = pd.read_sql_table(table_name, con=engine)
    else:
        tbl = None
    return tbl


def update_table(tbl, tblname, append=True):
    global engine
    if append:
        ifex = 'append'
    else:
        ifex = 'replace'
    tbl.to_sql(tblname, con=engine, if_exists=ifex, index=False)


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
# ***
# -----------------------------------------------------