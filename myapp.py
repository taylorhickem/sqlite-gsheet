# interface for gsheet ui for demo app 'myapp'
# -----------------------------------------------------
# Import
# -----------------------------------------------------
import datetime as dt
import pandas as pd

import database as db
##from sqlite-gsheet import database as db

# -----------------------------------------------------
# Module variables
# -----------------------------------------------------

# constants
UI_SHEET = 'myapp'

# dynamic
UI_CONFIG = {}
TABLES = {}

# -----------------------------------------------------
# Setup
# -----------------------------------------------------


def load():
    ''' loads basic configuration information for the module
    '''
    # loads the module interface to gsheet
    db.load()

    # load configuration information
    load_config()

    #load tables from gsheet
    load_gsheets()


def load_config():
    global UI_CONFIG
    config_tbl = db.get_sheet(UI_SHEET, 'config')
    UI_CONFIG = get_reporting_config(config_tbl)


def load_gsheets():
    global TABLES
    tables_config = db.GSHEET_CONFIG[UI_SHEET]['sheets'].copy()
    for t in tables_config:
        TABLES[t] = {}
        TABLES[t]['gsheet'] = db.get_sheet(UI_SHEET, t)


def get_reporting_config(tbl):
    DATE_FORMAT = '%Y-%m-%d'
    config = {}
    groups = list(tbl['group'].unique())
    for grp in groups:
        group_tbl = tbl[tbl['group'] == grp][['parameter', 'value', 'data_type']]
        params = dict(group_tbl[['parameter', 'value']]
                      .set_index('parameter')['value'])
        data_types = dict(group_tbl[['parameter', 'data_type']]
                          .set_index('parameter')['data_type'])
        for p in params:
            data_type = data_types[p]
            if ~(data_type == 'str'):
                if ~pd.isnull(params[p]):
                    if data_type in db.NUMERIC_TYPES:
                        numStr = params[p]
                        if numStr == '':
                            numStr = '0'
                        if data_type == 'int':
                            params[p] = int(numStr)
                        elif data_type == 'float':
                            params[p] = float(numStr)
                    elif data_type == 'date':
                        params[p] = dt.datetime.strptime(params[p], DATE_FORMAT).date()
        config[grp] = params
    return config


# -----------------------------------------------------
# Main
# -----------------------------------------------------


def update():
    # 01 load tables
    load()

    # 02 get new form responses
    form_responses = TABLES['form']['gsheet'].copy()
    records = TABLES['records']['gsheet'].copy()

    # 03 append new form responses to records and post to gsheet
    records = records.append(form_responses)
    db.post_to_gsheet(records,
                      UI_SHEET,
                      'records',
                      input_option='USER_ENTERED')


if __name__ == "__main__":
    update()
# -----------------------------------------------------
# Reference code
# -----------------------------------------------------