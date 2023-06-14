""" this script tests connection to the remote mysql server
"""
# -----------------------------------------------------
# Import
# -----------------------------------------------------
import json
import pymysql
import urllib.parse
from sqlalchemy import create_engine

##-----------------------------------------------------
# Module variables
##-----------------------------------------------------

PATH_MYSQL_CRED = 'mysql_credentials.json'
MYSQL_CONFIG = {}
MYSQL_CREDENTIALS = {}
engine = None
con = None

# -----------------------------------------------------
# Setup
# -----------------------------------------------------
def load():
    load_config()
    load_sql()


def load_config():
    global MYSQL_CONFIG, MYSQL_CREDENTIALS
    MYSQL_CREDENTIALS = json.load(open(PATH_MYSQL_CRED))


def load_sql():
    global engine, con
    if engine is None:
        params = {k: MYSQL_CREDENTIALS[k] for k in [
            'database',
            'login',
            'username',
            'password'
        ]}
        connect = get_connection(**params)
        engine = connect['engine']
        con = connect['con']


def get_connection(database='',
                   login='', username='', password='') -> dict:
    connect = {}
    login_request_url = login.format(
        user=username,
        pw=urllib.parse.quote_plus(password),
        db=database)
    try:
        connect['engine'] = create_engine(login_request_url)
        connect['con'] = connect['engine'].connect()
    except Exception as e:
        print(f'ERROR: unable to connect to database {e}')
    return connect
