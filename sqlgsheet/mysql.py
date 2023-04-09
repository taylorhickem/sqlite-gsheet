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
        login_request_url = MYSQL_CREDENTIALS['login'].format(
                user=MYSQL_CREDENTIALS['username'],
                pw=urllib.parse.quote_plus(MYSQL_CREDENTIALS['password']),
                db=MYSQL_CREDENTIALS['database'])
        engine = create_engine(login_request_url)
        con = engine.connect()