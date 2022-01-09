"""demo of how to use the sqlite-gsheet module for a simple UI for a python app
   sqlite features to be elaborated in future version of the documentation
"""
import datetime as dt
import pandas as pd

import database as db
#from sqlite-gsheet import database as db
import myapp


def d01_update():
    """run the main myapp procedure from interpreter which
       loads the new form responses from the gsheet and adds them as new records
    """
    myapp.update()
    print('new form responses updated for myapp gsheet')


def d02_load_gsheet_to_dataframe():
    db.load()
    form = db.get_sheet('myapp', 'form')
    print('new form responses from gsheet UI: \n %s' % form.head())


def d03_post_updates_to_gsheet():
    db.load()
    form_responses = db.get_sheet('myapp', 'form')
    records = db.get_sheet('myapp', 'records')
    records = records.append(form_responses)
    db.post_to_gsheet(records,
                          'myapp',
                          'records',
                          input_option='USER_ENTERED')
    print('myapp gsheet records updated with new UI form responses: \n %s' % records.head())
