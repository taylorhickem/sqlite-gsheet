"""demo of how to use the sqlite-gsheet module for a simple UI for a python app
   sqlite features to be elaborated in future version of the documentation
"""
import json
import datetime as dt
import pandas as pd

from sqlgsheet import database as db
from sqlgsheet import sync
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


def d04_db_sync():
    #sync.config('dbsync_config.json')
    #sync.update()

    print('starting test 04: db sync ...')
    tests = []
    print('04.01 connecting ...')
    try:
        syncer = sync.DBSyncer()
        syncer.db_connect()
    except Exception as e:
        print(f'04.01 connect failed. ERROR: {str(e)}')
        test_result = False
    else:
        test_result = syncer.connected('master') and syncer.connected('slave')
    tests.append(test_result)
    print(f'04.01 connect success?{test_result}')

    if test_result:
        print('04.02 comparing tables to check for differences ...')
        try:
            syncer.sync(edits_apply=False, keep_connection=True)
        except Exception as e:
            print(f'04.02 sync failed. ERROR: {str(e)}')
            test_result = False
        else:
            sync_status = syncer.sync_status()
            test_result = sync_status == 'pending edits'
            print(f'sync status: {sync_status}')
        tests.append(test_result)
        print(f'04.02 differences found?{test_result}')

    if test_result:
        print('04.03 comparing tables and applying edits ...')
        try:
            syncer.sync(edits_apply=True, keep_connection=True)
        except Exception as e:
            print(f'04.03 sync failed. ERROR: {str(e)}')
            test_result = False
        else:
            sync_status = syncer.sync_status()
            test_result = sync_status == 'synced'
            print(f'sync status: {sync_status}')
        tests.append(test_result)

    if test_result:
        print('04.04 comparing tables and validating no differences after applying edits ...')
        try:
            syncer.sync(edits_apply=False, keep_connection=False)
        except Exception as e:
            print(f'04.04 sync failed. ERROR: {str(e)}')
            test_result = False
        else:
            sync_status = syncer.sync_status()
            test_result = sync_status == 'synced'
            print(f'sync status: {sync_status}')
        tests.append(test_result)
        print(f'04.04 sync success?{test_result}')

    print(f'db sync test complete. results:{tests}')



def d05_db_edits():
    print('starting test 05: db edits ...')
    test_results = []
    table_name = 'logs'
    table_key = 'timestamp'
    db.DB_SOURCE = 'local'
    if db.DB_SOURCE == 'local':
        db.SQL_DB_NAME = 'sqlite:///hours.db'
    db.load_sql()
    print(f'connected to db. source:{db.DB_SOURCE}')
    t0 = db.get_table(table_name)
    cycle_rows = t0[t0['date'] == '2022-11-22'].copy()

    print('test 1.1 rows delete ...')
    db.rows_delete(cycle_rows, table_name, key=table_key, eng=db.engine)
    print('test 1.1 table query ...')
    t1 = db.get_table(table_name)
    test_success = len(t1[t1['date'] == '2022-11-22']) == 0
    print(f'test 1.1 success?{test_success}')
    test_results.append(test_success)

    if test_success:
        print('test 1.2 rows insert ...')
        bunny_rows = cycle_rows.copy()
        bunny_rows['comment'] = 'bunny'
        db.rows_insert(bunny_rows, table_name, eng=db.engine)
        print('test 1.2 table query ...')
        t2 = db.get_table(table_name)
        test_success = len(t2[t2['date'] == '2022-11-22']) > 0
        print(f'test 1.2 success?{test_success}')
        test_results.append(test_success)

    if test_success:
        print('test 1.3 rows update ...')
        db.rows_update(cycle_rows, table_name, key='timestamp', eng=db.engine)
        print('test 1.3 table query ...')
        t3 = db.get_table(table_name)
        test_success = t3[t3['date'] == '2022-11-22']['comment'].iloc[0] != 'bunny'
        print(f'test 1.3 success?{test_success}')
        test_results.append(test_success)

    print(f'db edits test complete. results: {test_results}')


def d06_syncer_connect():
    tests = []
    print('starting test 06: db syncer connect ...')
    print('06.01 connecting ...')
    try:
        syncer = sync.DBSyncer()
        syncer.db_connect()
    except Exception as e:
        print(f'06.01 connect failed. ERROR: {str(e)}')
        test_result = False
    else:
        test_result = syncer.connected('master') and syncer.connected('slave')
    tests.append(test_result)
    print(f'06.01 connect success?{test_result}')

    if test_result:
        print('06.02 query logs tables from master and slave ...')
        try:
            master_logs = syncer.get_table('master', 'logs')
            slave_logs = syncer.get_table('slave', 'logs')
        except Exception as e:
            print(f'06.02 query failed. ERROR: {str(e)}')
            test_result = False
        else:
            test_result = (len(master_logs) > 0) and (len(slave_logs) > 0)
        tests.append(test_result)
        print(f'06.02 query success?{test_result}')

    if test_result:
        print('06.03 disconnecting ...')
        syncer.disconnect()
        test_result = not (syncer.connected('master') or syncer.connected('slave'))
        tests.append(test_result)
        print(f'06.03 disconnect success?{test_result}')

    print(f'db syncer connect test complete. results:{tests}')

def d07_syncer_merge_edits():
    tests = []
    actions = ['delete', 'update', 'insert']
    print('starting test 07: db syncer merge ...')
    print('07.01 connecting ...')
    try:
        syncer = sync.DBSyncer()
        syncer.db_connect()
    except Exception as e:
        print(f'07.01 connect failed. ERROR: {str(e)}')
        test_result = False
    else:
        test_result = syncer.connected('master') and syncer.connected('slave')
    tests.append(test_result)
    print(f'07.01 connect success?{test_result}')

    if test_result:
        edits_file = 'merge_edits.json'
        print('07.02 comparing tables to get merge edits ...')
        try:
            syncer.sync(edits_apply=False, keep_connection=False)
            edits = syncer.edits.copy()
        except Exception as e:
            print(f'07.02 merge failed. ERROR: {str(e)}')
            test_result = False
        else:
            test_result = 'logs' in edits
            print(f'07.02 merge: logs in edits?{test_result}')
            if test_result:
                test_result = any([a in edits['logs']['slave'] for a in actions])
                if test_result:
                    test_result = max([len(edits['logs']['slave'][x]) if x in edits['logs']['slave'] else 0 for x in actions]) > 0
                    print(f'07.02 merge: has insert edits for slave db?{test_result}')
                    if test_result:
                        edits_json = {'logs':{'master':{}, 'slave':{}}}
                        for r in ['master', 'slave']:
                            for a in actions:
                                if a in edits['logs'][r]:
                                    if len(edits['logs'][r][a]) > 0:
                                        edits_json['logs'][r][a] = edits['logs'][r][a].to_json(orient='records')
                        with open(edits_file, 'w') as f:
                            json.dump(edits_json, f, indent=4)
                            f.close()
                        print(f'07.02 merge edits saved to {edits_file}')

        tests.append(test_result)
        print(f'07.02 merge success?{test_result}')

    print(f'db merge test complete. results:{tests}')

