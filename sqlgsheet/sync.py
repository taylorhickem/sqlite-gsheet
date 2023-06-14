""" this module implements sync between two databases labeled as 'master' and 'slave'
with the following specifications

for two identical dataset schemas, one MASTER and one SLAVE,
whose unique columns are identified by some column_subset and contain a last_updated field:

the database sync will

1. compare the two datasets and
2. merges updates from the SLAVE to the MASTER
3. update the SLAVE to match the master

with the conditions that

1. rows can only be deleted from the MASTER
2. rows can be added or updated in either the MASTER or the SLAVE

"""
import os
import sys
import json
import pandas as pd
from sqlgsheet import database as db

DB_ROLES = ['master', 'slave']
NULL_CONNECT = {'engine': None, 'con': None}
DEFAULT_CONFIG_PATH = 'dbsync_config.json'
SYNC_STATUS_CODES = {
    0: 'disconnected',
    1: 'synced',
    2: 'connected',
    3: 'pending edits',
    4: 'error'
}
SYNC_SPEC = {
    'master': {},
    'slave': {},
    'tables': {}
}
EDITS_TEMPLATE = {
    'master': {
        'delete': [],
        'update': [],
        'insert': []
    },
    'slave': {
        'delete': [],
        'update': [],
        'insert': []
    }
}
MERGE_RULES = {
    'exists_master': [0, 0, 1, 1, 1, 1],
    'exists_slave': [1, 1, 0, 0, 1, 1],
    'most_recent': ['slave', 'master','slave', 'master', 'slave', 'master'],
    'destination': ['master', 'slave', 'slave', 'slave', 'master', 'slave'],
    'edit': ['insert', 'delete', 'insert', 'insert', 'update', 'update']
}


class DBSyncer(object):
    sync_config = {}
    master = NULL_CONNECT.copy()
    slave = NULL_CONNECT.copy()
    tables = {}
    _connected = {'master': False, 'slave': False}
    _status_code = 0
    errors = ''
    edits = {}

    def __repr__(self):
        status_dict = {'status': self.sync_status()}
        status_dict.update({r: ('connected' if self.connected(db_role=r) else 'disconnected') for r in DB_ROLES})
        if self._status_code == 4:
            status_dict.update({'errors': self.errors})
        return str(status_dict)

    def _exception_handle(self, e=None, error_message='', print_out=True, re_raise=False):
        self._status_code = 4
        details = ' details:' + str(e) if e else ''
        self.errors = error_message + details
        if print_out:
            print(self.errors)
        if re_raise and e:
            raise e

    def __init__(self, sync_config={}, config_path=DEFAULT_CONFIG_PATH):
        if sync_config:
            self.sync_config = sync_config.copy()
        elif config_path:
            self.sync_config = _sync_config_from_file(config_path)
        else:
            self.sync_config = SYNC_SPEC.copy()
        self._set_table_scope()

    def _set_table_scope(self):
        if 'tables' in self.sync_config:
            self.tables = self.sync_config['tables']

    def connected(self, db_role=''):
        if db_role:
            is_connected = self._connected[db_role]
        else:
            is_connected = all([self.connected(db_role=r) for r in DB_ROLES])
        return is_connected

    def disconnect(self, db_role=''):
        null_connect = NULL_CONNECT.copy()
        if db_role:
            if self.connected(db_role=db_role):
                self.__setattr__(db_role, null_connect)
                self._connected[db_role] = False
        else:
            for r in DB_ROLES:
                self.disconnect(db_role=r)

    def db_connect(self, db_role='', refresh=False):
        connect = {}
        if db_role:
            spec = self.sync_config[db_role].copy()
            db_type = spec.pop('db_type')
            if refresh or not self.connected(db_role):
                try:
                    connect = db.db_connection(db_type, **spec)
                except Exception as e:
                    self._exception_handle(e=e, error_message='db connect failed.')
                if connect:
                    self._connected[db_role] = True
                    self.__setattr__(db_role, connect)
        else:
            for r in DB_ROLES:
                self.db_connect(db_role=r)
            if self.connected():
                self._status_code = 2

    def con(self, db_role):
        con_obj = None
        if self.connected(db_role=db_role):
            con_obj = self.__getattribute__(db_role)['con']
        return con_obj

    def engine(self, db_role):
        eng = None
        if self.connected(db_role=db_role):
            eng = self.__getattribute__(db_role)['engine']
        return eng

    def get_table(self, db_role, table_name):
        tbl = []
        if self.connected(db_role=db_role):
            tbl = db.get_table(table_name, con=self.con(db_role=db_role))
        return tbl

    def sync_status(self):
        return SYNC_STATUS_CODES[self._status_code]

    def _table_field(self, table_name, field_ref):
        return self.tables[table_name][field_ref]

    def _key_field(self, table_name):
        return self._table_field(table_name, 'key')

    def _last_modified_field(self, table_name):
        return self._table_field(table_name, 'last_modified')

    def _rows_insert(self, db_role, table_name, rows):
        db.rows_insert(rows, table_name, con=self.con(db_role))

    def _rows_delete(self, db_role, table_name, rows):
        key = self._key_field(table_name)
        db.rows_delete(rows, table_name, key=key, eng=self.engine(db_role))

    def _rows_update(self, db_role, table_name, rows):
        key = self._key_field(table_name)
        db.rows_update(rows, table_name, key=key, eng=self.engine(db_role))

    def _merge_edits_update(self, table_name):
        key = self._key_field(table_name)
        last_modified = self._last_modified_field(table_name)
        master = self.get_table('master', table_name)
        slave = self.get_table('slave', table_name)
        try:
            table_edits = merge_edits(master, slave, key, last_modified)
        except Exception as e:
            error_message = f'DB FATAL SYNC ERROR for table:{table_name}. '
            error_message = error_message + 'Error comparing databases. Unable to determine sync edits to apply.'
            self._exception_handle(e=e, error_message=error_message)
        else:
            if table_edits:
                self.edits[table_name] = table_edits

    def _merge_edits_apply(self, table_name, db_role='', action='', rows=[]):
        edits = {}
        if self.edits:
            if table_name in self.edits:
                edits = self.edits[table_name].copy()
        if edits:
            if db_role:
                if action:
                    self.__getattribute__('_rows_' + action)(db_role, table_name, rows)
                elif db_role in edits:
                    db_edits = edits[db_role]
                    actions = list(EDITS_TEMPLATE[DB_ROLES[0]].keys())
                    for a in actions:
                        if len(db_edits[a]) > 0:
                            rows = db_edits[a]
                            try:
                                self._merge_edits_apply(table_name,
                                    db_role=db_role,
                                    action=a,
                                    rows=rows
                                )
                            except Exception as e:
                                error_message = f'DB SYNC FATAL ERROR for {table_name} {db_role} rows {a}. '
                                error_message = error_message + 'Manual repair may be required.'
                                self._exception_handle(e=e, error_message=error_message, re_raise=True)
            else:
                for r in DB_ROLES:
                    self._merge_edits_apply(table_name, db_role=r)

    def has_edits(self, db_role='', table_name=''):
        if table_name:
            edits_check = table_name in self.edits
            if edits_check and db_role:
                edits_check = db_role in self.edits[table_name]
                if edits_check:
                    actions = list(EDITS_TEMPLATE[DB_ROLES[0]].keys())
                    edits_check = any([len(self.edits[table_name][db_role][a]) > 0
                                       if a in self.edits[table_name][db_role]
                                       else False for a in actions])
            elif edits_check:
                edits_check = any([self.has_edits(db_role=r, table_name=table_name) for r in DB_ROLES])
        else:
            edits_check = any([self.has_edits(table_name=t) for t in self.tables])
        return edits_check

    def _table_sync(self, table_name, edits_apply=True):
        if (not self._status_code == 4) and (table_name in self.tables):
            self._merge_edits_update(table_name)
            if self.has_edits(table_name=table_name) and not self._status_code == 4:
                self._status_code = 3
            if edits_apply and self._status_code == 3:
                self._merge_edits_apply(table_name)

    def sync(self, edits_apply=True, keep_connection=False):
        self.db_connect()
        if self.connected():
            self._status_code = 1
            for t in self.tables:
                self._table_sync(t, edits_apply=edits_apply)
            if self._status_code not in [1, 4]:
                if edits_apply:
                    self._status_code = 1
                else:
                    self._status_code = 3
            if not keep_connection:
                self.disconnect()


def _sync_config_from_file(config_path: str) -> dict:
    spec = {}
    file_exists = os.path.isfile(config_path)
    if file_exists:
        try:
            with open(config_path, 'r') as f:
                spec = json.load(f)
                f.close()
        except:
            pass
    return spec


def config(config_path=DEFAULT_CONFIG_PATH):
    global SYNC_SPEC
    file_spec = _sync_config_from_file(config_path)
    if file_spec:
        SYNC_SPEC = file_spec


def update(config_path=DEFAULT_CONFIG_PATH):
    syncer = DBSyncer(config_path=config_path)
    syncer.db_connect()
    syncer.sync()


def merge_edits(master: pd.DataFrame, slave: pd.DataFrame,
                key='index', last_modified='last_modified') -> dict:
    edits = EDITS_TEMPLATE.copy()
    #01 check trivial conditions
    if len(master) > 0 and len(slave) == 0:
        edits['slave']['insert'] = master.copy()

    elif len(master) == 0 and len(slave) > 0:
        edits['master']['insert'] = slave.copy()

    elif len(master) > 0 and len(slave) > 0:
        #02 create diff table
        def reduced(tbl: pd.DataFrame) -> pd.DataFrame:
            selected = tbl[[key, last_modified]].reset_index()
            return selected

        red_master = reduced(master)
        red_slave = reduced(slave)
        diff = pd.merge(red_master, red_slave, how='outer', on=key,
                        suffixes=('_master', '_slave'))

        #03 map the two index fields to two "exists" fields
        for d in ['_master', '_slave']:
            diff['exists' + d] = diff['index' + d].notnull()
            diff['index' + d].fillna(0, inplace=True)
            diff[last_modified + d].fillna(0, inplace=True)

        #04 determine most_recent from last_modified
        global_lm = {
            'master': master[last_modified].max(),
            'slave': slave[last_modified].max()
        }

        def most_recent(ex_master, ex_slave, lm_master, lm_slave):
            recent = None
            if ex_master and ex_slave:
                if lm_master > lm_slave:
                    recent = 'master'
                elif lm_slave > lm_master:
                    recent = 'slave'

            elif ex_master:
                if lm_master >= global_lm['slave']:
                    recent = 'master'
                else:
                    recent = 'slave'

            else: #must be slave only
                if lm_slave >= global_lm['master']:
                    recent = 'slave'
                else:
                    recent = 'master'

            return recent

        diff['most_recent'] = diff.apply(lambda x: most_recent(
            x['exists_master'], x['exists_slave'],
            x[last_modified + '_master'], x[last_modified + '_slave']), axis=1)
        del diff[last_modified + '_master']
        del diff[last_modified + '_slave']
        diff.dropna(subset=['most_recent'], inplace=True)

        #05 use merge rules to assign edit action
        merge_rules = pd.DataFrame(MERGE_RULES)
        join_fields = ['exists_master', 'exists_slave', 'most_recent']
        diff = pd.merge(diff, merge_rules, how='left', on=join_fields)

        #06 group by destination and edits
        actions = list(edits['master'].keys())
        edit_groups = diff.groupby(['destination', 'edit']).groups
        for d in ['master', 'slave']:
            for e in actions:
                if (d, e) in edit_groups:
                    diff_rows = diff.loc[edit_groups[(d, e)]].copy()
                    if d == 'master':
                        edits[d][e] = slave.loc[diff_rows['index_slave']].copy()
                    else:
                        edits[d][e] = master.loc[diff_rows['index_master']].copy()

    return edits

#***** Command line interface *******************************

if __name__ == '__main__':
    if len(sys.argv) > 1:
        function_name = sys.argv[1]
        if function_name == 'config':
            if len(sys.argv) > 2:
                config(sys.argv[2])
            else:
                config()

        elif function_name == 'update':
            if len(sys.argv) > 2:
                update(sys.argv[2])
            else:
                update()

        else:
            print('no function specified. available functions: [config, update]')