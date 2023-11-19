import json
from sqlgsheet.templates import DBConnection
from sqlgsheet.sync import DBSyncer


SYNC_CONFIG_FILE = 'dbsync_config.json'
DB_CONFIG_FILE = 'dynamodb_config.json'
syncer = None


def lambda_handler(event, context):
    global syncer
    sync_config = get_sync_config()
    db_config = get_db_config()
    dynamodb = DBConnection(config=db_config)
    syncer = DBSyncer(sync_config=sync_config)
    syncer.db_connect(db_role='slave', con_obj=dynamodb)
    syncer.db_connect(db_role='master')
    syncer.db_connect()
    syncer.disconnect()


def get_db_config():
    db_config = {}
    with open(DB_CONFIG_FILE, 'r') as f:
        db_config = json.load(f)
        f.close()
    return db_config


def get_sync_config():
    sync_config = {}
    with open(SYNC_CONFIG_FILE, 'r') as f:
        sync_config = json.load(f)
        f.close()
    return sync_config


if __name__ == "__main__":
    lambda_handler(None, None)