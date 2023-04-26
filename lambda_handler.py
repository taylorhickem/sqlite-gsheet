from sqlgsheet import database as db


USER_DATA_DIR = '/opt/user_data'
user_data = {
    'gsheet_config': USER_DATA_DIR + '/gsheet_config.json',
    'client_secret': USER_DATA_DIR + '/client_secret.json',
    'mysql_credentials': USER_DATA_DIR + '/mysql_credentials.json'
}


def lambda_handler(event, context):
    db.set_user_data(
        gsheet_config=user_data['gsheet_config'],
        client_secret=user_data['client_secret'],
        mysql_credentials=user_data['mysql_credentials']
    )
    db.DB_SOURCE = 'remote'
    db.load()
    events = db.get_table('event')

    return {
        'statusCode': 200,
        'blocky_first_record': events.iloc[0].to_json()
    }
