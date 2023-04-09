# sqlite-gsheet
add-on utility for simple python apps to use sqlite as storage and google sheets as user interface

## setup

see below for user data configuration from non-default directory

1. configure service account for google sheets api
    search online sources for steps to setup a service account 
       example: robocorp article: [how to read from and write into Google Sheets for your robots](https://robocorp.com/docs/development-guide/google-sheets/interacting-with-google-sheets)
    
    take note of your service account _client_secret.json_ and _client_email_
 
2. install the module into your python project from the github repository url using pip

     `pip install git+https://github.com/taylorhickem/sqlite-gsheet.git`

3. upload the _client_secret.json_ to your sqlgsheet package instance.
    you only need to do this once.

```   
(venv) >python -m sqlgsheet.database load_client_secret
```

or to specify a path other than the default 'client_secret.json' from the working diectory :

```   
(venv) >python -m sqlgsheet.database load_client_secret <path to client secret file>
```
                 
4. create your google spreadsheet

    demo sheet [myapp](https://docs.google.com/spreadsheets/d/1T8JCGdsTAjr8820l-iSHKnPlZFM2C7MKDLQLcoQA-sk/) 
    
    take note of 
    * the _workbookId_, read from the url ../spreadsheets/d/_workbookId_
    * the ranges of interest and give them names

5. grant write access to your service account

   select _share_ and add your _client_email_ with write priviledges for your sheet

6. configure your _gsheet_config.json_ file to match your spreadsheet

## sample code

For details see the demo project \myapp
and sample _myapp.py_

run from terminal

`\myapp>python myapp.py`

run from python interpreter

`import myapp`

`myapp.update()`

load from gsheet into pandas DataFrame _form_

`from sqlgsheet import database as db`

`db.load()`

`form = db.get_sheet('myapp', 'form')`

post to gsheet from pandas DataFrame _form_responses_

`from sqlgsheet import database as db`

`db.load()`

`form_responses = db.get_sheet('myapp', 'form')`

`records = db.get_sheet('myapp', 'records')`

`records = records.append(form_responses)`

`db.post_to_gsheet(records,
                      'myapp',
                      'records',
                      input_option='USER_ENTERED')
`

_##sqlite features to be elaborated in future version of the documentation##_

## sample files

sample _client_secret.json_
    
`    {
  "type": "service_account",
  "project_id": "helvasheets",
  "private_key_id": "###",
  "private_key": "###",
  "client_email": "gsheets@helvasheets.iam.gserviceaccount.com",
  "client_id": int,
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gsheets%40helvasheets.iam.gserviceaccount.com"
}
`

sample _gsheet_config_

`{
  "myapp": {
    "wkbid": "1T8JCGdsTAjr8820l-iSHKnPlZFM2C7MKDLQLcoQA-sk",
    "sheets": {
      "config": {
        "data": "config!A2:D",
        "header": "config!A1:D1"
      },
      "records": {
        "data": "records!A2:C",
        "header": "records!A1:C1",
        "data_types": {
          "date": "date",
          "parameter": "str",
          "value": "float"
        }
      },
      "form": {
        "data": "form!A3:C",
        "header": "form!A2:C2",
        "data_types": {
          "date": "date",
          "parameter": "str",
          "value": "float"
        }
      }
    }
  }
}
`

## User data from non-default directory

For some use cases, such as with use in AWS lambda, 
you need to set user data from a custom directory location 
other than the runtime directory.

before calling any methods from database.py, first run .set_user_data()
and specify the full path to the user data files identified with keywords.

```
db.set_user_data(
    gsheet_config='/path/gsheet_config.json'
    gas_client_secret='/path/.../client_secret.json'
    mysql_credentials='/path/.../mysql_credentials.json'
)
```
