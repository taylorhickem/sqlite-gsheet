# sqlite-gsheet
add-on utility for simple python apps to use sqlite as storage and google sheets as user interface

## setup

1. configure service account for google sheets api
    search online sources for steps to setup a service account 
       example: robocorp article: [how to read from and write into Google Sheets for your robots](https://robocorp.com/docs/development-guide/google-sheets/interacting-with-google-sheets)
    
    take note of your service account _client_secret.json_ and _client_email_
 
2. install the module into your python project from the github repository url using pip

     `pip install git+https://github.com/taylorhickem/https://github.com/taylorhickem/sqlite-gsheet.git`

3. add the _client_secret.json_ file into your project root directory

    \myapp 
    * LICENSE   
    * .gitignore
    * client_secret.json
    * myapp.py

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

`import sqlite-gsheet.database as db`

`db.load()`

`form = db.get_sheet('myapp', 'form')`

post to gsheet from pandas DataFrame _form_

`import sqlite-gsheet.database as db`

`db.load()`

`db.post_to_gsheet(form, 'myapp', 'form')`


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