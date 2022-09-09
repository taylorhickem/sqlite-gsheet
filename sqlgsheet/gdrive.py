""" this module uses the Google Drive API to
 perform file management operations on a Google Drive account
 for details refer to Developer documentation
 https://developers.google.com/drive/api/guides/about-files
"""
#-----------------------------------------------------------------------------
# import dependencies
#-----------------------------------------------------------------------------
import os
import io
import os.path
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
#from apiclient import discovery
#from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
#import httplib2
import pandas as pd
import numpy as np

#-----------------------------------------------------------------------------
# module variables
#-----------------------------------------------------------------------------
SCOPES = ['https://www.googleapis.com/auth/drive']
CLIENT_SECRET_FILE ='client_secret.json'   #must be located in the same directory
ordRef = {'A': 65}
gdrive_engine = None
service = None

#-----------------------------------------------------------------------------
# file folder operations
#-----------------------------------------------------------------------------


def download_file(file_id):
    #returns the file as a bytes type
    payload = False
    request = service.files().get_media(fileId=file_id)
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        #print(F'Download {int(status.progress() * 100)}.')

    payload = file.getvalue()
    return payload


def download_files_in_folder(folder_name, folder_id, mime_type=None):
    payloads = []
    names = []
    file_references = get_files_in_folder(
        folder_name, folder_id,
        include_subfolders=False,
        mime_type=mime_type)
    if len(file_references) > 0:
        for f in file_references:
            payload = download_file(f['id'])
            payloads.append(payload)
            names.append(f['name'])

    files = dict(zip(names, payloads))
    return files


def get_folder_id(folder_name):
    folder_id = ''
    qry = "name='"+folder_name+"' and "
    qry = qry + "mimeType='application/vnd.google-apps.folder'"
    response = service.files().list(
        q=qry,
        spaces='drive'
    ).execute()
    found_folder = len(response['files']) > 0
    if found_folder:
        folder_id = response['files'][0]['id']
    return folder_id


def get_files_in_folder(folder_name, folder_id='',
                        include_subfolders=False,
                        mime_type=None):
    files = []
    if folder_id == '':
        folder_id = get_folder_id(folder_name)
    if len(folder_id) > 0:
        qry = "'"+folder_id+"' in parents"
        if not include_subfolders:
            qry = qry + " and mimeType!='application/vnd.google-apps.folder'"
        if not mime_type is None:
            qry = qry + " and mimeType='"+mime_type+"'"
        response = service.files().list(
            q=qry,
            spaces='drive'
        ).execute()
        files = response['files']
    return files


def move_file_to_folder(file_id,
                        destination_id, source_id=''):
    if source_id != '':
        response = service.files().update(
        fileId=file_id,
        addParents=destination_id,
        removeParents=source_id,
        fields='id, parents'
        ).execute()
    else:
        response = service.files().update(
        fileId=file_id,
        addParents=destination_id,
        fields='id, parents'
        ).execute()


def move_files_to_folder(file_ids, destination_id, source_id=''):
    for f in file_ids:
        move_file_to_folder(f, destination_id, source_id)


def get_file_parent_folder_ids(file_id):
    parent_ids = []
    response = service.files().get(
        fileId=file_id,
        fields='parents'
    ).execute()
    if 'parents' in response:
        parent_ids = response['parents']
    return parent_ids


#-----------------------------------------------------------------------------
# setup
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# authentication
#-----------------------------------------------------------------------------
def get_credentials():
    #current_path = os.path.abspath('')
    #file_path = current_path[:current_path.find('PyTools')] + 'PyTools\\GsheetsAPI\\'
    file_path = os.path.dirname(__file__) + '\\'
    #credentials = service_account.Credentials.from_service_account_file(file_path + CLIENT_SECRET_FILE,scopes=SCOPES)
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        file_path + CLIENT_SECRET_FILE, SCOPES)
    return credentials


def login():
    global service
    credentials = get_credentials()
    # self.service = discovery.build('sheets','v4',credentials=credentials)
    http = credentials.authorize(Http())
    service = build('drive', 'v3', http=http)


#-----------------------------------------------------------------------------
# class: GDriveEngine
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
# helper functions
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
# example script : login and get folder id from folder name
#-----------------------------------------------------------------------------
#from sqlgsheet import gdrive
#folder_name = '01 NowThen data'
#gdrive.login()
#folder_id = gdrive.get_folder_id(folder_name)

#-----------------------------------------------------------------------------
# END
#-----------------------------------------------------------------------------