'''Created on 2018/07/22
@author: Taylor W Hickem
'''
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import os
#import httplib2
#from apiclient import discovery
#from google.oauth2 import service_account
import pandas as pd
import numpy as np

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CLIENT_SECRET_FILE ='client_secret.json'   #must be located in the same directory
ordRef = {'A':65}

def get_credentials():
    #current_path = os.path.abspath('')
    #file_path = current_path[:current_path.find('PyTools')] + 'PyTools\\GsheetsAPI\\'
    file_path = os.path.dirname(__file__) + '\\'
    #credentials = service_account.Credentials.from_service_account_file(file_path + CLIENT_SECRET_FILE,scopes=SCOPES)
    credentials = ServiceAccountCredentials.from_json_keyfile_name(file_path + CLIENT_SECRET_FILE, SCOPES)
    return credentials

shtEng = None

class SheetsEngine():
    service = None
    sheetIds = {}
    def __init__(self):
        self.login()

    def login(self):
        credentials = get_credentials()
        #self.service = discovery.build('sheets','v4',credentials=credentials)
        http = credentials.authorize(Http())
        self.service = build('sheets', 'v4', http=http)

    def add_sheet(self,key,spreadsheetId):
        self.sheetIds[key] = spreadsheetId

    def add_sheets(self,keyIdPairs):
        for key in keyIdPairs:
            self.add_sheet(key,keyIdPairs[key])

    def get_rangevalues(self,spreadsheetId,rangeName):
        #when calling the query -- it is acceptable to specify range with ambiguous end row --
        #    ex : sheet!A2:V 
        #the service automatically finds the end row just as the google query method
        result = self.service.spreadsheets().values().get(
            spreadsheetId=spreadsheetId, range=rangeName).execute()
        return result.get('values', [])

    def set_rangevalues(self,spreadsheetId,rangeName,values,input_option='RAW'):
        body = {'range':rangeName,'values':values}
        self.service.spreadsheets().values().update(
            spreadsheetId=spreadsheetId, valueInputOption= input_option, range=rangeName,body=body).execute()

    def clear_rangevalues(self,spreadsheetId,rangeName):
        self.service.spreadsheets().values().clear(
            spreadsheetId=spreadsheetId, range=rangeName).execute()

    def get_tabledata(self,wkbkey,sheetStr,Col=None,fixedRef=None):
        df = None
        if wkbkey in self.sheetIds and not (Col is None and fixedRef is None):
            if not Col is None:
                rangeHeader = sheetStr + '!' + Col[0] + '1:' + Col[1] + '1'
                rangeData = sheetStr + '!' + Col[0] + '2:' + Col[1]
            elif not fixedRef is None:
                rangeHeader = sheetStr + '!' + fixedRef[0]
                rangeData = sheetStr + '!' + fixedRef[1]
            fields = self.get_rangevalues(self.sheetIds[wkbkey],rangeHeader)[0]
            data = self.get_rangevalues(self.sheetIds[wkbkey],rangeData)
            df = pd.DataFrame(data,columns=fields)
        return df

def load():
    global shtEng
    if shtEng is None:
        shtEng = SheetsEngine()

def autorun():
    load()

class ParameterTable():
    tblData = None
    parameterLabels = []
    parameterKeys = {}
    fields = []
    sheetId = ''
    pageId = ''
    valueCol = None    
    countCol = None
    valField = ''
    parField = ''
    col = []
    def __init__(self,sheetId,pageId,valueCol=2,countCol=3,wkbKey='wkb'):
        global shtEng
        load()
        self.sheetId = sheetId
        self.pageId = pageId
        self.wkbKey = wkbKey
        self.valueCol = valueCol
        self.countCol = countCol
        self.col = ['B',chr(ordRef['A']+countCol-1)]
        shtEng.add_sheet(self.wkbKey,self.sheetId)
        self.refresh_parameterKeys()
    def __repr__(self):
            return self.tblData.__repr__()
    def head(self,*arg,**kwarg):
            return self.tblData.head(*arg,**kwarg)
    def __len__(self):
            return len(self.tblData)
    def refresh_tblData(self):
        self.tblData = shtEng.get_tabledata(self.wkbKey,self.pageId,self.col)
        self.parField = self.tblData.columns[0]
        self.valField = self.tblData.columns[self.valueCol-1]
    def commit(self):
        values = self.gsheet_compatible()
        rangeName = self.range_address()
        shtEng.set_rangevalues(self.sheetId,rangeName,values)
    def refresh_parameterKeys(self):
        self.refresh_tblData()
        self.fields = self.tblData.columns
        self.parameterLabels = list(self.tblData[self.parField])
        self.parameterKeys = dict(zip(self.parameterLabels,list(self.tblData.index)))
    def get_parameterId(self,key):
        return self.parameterKeys[key]
    def get_parameterIds(self,keys):
        return [self.parameterKeys[key] for key in keys]
    def range_address(self,key=None,p_id=None,fullrow=True):
        if (key is None and p_id is None and fullrow):
            localAddress = self.col[0] + str(2) + ':' + self.col[-1]
        else:
            if p_id is None:
                p_id = self.get_parameterId(key)
            if fullrow:
                localAddress = self.col[0] + str(p_id+2) + ':' + self.col[-1] + str(p_id+2)
            else:
                localAddress = chr(ordRef['A']+self.valueCol-1) + str(p_id+2)
        return self.pageId + '!' + localAddress
    def getInfo(self,key=None,p_id=None):
        if p_id is None:
            p_id = self.get_parameterId(key)
        return dict(self.tblData.iloc[p_id])
    def getInfos(self,keys=None,p_ids=None):
        if p_ids is None:
            p_ids = [self.parameterKeys[key] for key in keys]
        df = self.tblData.iloc[p_ids].copy()
        return df
    def p_ids_from_infoData(self,infoData):
        keys = list(infoData[self.parField])
        p_ids = self.get_parameterIds(keys)
        return p_ids
    def setInfo(self,infoRow,key=None,p_id=None):
        if key is None:
            key = infoRow[self.parField]
        if p_id is None:
            p_id = self.get_parameterId(key)
        type_infoRow = type(infoRow)
        if type_infoRow in [pd.DataFrame,pd.Series]:
            if type_infoRow == pd.DataFrame:
                infoRow['p_id'] = p_id
                infoRow.set_index('p_id',inplace=True)
                infoRow.index.name = None
            elif type_infoRow == pd.Series:
                infoRow = pd.DataFrame.from_records([infoRow],index=[p_id])
        else:
            raise ValueError('infoRow datatype :' +type_infoRow + '. Supported types are DataFrame and Series')
        self.tblData.update(infoRow)
    def setInfos(self,infoData,keys=None,p_ids=None):
        #reset index to match the local tblData index p_ids
        if p_ids is None:
            if keys is None:
                p_ids = self.p_ids_from_infoData(infoData)
            else:
                p_ids = self.get_parameterIds(keys)
        infoData = pd.DataFrame(infoData,index=p_ids)
        #infoData['p_id'] = p_ids
        #df = infoData.set_index('p_id')
        #df.index.name = None
        self.tblData.update(infoData)
    def getValue(self,key=None,p_id=None,dataType=float):
        if p_id is None:
            p_id = self.get_parameterId(key)
        value = self.tblData.iloc[p_id][self.valField]
        if dataType in [int,float]:
            if dataType == int:
                returnValue = int(value)
            elif dataType == float:
                returnValue = float(value)
        else:
            returnValue = value
        return returnValue
    def getValues(self,keys=None,p_ids=None,dataType=float,as_dict=False):
        if p_ids is None:
            p_ids = self.get_parameterIds(keys)
        df = self.tblData[[self.parField,self.valField]].iloc[p_ids].copy()
        df.columns = ['parameter','value']
        if dataType in [int,float]:
            df['value'] = pd.to_numeric(df['value'])
            #if dataType == int:
            #    df['value'] = df.apply(lambda x:int(x.value),axis=1)
            #elif dataType == float:
            #    df['value'] = df.apply(lambda x:float(x.value),axis=1)
        if as_dict:
            values = dict(df.to_records(index=False))
        else:
            values = df
        return values
    def setValue(self,value,key=None,p_id=None):
        if p_id is None:
            p_id = self.get_parameterId(key)
        self.tblData[self.valField][p_id] = value
    def setValues(self,values,keys=None,p_ids=None):
        if p_ids is None:
            p_ids = self.get_parameterIds(keys)
        type_values = type(values)
        if type_values in [list,pd.DataFrame]:
            if type_values == list:
                df = pd.DataFrame({self.valField:values},index=p_ids)
            else:
                if len(values.columns)==1:
                    values.columns = [self.valField]
                    df = pd.DataFrame(values,index=p_ids)
                else:
                    raise ValueError('# of columns passed =' + len(values.columns) + '.  Only allowed 1 column')
        else:
            raise ValueError('value datatype:'+ type_values + '. Supported types are DataFrame and list')
        self.tblData.update(df)
    def gsheet_compatible(self,data=None):
        if data is None:
            data = self.tblData
        rows = [list(x) for x in data.to_records(index=False)]
        for i in range(len(rows)):
            for j in range(len(rows[0])):
                if type(rows[i][j])== np.int64:
                    rows[i][j] = int(rows[i][j])
        return rows

class ReportTable():
    #Report table is closely related to a pivot table structure
    #with a two-level row index and a two-level column index
    #the motivating application is for financial reports

    #the ReportTable class interfaces with an actual report on a google sheet
    #and is able to read/write and preserve the essential features
    #the report table maintains two representations of the dataset
    #1 - a traditional recordset, and 2 - a pivot table of the dataset
    rcds = None
    pivot = None
    categories = {}
    periods = []
    countCol = 0
    col = []
    wkbId = ''
    pageId = ''
    def __init__(self,wkbId,pageId,countCol=14):
        global shtEng
        load()
        self.wkbId = wkbId
        self.pageId = pageId
        self.countCol = countCol
        self.col = ['B',chr(ordRef['A']+countCol-1)]
        shtEng.add_sheet('wkb',self.wkbId)
        self.refresh_table()
    #def __repr__(self):
    #        return self.pivot.__repr__()
    def refresh_table(self):
        df = None
        print('im working on it')
        #2 - get Nx2 2-level categories  range
        #1 - get 2xM 2-level periods  range
        #3 - get NxM table data
        #4 construct a pandas multi-index dataframe -- pivot
        #4 de-pivot and construct a recordset -- rcds

        #prior notes
        #if wkbkey in self.sheetIds and not (Col is None and fixedRef is None):
        #    if not Col is None:
        #        rangeHeader = sheetStr + '!' + Col[0] + '1:' + Col[1] + '1'
        #        rangeData = sheetStr + '!' + Col[0] + '2:' + Col[1]
        #    elif not fixedRef is None:
        #        rangeHeader = sheetStr + '!' + fixedRef[0]
        #        rangeData = sheetStr + '!' + fixedRef[1]
        #    fields = shtEng.get_rangevalues(self.sheetIds[wkbkey],rangeHeader)[0]
        #    data = shtEng.get_rangevalues(self.sheetIds[wkbkey],rangeData)
        #    df = pd.DataFrame(data,columns=fields)    
        #?self.parField = self.tblData.columns[0]
        #?self.valField = self.tblData.columns[self.valueCol-1]
        #self.fields = self.tblData.columns
        #self.parameterLabels = list(self.tblData[self.parField])
        #self.parameterKeys = dict(zip(self.parameterLabels,list(self.tblData.index)))
        pass
    def commit(self):
        #values = self.gsheet_compatible()
        #rangeName = self.range_address()
        #shtEng.set_rangevalues(self.sheetId,rangeName,values)
        pass
    def range_address(self,key=None,p_id=None,fullrow=True):
        #if (key is None and p_id is None and fullrow):
        #    localAddress = self.col[0] + str(2) + ':' + self.col[-1]
        #else:
        #    if p_id is None:
        #        p_id = self.get_parameterId(key)
        #    if fullrow:
        #        localAddress = self.col[0] + str(p_id+2) + ':' + self.col[-1] + str(p_id+2)
        #    else:
        #        localAddress = chr(ordRef['A']+self.valueCol-1) + str(p_id+2)
        #return self.pageId + '!' + localAddress
        pass
    def gsheet_compatible(self,data=None):
        #if data is None:
        #    data = self.tblData
        #rows = [list(x) for x in data.to_records(index=False)]
        #for i in range(len(rows)):
        #    for j in range(len(rows[0])):
        #        if type(rows[i][j])== np.int64:
        #            rows[i][j] = int(rows[i][j])
        #return rows
        pass

def example_ReadFromSheet():
    """Shows basic usage of the Sheets API.

    Creates a Sheets API service object and prints the names and majors of
    students in a sample spreadsheet:
    https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit
    """
    credentials = get_credentials()
    http = credentials.authorize(Http())
    service = build('sheets', 'v4', http=http)

    spreadsheetId = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
    rangeName = 'Class Data!A2:E'
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeName).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        print('Name, Major:')
        for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            print('%s, %s' % (row[0], row[4]))

#sample #2

def get_rangedata():
    global rangedata
    #WARNING : last time tested 2020-09-04 API key didn't work for private access
    #workaround was to make it public -- not idea.

    # for every sheet that you plan to access you must give permission to the service account
    # -->service account id: gsheets@helvasheets.iam.gserviceaccount.com
    #import GsheetsAPI.googlesheetsapi as xxx
    wkbid = '1nG5mLBhYam7GpEOumd6NN2Rer7tdrsi0M53a3LJXBoM'
    rangeid = 'NewActivity!A2:V'

    load()
    engine = SheetsEngine()
    rangedata = engine.get_rangevalues(wkbid, rangeid)

    #to set range data :
    #values = ....
    #engine.set_rangevalues(wkbid, rangeid,values)

    #method #2 .get_tabledata
    #re-use workbook which is already added to the engine
    #and separate the shtid from the A2 range notation
    #shtid = 'NewActivity'

    #engine.add_sheet('wkb',wkbid)
    #data = self.gsEng.get_tabledata('wkb',shtid,['A','V'])

    #public sample (read only)
    #spreadsheetId = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
    #rangeName = 'Class Data!A2:E'


if __name__ == '__main__':
    autorun()
