'''Created on 2018/11/27
    last update 2018/11/27
@author: Taylor W Hickem
'''
from os import listdir, rmdir
from os.path import isfile, isdir, join
import shutil as sh
folder_path = ''

#sh.move(src,dst)
#recursively moves a file or directory to another location

def getSubFolders(folder_path):
    folders = [f for f in listdir(folder_path) 
                 if isdir(join(folder_path,f))]
    return folders

def getFilesInFolder(folder_path):
    files = [f for f in listdir(folder_path) 
                 if isfile(join(folder_path,f))]
    return files

def moveFiles(files,source,destination):
    for file in files:
        sh.move(source + '\\' + file,destination + '\\' + file)

def removeSubFolders(parent_directory):
    subFolders = getSubFolders(parent_directory)
    for folder in subFolders:
        files = getFilesInFolder(parent_directory + folder)
        moveFiles(files,parent_directory + folder,parent_directory)
        rmdir(parent_directory + folder)
