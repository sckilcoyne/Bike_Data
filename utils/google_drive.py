
# %% Initialize
import os

from pydrive2.drive import GoogleDrive
from pydrive2.auth import GoogleAuth

# %% Authentication

# Below code does the authentication
# part of the code
gauth = GoogleAuth()

# Creates local webserver and auto
# handles authentication.
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)


# %% Upload data folder

path = os.getcwd()
currentFolder = os.path.basename(path)

if currentFolder == 'utils':
    parent = os.path.dirname(path)
    dataFolder = parent + '/data'
else:
    dataFolder = path + '/data'

# iterating thought all the files/folder
# of the desired directory
for x in os.listdir(path):

    f = drive.CreateFile({'title': x})
    f.SetContentFile(os.path.join(path, x))
    f.Upload()

    # Due to a known bug in pydrive if we 
    # don't empty the variable used to
    # upload the files to Google Drive the
    # file stays open in memory and causes a
    # memory leak, therefore preventing its 
    # deletion
    f = None