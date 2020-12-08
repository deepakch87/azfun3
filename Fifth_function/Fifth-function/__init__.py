import logging
import os
import azure.functions as func
import datetime
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from datetime import timedelta
storage_account_name=os.getenv('STORAGE_ACCOUNT_NAME')
storage_account_key=os.getenv('STORAGE_ACCOUNT_KEY')

def list_directory_contents():
    try:
        file_system_client = service_client.get_file_system_client(file_system="backup")
        paths = file_system_client.get_paths(path="loki/chunks")
        filenames = ""       
        global count
        count = 0
                
        for path in paths:
            filenames = filenames + "," + path.name
            today = datetime.datetime.now().date()
            filedate_string = path.last_modified
            date_time_obj = datetime.datetime.strptime(path.last_modified, "%a, %d %b %Y %H:%M:%S %Z")
            date_time_obj_cst = date_time_obj - timedelta(hours=5, minutes=0)
            filedate = date_time_obj_cst.date()
            days_since = (today - filedate).days
            filedatefinal = filedate.strftime("%Y-%m-%d")
            
            if days_since < 29:
            #if days_since > 7:
                count = count + 1
                logging.info(path.name + "," + filedatefinal)
                file_system_client.delete_file(file=path.name)
        
        #print('total files which will be deleted: {}'.format(count))
        #print('total files pending for deletion: {}'.format(count))
        #print('total files which are be deleted: {}'.format(count))
        #logging.info(mylist)
        return filenames
        
    except Exception as e:
     print(e)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", storage_account_name), credential=storage_account_key)

        filenames_str = list_directory_contents()
        #filenames_list = filenames_str.split(",")
        #filenames_list_final = filenames_list[1:]
    
    except Exception as e:
        print(e)

    
            
    #return func.HttpResponse(mylist)
    return func.HttpResponse("Hello,  Below files deleted from storage account \n {} .".format(filenames_str))

