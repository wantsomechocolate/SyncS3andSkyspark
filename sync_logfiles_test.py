## To run this:
## navigate to directory with script
## source env/bin/activate
## python sync_logfiles_historical.py


import sync_logfiles_module as sync
from datetime import timedelta
from datetime import datetime

## Examples of datetimes
##from_date=datetime.utcnow()-timedelta(hours=23),
from_date=datetime(2017,11,9,19,0,0),
to_date = datetime(2017,11,14,20,0,0),
#to_date=datetime.utcnow(),

## The main dictionary for skyspark projects. Comment out lines if you do not want to update a certain project.
info_dict=dict(

		tbp_900Hart=dict(
		    das_id="001EC600284D",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

               )

## Initialize some variables
TABLE_NAME='log_files'
DATE_COLUMN='date_added'

## Set dates for all projects
#duration_minutes=60
#duration=timedelta(minutes=duration_minutes)
#from_date=datetime(2017,3,13,0,0,0)  #(datetime.utcnow()-duration) #-offset
#to_date=datetime.utcnow() #-offset

for project, project_info in info_dict.items():

    ## Extract the info about the current project
    das_id=project_info["das_id"]
    proj_name=project_info["proj_name"]

    ## Extract date info
    from_date=project_info["from_date"]
    to_date=project_info["to_date"]

    ## Generate the query string
    ## For 1 DAS
    query_string = """SELECT * FROM log_files WHERE das_id = %(das_id)s AND date_added >= %(from_date)s AND date_added < %(to_date)s"""
    query_params = {'das_id' : das_id, 'from_date': from_date, 'to_date': to_date}

    ## For 3 DAS
    #query_string = """SELECT * FROM log_files WHERE (das_id = %(das_id)s OR das_id = %(das_id2)s OR das_id = %(das_id3)s) AND date_added >= %(from_date)s AND date_added < %(to_date)s"""
    #query_params = {'das_id1' : das_id1, 'das_id2' : das_id2, 'das_id3' : das_id3, 'from_date': from_date, 'to_date': to_date}

    ## Where to save the files?
    SAVE_DIR='/home/wantsomechan/Projects/SyncS3andSkyspark/'+proj_name+'/io/logfiles'

    ## Examples
    #SAVE_DIR='/home/skyspark/Projects/Skyspark/Global/var/skyspark/proj/andazHotel/io/logfiles' #andaz
    #SAVE_DIR='/home/skyspark/Projects/Skyspark/Global/var/skyspark/proj/codeGreen/io/logfiles' #general
    #SAVE_DIR='/home/skyspark/Projects/Skyspark/Global/var/skyspark/proj/tenantBilling/io/logfiles' #tenantBilling

    ## Make output look a little better
    sync.log_write('sep', print_to_terminal=True)
    sync.log_write('sep', print_to_terminal=True)
    sync.log_write("Script started for project: "+str(project), end="\n", print_to_terminal=True)

    ## Finally, call the script that saves the files. 
    sync.save_files_newer_than(
        query_string=query_string,
        query_params=query_params,
        from_date=from_date,
        to_date=to_date,
        save_dir=SAVE_DIR,
        table_name = TABLE_NAME,
        date_column=DATE_COLUMN,
        )

