import sync_logfiles_module as sync
from datetime import timedelta
from datetime import datetime

## Create query string for getting records newer than certain time
das_id="001EC6052E9A" #400 madison
das_id2="001EC60020E0"
das_id3="001EC6051D64" # woodhaven




duration_minutes=30
duration=timedelta(minutes=duration_minutes)

from_date=(datetime.utcnow()-duration) #-offset
to_date=datetime.utcnow() #-offset

## Change query to allow for all serial numbers (all Acquisuites) because filters are applied later in the code
query_string = """SELECT * FROM log_files WHERE (das_id = %(das_id)s OR das_id = %(das_id2)s OR das_id = %(das_id3)s) AND date_added >= %(from_date)s AND date_added < %(to_date)s"""
query_params = {'das_id' : das_id, 'das_id2' : das_id2, 'das_id3' : das_id3, 'from_date': from_date, 'to_date': to_date}

## Other stuff
SAVE_DIR='/var/skyspark/proj/codeGreen/io/logfiles'        
TABLE_NAME='log_files'
DATE_COLUMN='date_added'

sync.log_write('sep', print_to_terminal=False)
sync.log_write("Script started", end="\n", print_to_terminal=True)

sync.save_files_newer_than(
    query_string=query_string,
    query_params=query_params,
    from_date=from_date,
    to_date=to_date,
    save_dir=SAVE_DIR,
    table_name = TABLE_NAME,
    date_column=DATE_COLUMN,
    )






