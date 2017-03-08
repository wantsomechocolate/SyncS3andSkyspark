import sync_logfiles_module as sync
from datetime import timedelta
from datetime import datetime

## Create query string for getting records newer than certain time
das_id1="001EC6002304" #90WCD
das_id2="001EC6053C99" #311


duration_minutes=60*25
duration=timedelta(minutes=duration_minutes)

from_date=(datetime.utcnow()-duration) #-offset
to_date=datetime.utcnow() #-offset

## Cerate query
query_string = """SELECT * FROM log_files WHERE (das_id = %(das_id1)s OR das_id = %(das_id2)s) AND date_added >= %(from_date)s AND date_added < %(to_date)s"""
query_params = {'das_id1' : das_id1, 'das_id2' : das_id2, 'from_date': from_date, 'to_date': to_date}

## Other stuff
SAVE_DIR='/var/skyspark/proj/tenantBilling/io/logfiles'
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















