import boto
import sys, os
from boto.s3.key import Key
from datetime import timedelta
from datetime import datetime


## This function returns a bucket cursor object
def connect_to_s3():
    ## Get credentials - use env variables for sensative info like this
    ## don't include directly in script.
    AWS_ACCESS_KEY_ID = os.environ['AWS_S3_KEY_CG']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_S3_SECRET_CG']

    ## Get bucket info - use env variable here as well.
    ## I didn't for prefix becuase I already have env variable pointing at test bucket.
    BUCKET_NAME = os.environ['AWS_S3_BUCKET_NAME_CG']
    #BUCKET_PREFIX=os.environ['AWS_S3_BUCKET_PREFIX_CG']
    BUCKET_PREFIX='acquisuite'

    ## Connect to the bucket - bucket here is analogous to the cursor
    ## object you typically see, I guess.
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(BUCKET_NAME)

    return bucket


## This function returns a database connection object
def connect_to_db():
    log_write("About to import psycopg2")
    import psycopg2
    import os
    log_write("About to try and access an environment variable")
    conn = psycopg2.connect(os.environ['CODEGREENAPPS_DB'])
    return conn

## This function takes a connection object and a query string and returns a cursor
def query_db(conn, query_string, query_params):
    cur = conn.cursor()
    if query_params==None:
        cur.execute(query_string)
    else:
        cur.execute(query_string, query_params)
    return cur

def log_write(message, end='\n', base_dir="", print_to_terminal=False):
    import os
    base_dir=os.path.dirname(os.path.realpath(__file__))
    log = open(os.path.join(base_dir,'log.txt'),'a')
    if message == 'sep':
        message = "------------------------------------------------------------"
    log.write("[{}]: {}{}".format(datetime.now(),message, end))
    log.close()

    if print_to_terminal == True:
        print("[{}]: {}{}".format(datetime.now(),message, end))
    
    return None

## This function uses the above functions to get data out of postgres including files from s3
## It doesn't return anything. The inputs are a table name
def save_files_newer_than(save_dir='io/logfiles',table_name='log_files', date_column='date_added', interval_string=None, bucket_prefix='acquisuite'):
    import psycopg2
    
    from datetime import timedelta
    
    ## Start logfile
    #log = open('log.txt','a')
    #seperator="\n------------------------------------------------------------\n"
    
    log_write("Starting logfile transfer", print_to_terminal=True)

    ## Connect to DB and retrieve records
    try:
        log_write("Attempting to connect to database", print_to_terminal=True)
        conn = connect_to_db()
        log_write("Connection successful", print_to_terminal=True)

        ## Create query string for getting records newer than certain time
        #query_string = """SELECT * FROM {} WHERE {} > NOW() - INTERVAL '{}'""".format(table_name, date_column, interval_string)

        

        #multiplier=9
        
        #offset_days=0
        #duration_days=1
        duration_hours=36
        #duration_minutes=60*24*7

        #duration=timedelta(days=duration_days)
        duration=timedelta(hours=duration_hours)
        #duration=timedelta(minutes=duration_minutes)
        #offset=timedelta(days=offset_days)

        from_date=(datetime.utcnow()-duration) #-offset
        to_date=datetime.utcnow() #-offset

        #from_date=datetime(2017,1,31,0,0,0)
        #to_date=datetime(2017,2,5,0,0,0)

        das_id="001EC60020E0" #andaz
        #das_id="001EC6052E9A" #400 madison
        #das_id="001EC6051D64" # woodhaven
        
        query_string = """SELECT * FROM log_files WHERE das_id = %(das_id)s AND date_added >= %(from_date)s AND date_added < %(to_date)s"""
        query_params = {'das_id' : das_id, 'from_date': from_date, 'to_date': to_date}

        ## Execute the query
        log_write("Querying the table {} for file that are less than {} old based on the {} column".format(table_name, interval_string, date_column), print_to_terminal=True)
        #cur = query_db(conn,query_string)
        cur = query_db(conn,query_string,query_params)
        
        log_write("Query successful", print_to_terminal=True)
    
        ## Fetch the records
        log_write("Fetching the records associated with the query", print_to_terminal=True)
        records = cur.fetchall()
        log_write("{} records were retrieved".format(len(records)), print_to_terminal=True)
        
    except psycopg2.OperationalError:
        log_write("There was an operational error, This probably means that there is no internet connection, exiting", print_to_terminal=True)
        goodbye=1/0

    
    ## Connect to the AWS S3 bucket
    try:
        log_write("Attempting to connect to S3", print_to_terminal=True)
        bucket = connect_to_s3()
        log_write("Attempt completed", print_to_terminal=True)
    except socket.gaierror:
        log_write("Error connecting to S3, probably internet connectivity, exiting", print_to_terminal=True)
        goodbye=1/0
    

    log_write("Starting to loop through records and save in: {}".format(save_dir), print_to_terminal=True)
    ## Save each record
    number_of_new_files = 0
    number_of_not_found = 0
    number_of_existing_files = 0
    for record in records:

## ADDED BECAUSE OF LICENSE ISSUE
        if record[6]==das_id or record[1].startswith(das_id):

            #log_write("Record: {}".format(record), print_to_terminal=True)

            ## The s3_key is stored in the 6th field, get the key for downloading file and the file extension for saving the file
            s3_key = record[3]
            s3_key_file_ext = os.path.splitext(s3_key)[-1]

            ## Get the last modified date (currently 5th field)
            ## I wanted to include the microseconds (%f), but apparently I'm not adding them
            ## on the way in! I'm pretty sure that multiple logfiles for the same device
            ## should not be added within the same second...
            date_added=record[4]
            ## http://strftime.org/ for reference on date formatting
            date_added_string = date_added.strftime("-%Y_%m_%d_%I_%M_%S")

            ##Get the info about the device
            server_id=record[6]
            device_id=record[1]

            if server_id==None:
                server_id_device_id=record[1]
                server_id=server_id_device_id.split("_")[0]
                device_id=server_id_device_id.split("_")[1]

            ## Put the filename together. server_id, device_id, date_added, and file extension
            filename = server_id+"_"+device_id+date_added_string+s3_key_file_ext

            ## Get the serial number and modbus ID from the filename to create the final save directory location
            #filename_parts = filename.split('_')
            #acquisuite_id = filename_parts[0]
            #modbus_id = filename_parts[1].split('-')[0]

            ## Concatonate everything together
            save_dir_device = os.path.join(save_dir,server_id,device_id)

            ## Use os.path.join to join path and filename to get filepath
            filepath = os.path.join(save_dir_device,filename)

            ## Get the file from s3 as a filehandle (or buffer whatever)
            fh = bucket.get_key(os.path.join(bucket_prefix,s3_key))

            ## Check to see if save directory exists
            if not os.path.exists(save_dir_device):
                log_write("Save directory doesn't exist, making it now",  print_to_terminal=True)
                ## makedirs will create all directories down to the last one even if some of the intermediate ones don't exist. 
                os.makedirs(save_dir_device)

            
            ## Check to see if file exists
            if not os.path.exists(filepath):
                ## If it doesn't then try saving the file
                try:
                    fh.get_contents_to_filename(filepath)
                    log_write("SAVED: {} || ".format(filename),end="")
                    number_of_new_files+=1
                except AttributeError:
                    log_write("NOT FOUND IN S3: {} || ".format(filename), end="")
                    number_of_not_found+=1
            else:
                ## If it does, then move on.
                log_write("SKIPPED: {} || ".format(filename), end="")
                number_of_existing_files+=1

    ## Don't forgot to close your database connections, man.
    log_write("", end='\n')
    log_write("{} new logfiles added".format(number_of_new_files),  print_to_terminal=True)
    log_write("{} logfiles that could not be found on s3".format(number_of_not_found),  print_to_terminal=True)
    log_write("{} existing logfiles".format(number_of_existing_files),  print_to_terminal=True)
    log_write("Program complete, closing database connection", end='\n\n', print_to_terminal=True)
    conn.close()


###########################################################################
########################## RUNNING THE CODE ###############################
###########################################################################

log_write('sep')
log_write("Script started", end="\n")

BASE_DIR=os.path.dirname(os.path.realpath(__file__))
#SAVE_DIR=os.path.join(BASE_DIR,'io/logfiles')
#Save dir for skyspark2
#SAVE_DIR='/opt/skyspark/db/demo/io/logfiles'
#Save dir for skyspark3
SAVE_DIR='/opt/skyspark3/var/proj/andazHotel/io/logfiles'
TABLE_NAME='log_files'
DATE_COLUMN='date_added'
#INTERVAL_STRING='2 hours'
INTERVAL_STRING='7 days'
# This is only here because I already have env variable pointing somewhere else, usually this is fetched from within connection function
BUCKET_PREFIX='acquisuite'
save_files_newer_than(save_dir=SAVE_DIR, table_name = TABLE_NAME, date_column=DATE_COLUMN, interval_string=INTERVAL_STRING, bucket_prefix=BUCKET_PREFIX)




###########################################################################
########################## UNUSED FUNCTIONS ###############################
###########################################################################

def save_file_from_key(bucket, key, prefix):
    fh = bucket.get_key(os.path.join(prefix,key))
    fh.get_contents_to_filename(str(key))
    return fh

def save_files_newer_than_old(bucket,days,save_path):
    # Go through the list of files
    bucket_list = bucket.list()
    
    for item in bucket_list:
        item_modified_date = datetime.strptime(item.last_modified, '%Y-%m-%dT%H:%M:%S.000Z')
        threshold_date = datetime.now()-timedelta(days=1)

        if item_modified_date > threshold_date:
            #print ("file was new enough")
            #print (item_modified_date)
            keyString = str(item.key)
            if not os.path.exists(os.path.join(save_path,keyString)):
                item.get_contents_to_filename(os.path.join(save_path,keyString))


###########################################################################
############################ SCRATCH WORK #################################
###########################################################################
## This was to test saving file with db info
#### Connect to DB
##conn = connect_to_db()
##
#### Example to get a single key
###query_string = """SELECT * FROM log_files"""
###query_string = """SELECT * FROM log_files WHERE date_added > NOW() - INTERVAL '2 hours'"""
##
##
##query_string = """SELECT * FROM {} WHERE {} > NOW() - INTERVAL '{}'""".format(TABLE_NAME, DATE_COLUMN, INTERVAL_STRING)
##
##records = cur.fetchall()
##bucket = connect_to_s3()


###########################################################################
###########################################################################
## This was to test saving file without db info
#### Sample code for single file
##s3_key = rows[0][3]
##
#### Example to get file using key
##bucket = connect_to_s3()
##fh = bucket.get_key(os.path.join(BUCKET_PREFIX,s3_key))
##
##filename=rows[0][1]
##file_ext='.gz'
##filename = filename+file_ext
##fh.get_contents_to_filename(filename)
##
#### Getting only the most recent files
##from datetime import timedelta
##cutoff_date = datetime.utcnow()-timedelta(hours=2)


###########################################################################
###########################################################################
## Other queries
##query_headers="""select column_name from information_schema.columns where table_name='log_files'"""



##def fn(dStart, dEnd):
##    query = """ 
##        SELECT "recordId" 
##        FROM "Table" 
##        WHERE "dateTime" >= %(start)s AND "dateTime" < %(end)s + interval '1 day'
##    """
##    query_params = {'start': dStart, 'end': dEnd}
##    df1 = pd.read_sql_query(query1, con=engine, params=query_params)
##    return df1













