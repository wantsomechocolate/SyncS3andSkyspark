import boto
import sys, os
from boto.s3.key import Key
from datetime import timedelta
from datetime import datetime


## This function returns a bucket cursor object
def connect_to_s3():
    ## Get credentials - use env variables for sensative info like this
    ## don't include directly in script.
    AWS_ACCESS_KEY_ID     = os.environ['AWS_S3_KEY_CG']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_S3_SECRET_CG']

    ## Get bucket info - use env variable here as well.
    ## I didn't for prefix becuase I already have env variable pointing at test bucket.
    BUCKET_NAME   = os.environ['AWS_S3_BUCKET_NAME_CG']
    #BUCKET_PREFIX=os.environ['AWS_S3_BUCKET_PREFIX_CG']
    BUCKET_PREFIX = os.environ['AWS_S3_BUCKET_PREFIX_CG'] #'acquisuite'

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
def save_files_newer_than(
    query_string,
    query_params,
    from_date=datetime.utcnow()-timedelta(minutes=60),
    to_date=datetime.utcnow(),    
    save_dir='io/logfiles',
    table_name='log_files',
    date_column='date_added',
    ):

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

        ## Execute the query
        log_write("Querying the table {} for files".format(table_name), print_to_terminal=True)
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
        bucket_prefix=os.environ['AWS_S3_BUCKET_PREFIX_CG']
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

    ## Close database connections
    log_write("", end='\n')
    log_write("{} new logfiles added".format(number_of_new_files),  print_to_terminal=True)
    log_write("{} logfiles that could not be found on s3".format(number_of_not_found),  print_to_terminal=True)
    log_write("{} existing logfiles".format(number_of_existing_files),  print_to_terminal=True)
    log_write("Program complete, closing database connection", end='\n\n', print_to_terminal=True)
    conn.close()

