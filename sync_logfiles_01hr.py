import sync_logfiles_module as sync
from datetime import timedelta
from datetime import datetime

## Set datetimes for all sites
from_date=datetime.utcnow()-timedelta(hours=25),
to_date=datetime.utcnow(),

## The main dictionary for skyspark projects. Comment out lines if you do not want to update a certain project.
info_dict=dict(

		########## CodeGreen Projects ###########



		####### Tenant billing projects.

#                tbp_90WCD=dict(
#                    das_id="001EC6002304",
#                    proj_name="tenantBilling",
#                    from_date=from_date,
#                    to_date=to_date,
#                    ),

                tbp_311=dict(
                    das_id="001EC6053C99",
                    proj_name="tenantBilling",
                    from_date=from_date,
                    to_date=to_date,
                    ),

		tbp_610BWAY=dict(
		    das_id="001EC600229C",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_LIN=dict(
		    das_id="001EC60012BB",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_MPX=dict(
		    das_id="001EC600134B",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_MPX2=dict(
		    das_id="001EC60011C0",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_MTOP=dict(
		    das_id="001EC6001077",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_MTOP2=dict(
		    das_id="001EC6050D8B",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_WHV=dict(
		    das_id="001EC6051D64",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_WOT=dict(
		    das_id="001EC6001127",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_001EC60011C5=dict(
		    das_id="001EC60011C5",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_001EC60025B4=dict(
		    das_id="001EC60025B4",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_001EC6051266=dict(
		    das_id="001EC6051266",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

		tbp_900Hart=dict(
		    das_id="001EC600284D",
		    proj_name="tenantBilling",
		    from_date=from_date,
		    to_date=to_date,
		    ),

                tbp_mad400_TB=dict(
                    das_id="001EC6052E9A",
                    proj_name="tenantBilling",
                    from_date=from_date,
                    to_date=to_date,
                    ),

#                tbp_mad400_CG=dict(
#                    das_id="001EC6052E9A",
#                    proj_name="codeGreen",
#                    from_date=from_date,
#                    to_date=to_date,
#                    ),

#		tbp_sobr=dict(
#		    das_id="001EC60025B4",
#		    proj_name="codeGreen",
#		    from_date=from_date,
#		    to_date=to_date,
#		    ),

#		tbp_EDWOODS=dict(
#		    das_id="001EC6051266",
#		    proj_name="codeGreen",
#		    from_date=from_date,
#		    to_date=to_date,
#		    ),

		####### AndazHotel Project ######

		# andazHotel=dict(
		#     das_id="001EC60020E0",
		#     proj_name="andazHotel",
		#     from_date=from_date,
		#     to_date=to_date,
		#     ),


               )

## Initialize some variables
TABLE_NAME='log_files'
DATE_COLUMN='date_added'


## Loop through all the projects
for project, project_info in info_dict.items():

    ## Extract the info about the current project
    das_id=project_info["das_id"]
    proj_name=project_info["proj_name"]

    ## Extract date info
    from_date=project_info["from_date"]
    to_date=project_info["to_date"]

    ## Generate the query string
    ## For 1 DAS
    query_string = ("SELECT * FROM log_files WHERE das_id = %(das_id)s AND date_added >= %(from_date)s AND date_added < %(to_date)s AND "
		    "log_files.id in (SELECT logfile_import_status.logfile_id FROM logfile_import_status WHERE logfile_import_status.importedTo_skysparkTenantBilling = 'F')"
		   )

    query_params = {'das_id' : das_id, 'from_date': from_date, 'to_date': to_date}

    ## For 3 DAS
    #query_string = """SELECT * FROM log_files WHERE (das_id = %(das_id)s OR das_id = %(das_id2)s OR das_id = %(das_id3)s) AND date_added >= %(from_date)s AND date_added < %(to_date)s"""
    #query_params = {'das_id1' : das_id1, 'das_id2' : das_id2, 'das_id3' : das_id3, 'from_date': from_date, 'to_date': to_date}

    ## Where to save the files?
    SAVE_DIR='/home/skyspark/Projects/Skyspark/Global/var/skyspark/proj/'+proj_name+'/io/logfiles'

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










