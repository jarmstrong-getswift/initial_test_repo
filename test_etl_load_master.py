import pyodbc
import test_mssql_to_scylla
import threading

conn = pyodbc.connect('DSN=MYMSSQL;UID=userid;Database=databasename;PWD=password')
conn.autocommit = False
cursor = conn.cursor()

success_status = 'SUCCESS'
running_status = 'RUNNING'
failure_status = 'FAILURE'
process_name = 'archivejobs'
sql = ''

def try_sql_commit(sql_in):
    try:
        cursor.execute(sql_in)
    except pyodbc.DatabaseError as err:
        conn.rollback()
    else:
        conn.commit()

def main():
    cursor.execute("SELECT id, archivefromdatetime, archivetodatetime FROM archivecontrol WHERE id NOT IN (SELECT archive_control_rec_id FROM etl_control WHERE process_status IN (?,?))",(success_status, running_status))
    rows = cursor.fetchall()
    for row in rows:

        sql_log = "INSERT INTO etl_control(process_name, archive_control_rec_id, archive_from_date_time, archive_to_date_time, process_status) VALUES \
        (" + "'" + process_name + "'," + str(row.id) + ",'" + str(row.archivefromdatetime) + "','" + str(row.archivetodatetime) + "','" + running_status + "')"
        print (sql_log)
        try_sql_commit(sql_log)

        thread = threading.Thread(target=test_mssql_to_scylla.etl_df(row.id))
        thread.start()
        thread.join()

        sql_log = "UPDATE etl_control SET process_status = '" + success_status + "' WHERE archive_control_rec_id = " + str(row.id)
        print (sql_log)
        try_sql_commit(sql_log)

        #update etlcontrol table with processtatus = 'SUCCESS' (or FAILURE) after run
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()

#######################################################################################################################################################
# 1. Query archivecontrol table and for any archive_control_rec_id's that are not in etlcontrol.archive_control_rec_id where process_status = 'SUCCESS'
# 2. Open cursor to insert record into etlcontrol table with process_status = 'RUNNING'
# 3. Call etl_load_dataframe.py and load particular archive_control_rec_id into scylla table.
# 4. Update etlcontrol table with process_status = 'SUCCESS' after run as well as number of rows inserted.
# 5. Update scylla metrics table with requisite information after load as well (s/b redundant with etlcontrol table -- mainly as an exercise to start)
# 6. Rerun any run with a process_status = 'FAILED'
# (*) First check archivejobsstaging table for existence of control record -- if not, pull from archivejobs table
#######################################################################################################################################################
# drop table etl_control;
#
# create table etl_control
# (
#   id int identity
#     constraint pk_etlcontrol
#     primary key,
#   process_name varchar(100) not null,
#   archive_control_rec_id int,
#   archive_from_date_time datetime,
#   archive_to_date_time datetime,
#   process_start_date datetime,
#   process_end_date datetime,
#   process_status varchar(100)
# );
#
# truncate table etl_control
########################################################################################################################################################
# CREATE TABLE archive.archivejobs (
#     partition_year int,
#     partition_month int,
#     partition_merchant_id int,
#     transportjob_id int,
#     archive_control_rec_id int,
#     archive_date timestamp,
#     arrived_at_dropoff_time timestamp,
#     arrived_at_pickup_time timestamp,
#     available_after_time timestamp,
#     completed_time timestamp,
#     created_time timestamp,
#     delivery_price decimal,
#     drop_off_earliest_time timestamp,
#     drop_off_latest_time timestamp,
#     estimate_kilometres float,
#     estimate_minutes float,
#     handler_email text,
#     handler_firstname text,
#     handler_home_address text,
#     handler_id int,
#     handler_lastname text,
#     handler_phone text,
#     handler_region_country text,
#     handler_region_locality text,
#     handler_region_state text,
#     items list<frozen<transportjobs_item>>,
#     job_contact list<frozen<transportjobs_job_contact>>,
#     job_ratings list<frozen<transportjobs_rating>>,
#     job_stages list<frozen<transportjobs_job_stages>>,
#     merchant_email text,
#     merchant_id int,
#     merchant_name text,
#     merchant_region_country text,
#     merchant_region_locality text,
#     merchant_region_state text,
#     order_on_way_time timestamp,
#     order_ready_time timestamp,
#     organisation text,
#     purchase_receipt_total decimal,
#     region text,
#     tax decimal,
#     tax_inclusive_price boolean,
#     tip decimal,
#     PRIMARY KEY ((partition_year, partition_month, partition_merchant_id), transportjob_id)
# ) WITH CLUSTERING ORDER BY (transportjob_id DESC)
#     AND bloom_filter_fp_chance = 0.01
#     AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
#     AND comment = ''
#     AND compaction = {'class': 'SizeTieredCompactionStrategy'}
#     AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
#     AND crc_check_chance = 1.0
#     AND dclocal_read_repair_chance = 0.1
#     AND default_time_to_live = 0
#     AND gc_grace_seconds = 3600
#     AND max_index_interval = 2048
#     AND memtable_flush_period_in_ms = 0
#     AND min_index_interval = 128
#     AND read_repair_chance = 0.0
#     AND speculative_retry = '99.0PERCENTILE';
########################################################################################################################################################
