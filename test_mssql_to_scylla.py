#had to reinstall pyspark (pip3 install pyspark) and cassandra (now cassandra-driver) after Mac "python apocalypse"
#pyspark install is in my other pyspark work instruction
#pip3 install cassandra-driver --to install "cassandra" package
from pyspark import SparkContext, SparkConf, SQLContext
from cassandra.cluster import Cluster
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 --jars /home/vagrant/mssql-jdbc-7.2.1.jre8.jar pyspark-shell'

# the following works in local pyspark config -- insert is sloooowww running in local mode
# appName = "PySpark SQL Server Example - via JDBC"
# master = "local"
# conf = SparkConf() \
#     .setAppName(appName) \
#     .setMaster(master) \
#     .set("spark.driver.extraClassPath","/home/vagrant/mssql-jdbc-7.2.1.jre8.jar")
# sc = SparkContext(conf=conf)
# sqlContext = SQLContext(sc)
# spark = sqlContext.sparkSession

#this is just all of the config parameters on one line (don't ask -- I had thought it was perhaps a formatting issue with my conf variable previously, hence the single line...)
#conf = SparkConf().setAppName('cassandra app').set("spark.cassandra.connection.host", "18.189.14.246").setMaster('spark://192.168.2.107:7077').set("spark.driver.host","192.168.2.107").set("spark.driver.extraClassPath","/home/vagrant/mssql-jdbc-7.2.1.jre8.jar")

# now attempt to get this running on the actual cluster...
appName = "PySpark SQL Server Example - via JDBC"
## master = "local"
conf = SparkConf().setAppName('cassandra app') \
	.set("spark.cassandra.connection.host", "18.189.14.246") \
        .setMaster("spark://192.168.2.107:7077") \
        .set("spark.driver.host","192.168.2.107") \
	.set("spark.driver.extraClassPath","/home/vagrant/mssql-jdbc-7.2.1.jre8.jar")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession

database = "archive"
table = "dbo.archiveerrors_spark"
user = "username"
password  = "password"
mssql_ip_address = "172.33.8.126"
scylla_ip_address = "18.189.14.246,18.221.226.93,18.189.31.231"
keyspace_name = "cash_management"
scylla_table = "archiveerrors"
id_input = 1

def etl_df(archive_control_rec_id_in):

    sql_query = "SELECT partition_year, partition_month, partition_merchant_id, organisation, transportjob_id, handler_id, \
    handler_firstname, handler_lastname, merchant_name, delivery_price, purchase_receipt_total, tax, CAST(tax_inclusive_price AS int) tax_inclusive_price, \
    tip, estimate_minutes, estimate_kilometres,  created_time, completed_time, order_ready_time, available_after_time,drop_off_earliest_time, drop_off_latest_time, \
    order_on_way_time, arrived_at_pickup_time, arrived_at_dropoff_time, handler_email, handler_phone, handler_home_address, handler_region_state, \
    handler_region_country, handler_region_locality, merchant_email, merchant_region_state, merchant_region_country, merchant_region_locality, items, \
    job_stages, job_contact, job_ratings, archive_control_rec_id, archive_date \
    FROM dbo.archivejobs \
    WHERE archive_control_rec_id = " + str(archive_control_rec_id_in)

    #print ("SQL_QUERY IS", sql_query)

    df = spark.read.format("jdbc").option("url", "jdbc:sqlserver://172.33.8.126:1433;databaseName=archive").option("query", sql_query).option("user", user).option("password", password).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

    #print ("testing df output")
    #df.show()

    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .option("spark.cassandra.connection.host", "18.189.14.246") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(table="archivejobs", keyspace="cash_management") \
        .save()

###########################################################################################################################
# option 1   - load into dataframe, insert dataframe e.g., https://stackoverflow.com/questions/49108809/how-to-insert-pandas-dataframe-into-cassandra
# option 1.5 - load into dataframe and batch out, insert batches e.g., https://stackoverflow.com/questions/49108809/how-to-insert-pandas-dataframe-into-cassandra
# option 2   - load output into a map function e.g., https://www.slideshare.net/DataStax/using-spark-to-load-oracle-data-into-cassandra-jim-hatcher-ihs-markit-c-summit-2016
# option 3   - dump data to a flat file, use bulk load utility sstableloader e.g., https://www.datastax.com/blog/2014/09/using-cassandra-bulk-loader-updated
# option 4   - talend
# option 5   - any number of the above things, but ensuring spark functionality is workable in context
###########################################################################################################################
#
# CREATE TABLE cash_management.archiveerrors (
#     id int,
#     archivetodatetime text,
#     applicationindicator text,
#     archivecontrol_id int,
#     archiveerror text,
#     archivefromdatetime text,
#     created text,
#     databasename text,
#     servername text,
#     PRIMARY KEY (id, archivetodatetime)
# ) WITH CLUSTERING ORDER BY (archivetodatetime ASC)
#     AND bloom_filter_fp_chance = 0.01
#     AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
#     AND comment = ''
#     AND compaction = {'class': 'SizeTieredCompactionStrategy'}
#     AND compression = {}
#     AND crc_check_chance = 1.0
#     AND dclocal_read_repair_chance = 0.1
#     AND default_time_to_live = 0
#     AND gc_grace_seconds = 864000
#     AND max_index_interval = 2048
#     AND memtable_flush_period_in_ms = 0
#     AND min_index_interval = 128
#     AND read_repair_chance = 0.0
#     AND speculative_retry = '99.0PERCENTILE';
###########################################################################################################################
#
# CREATE TABLE cash_management.archivejobs (
#     partition_year int,
#     partition_month int,
#     partition_merchant_id int,
#     organisation text,
#     transportjob_id int,
#     handler_id int,
#     handler_firstname text,
#     handler_lastname text,
#     merchant_name text,
#     delivery_price double,
#     purchase_receipt_total double,
#     tax double,
#     tax_inclusive_price int, --not sure what best type for "bit" equivalent is
#     tip double,
#     estimate_minutes float,
#     estimate_kilometres float,
#     created_time timestamp,
#     completed_time timestamp,
#     order_ready_time timestamp,
#     available_after_time timestamp,
#     drop_off_earliest_time timestamp,
#     drop_off_latest_time timestamp,
#     order_on_way_time timestamp,
#     arrived_at_pickup_time timestamp,
#     arrived_at_dropoff_time timestamp,
#     handler_email text,
#     handler_phone text,
#     handler_home_address text,
#     handler_region_state text,
#     handler_region_country text,
#     handler_region_locality text,
#     merchant_email text,
#     merchant_region_state text,
#     merchant_region_country text,
#     merchant_region_locality text,
#     items text,
#     job_stages text,
#     job_contact text,
#     job_ratings text,
#     archive_control_rec_id int,
#     archive_date timestamp,
#     PRIMARY KEY ((partition_year, partition_month, partition_merchant_id), transportjob_id)
# )   WITH bloom_filter_fp_chance = 0.01
#     AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
#     AND comment = ''
#     AND compaction = {'class': 'SizeTieredCompactionStrategy'}
#     AND compression = {}
#     AND crc_check_chance = 1.0
#     AND dclocal_read_repair_chance = 0.1
#     AND default_time_to_live = 0
#     AND gc_grace_seconds = 864000
#     AND max_index_interval = 2048
#     AND memtable_flush_period_in_ms = 0
#     AND min_index_interval = 128
#     AND read_repair_chance = 0.0
#     AND speculative_retry = '99.0PERCENTILE';


#   .option("dbtable", table) \
#   .option("query", "(SELECT id, servername, databasename, applicationindicator, archivefromdatetime, archivetodatetime, archivecontrol_id, archiveerror, created FROM dbo.archiveerrors_spark WHERE id = 1)") \
