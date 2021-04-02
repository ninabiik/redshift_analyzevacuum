import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import pg8000 as pg
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import math as m

client = boto3.client('ssm')

## @params: [JOB_NAME]
args_keys = [
    "JOB_NAME",
    "STAGE"
]
args = getResolvedOptions(sys.argv, args_keys)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

stage = args["STAGE"]

if stage == "prod":
    _stage = ""
else:
    _stage = stage + "_"

db_name = "/redshiftApp/" + stage + "/db-name"
db_password = "/redshiftETL/" + stage + "/db-password"
db_port = "/redshiftApp/" + stage + "/db-port"
db_url = "/redshiftApp/" + stage + "/db-url"
db_user = "/redshiftETL/" + stage + "/db-username"

response = client.get_parameters(
    Names=[
        db_name,
        db_password,
        db_port,
        db_url,
        db_user
    ],
    WithDecryption=True|False
)

try:
    conn = pg.connect(
        database=response['Parameters'][0]['Value'],
        password=response['Parameters'][3]['Value'],
        port=response['Parameters'][1]['Value'],
        host=response['Parameters'][2]['Value'],
        user=response['Parameters'][4]['Value'],
        ssl_context=True
    )
except Exception as err:
    print(err)

output_schema = "{output_schema}"

query_str_get_tables = """SELECT table_name 
                        from information_schema.tables 
                        where table_schema = '{output_schema}' and table_name like 'tbl_%'"""
   
conn.autocommit = True
cursor = conn.cursor()
cursor.execute(query_str_get_tables)
records = cursor.fetchall()

for row in records:
    tablename = row[0]
    print(tablename)
    query_str_check_vacuum = '''SELECT unsorted, stats_off
                                 FROM svv_table_info 
                                 where "table" = '%s'
                                 and (unsorted > 10 or stats_off >10)'''%(tablename)
    cursor.execute(query_str_check_vacuum)
    res_vacuum_analyze = cursor.fetchone()
    
    res_vacuum = 0
    res_analyze = 0
    if res_vacuum_analyze and len(res_vacuum_analyze) > 0:
         res_vacuum = float(res_vacuum_analyze[0]) if res_vacuum_analyze[0] else 0
         res_vacuum = int(m.ceil(res_vacuum))

         res_analyze = float(res_vacuum_analyze[1]) if res_vacuum_analyze[1] else 0
         res_analyze = int(m.ceil(res_analyze))
    
    print("Table: {} - unsorted:{} statsoff:{}\n".format(tablename, res_vacuum, res_analyze))
    print("Start vacuum full on {}...\n".format(tablename))
    if res_vacuum and res_vacuum > 10:
        run_vacuum = '''VACUUM FULL "{output_schema}"."%s"'''%(tablename)
        print(run_vacuum)
        cursor.execute(run_vacuum)
    
    print("Start vacuum analyze on {}...".format(tablename))
    if res_analyze and res_analyze > 10:
        run_analyze = '''ANALYZE "{output_schema}"."%s"'''%(tablename)
        print(run_analyze)
        cursor.execute(run_analyze)

conn.commit()
cursor.close()
conn.close()

print("Maintenance Completed")

job.commit()