import json
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from org.apache.spark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'], ['JDBC_URL'], ['JDBC_DRIVER'], ['TABLES'],
                          ['S3_CATALOGUE_BUCKET'], ['S3_TUMESTAMNP_BUCKET'], ['ES_URL'])
sc = SparkContext()
sql = SQLContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

url = args['JDBC_URL']
driver = args['JDBC_DRIVER']
s3_write_path_bucket = args['S3_CATALOGUE_BUCKET']
s3_timestamp_bucket = args['S3_TUMESTAMNP_BUCKET']
es_url = args['ES_URL']
tables = json.load(args['TABLES'])

queries = {
    'HUMAN_RESOURCE': { 'query' : 'select hr.id as resource_id, ts.startDate, ts.endDate, hr.type from HumanResource hr left join TimeSlot ts on hr._id = ts._resource_id where ts._updatedAt>=\'{0}\'', 'ts_file': 'HR/timestamp'},
    'MATERIAL_RESOURCE': { 'query' : 'select mr.id as resource_id, mr.consumedDate as day, mr.type from MaterialResource mr where ms._updatedAt>=\'{0}\' and utlized=true', 'ts_file': 'MR/timestamp'},
}

def process(tableName):
    table = queries[tableName]
    last_updated_date = get_data_from_s3(s3_timestamp_bucket, table['ts_file'])
    query = table['query'].format(last_updated_date)
    df = fetch_data_from_mysql(spark, driver, url, query)

    if tableName == 'HUMAN_RESOURCE':
        df = df.withColumn('day', F.explode(F.expr('sequence(startDate, endDate, interval 1 day)')))

    df.createOrReplaceTempView('ResourceCountView')
    rc_df = sql.sql('select count(resource_id), type, day from ResourceCountView group by allocatedDate')
    rc_df = rc_df.withColumn('year', rc_df.to_timestamp(rc_df.day, 'yyyy'))
    rc_df = rc_df.withColumn('month', rc_df.to_timestamp(rc_df.day, 'MM'))

    if len(rc_df.take(1)) > 0:
        write_to_es(rc_df, '{}_statistics_index'.format(tableName.lower()), es_url)
        write_to_catalogue(rc_df, s3_write_path_bucket, ['year', 'month', 'day'])

for tableName in tables:
    process(tableName)

job.commit()





