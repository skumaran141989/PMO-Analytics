from awsglue.dynamicframe import DynamicFrame
import boto3

def write_to_catalogue(glueContext, df, bucket, partitionKeys):
    data = DynamicFrame.fromDF(df, glueContext, 'rc_data')
    glueContext.write_dynamic_frame.from_options(frame = data, connection_type = 's3',
         connection_options = {'path': bucket, 'partitionKeys' : partitionKeys}, format = 'parquet')

def write_to_es(df, index, esURL, mapping_key = 'id', mode = 'upsert'):
    df.write.mode('append').format('org.elasticsearch.spark.sql').option('es.nodes.wan.only', 'true') \
            .option('es.nodes.client.only', 'false').option('es.net.ssl', 'true').options('es.resource', index/mapping_key) \
            .options('es.mapping.id', 'id').option('es.mapping.id', mapping_key).option('es.write.operation', mode) \
            .option('es.nodes', esURL).option('es.port', '443').mode('append').save()

def get_data_from_s3(bucket, key):
    client = boto3.client('s3')
    data = client.get_object(Bucket = bucket, Key = key)
    contents = data['Body'].read()

    return contents

def fetch_data_from_mysql(spark, driver, url, query, user, password):
    df = spark.read().jdbc(query = query, driver = driver, url = url, fetchsize = 1000, properties = {user : user, password : password})

    return df
