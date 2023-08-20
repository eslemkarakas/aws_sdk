import os
import boto3
import urllib
import configparser
import numpy as np
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine

# create a class for storing static paramaters
class C:
    CURRENT_DIRECTORY = os.getcwd()

# create a ConfigParser object
config = configparser.ConfigParser()

# read the configuration from the file
config.read(C.CURRENT_DIRECTORY + '/config.ini')

def url_parse(s3_path):
    # parse url
    parsed_url = urllib.parse.urlparse(s3_path)

    # split url as bucket and key
    s3_bucket, s3_key = parsed_url.netloc, parsed_url.path.lstrip('/')

    return s3_bucket, s3_key

def s3_upload(config, s3_path, send_path, recieve_path):
    # access the credentials
    region_name, access_key, secret_access_key, session_token = config.get('aws_creds', 'region_name'), config.get('aws_creds', 'access_key'), config.get('aws_creds', 'secret_access_key'), config.get('aws_creds', 'session_token')

    # obtain bucket and key parameters from absolute s3 path
    bucket, key = url_parse(s3_path)

    # create data transferring session with AWS S3
    s3 = boto3.client('s3', region_name=region_name, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key, aws_session_token=session_token)

    # upload file
    s3.upload_file(send_path, bucket, key + recieve_path)

def s3_download(config, s3_path, send_path, recieve_path):
    # access the credentials
    region_name, access_key, secret_access_key, session_token = config.get('aws_creds', 'region_name'), config.get('aws_creds', 'access_key'), config.get('aws_creds', 'secret_access_key'), config.get('aws_creds', 'session_token')

    # obtain bucket and key parameters from absolute s3 path
    bucket, key = url_parse(s3_path)

    # create data transferring session with AWS S3
    s3 = boto3.client('s3', region_name=region_name, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key, aws_session_token=session_token)

    # download file
    s3.download_file(send_path, key + send_path, recieve_path)

def s3_delete(config, s3_path, target_path):
    # access the credentials
    region_name, access_key, secret_access_key, session_token = config.get('aws_creds', 'region_name'), config.get('aws_creds', 'access_key'), config.get('aws_creds', 'secret_access_key'), config.get('aws_creds', 'session_token')

    # obtain bucket and key parameters from absolute s3 path
    bucket, key = url_parse(s3_path)

    # create data transferring session with AWS S3
    s3 = boto3.client('s3', region_name=region_name, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key, aws_session_token=session_token)

    # delete file
    s3.delete_object(Bucket=bucket, Key=key+target_path)

def s3_read(config, s3_path, file_type):
    # access the credentials
    region_name, access_key, secret_access_key, session_token = config.get('aws_creds', 'region_name'), config.get('aws_creds', 'access_key'), config.get('aws_creds', 'secret_access_key'), config.get('aws_creds', 'session_token')
    
    if file_type in ['csv', 'xlsx']:
        # obtain bucket and key parameters from absolute s3 path
        bucket, key = url_parse(s3_path)

        # create data transferring session with AWS S3
        s3 = boto3.client('s3', region_name=region_name, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key, aws_session_token=session_token)

        # get object from AWS S3
        csv_object = s3.get_object(Bucket=bucket, Key=key)

        # read the body of object to extract csv file
        csv_string = csv_object['Body'].read().decode('utf-8')

        # convert csv file to pandas dataframe
        df = pd.read_csv(StringIO(csv_string))

        return df
    else:
        return None

def redshift_read(config, sql_query):
    # access the credentials
    username, password = config.get('rs_creds', 'username'), config.get('rs_creds', 'password')
    database, table, engine, port, host = config.get('db_creds', 'database'), config.get('db_creds', 'table'), config.get('db_creds', 'engine'), config.get('db_creds', 'port'), config.get('db_creds', 'host')

    # create data transferring session with AWS Redshift
    connection = f"{engine}://{username}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection, encoding='utf-8')

    # read data
    df = pd.read_sql(sql_query, engine)

    # kill engine
    engine.dispose()

    return df

     
file_type = 'csv'
print(file_type in ['csv', 'xlsx'])
