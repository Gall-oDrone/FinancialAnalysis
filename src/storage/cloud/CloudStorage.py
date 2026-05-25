import os
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
from io import StringIO
from botocore.exceptions import ClientError, NoCredentialsError
import json
from datetime import datetime

from storage.postgres.news_dataframe import normalize_financial_news_datetime_column


def _partition_value_provided(value) -> bool:
    """True if a Hive-style path segment should be appended (skip None and '')."""
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def _format_partition_component(part_name: str, value) -> str:
    if part_name == "year":
        return str(value).strip()
    text = str(value).strip()
    if text.isdigit():
        return f"{int(text):02d}"
    return text


def build_s3_datetime_partition_prefix(
    prefix: str,
    *,
    year=None,
    month=None,
    day=None,
    hour=None,
    minute=None,
    second=None,
) -> str:
    """Build S3 prefix matching upload layout (year=/month=/day=/hour=/minute=/second=/)."""
    base = prefix if prefix.endswith("/") else f"{prefix}/"
    segments = []
    for part_name, val in (
        ("year", year),
        ("month", month),
        ("day", day),
        ("hour", hour),
        ("minute", minute),
        ("second", second),
    ):
        if not _partition_value_provided(val):
            continue
        segments.append(f"{part_name}={_format_partition_component(part_name, val)}")
    if not segments:
        return base
    return base + "/".join(segments) + "/"


def _load_aws_env() -> None:
    """Load AWS (and other) vars from repo .env in Docker or local runs."""
    for path in (Path("/app/.env"), Path.cwd() / ".env", Path(__file__).resolve().parents[3] / ".env"):
        if path.is_file():
            load_dotenv(path, override=False)


def _boto3_session():
    _load_aws_env()
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    key = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    if key and secret:
        return boto3.Session(
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            region_name=region,
        )
    return boto3.Session(region_name=region)


def _ensure_aws_credentials() -> None:
    _load_aws_env()
    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        return
    raise NoCredentialsError(
        provider="env",
    )


class CloudStorageProvider:
    
    class AWS:
        def __init__(self):
            _ensure_aws_credentials()
            session = _boto3_session()
            self.s3_client = session.client("s3")
            self.s3_resource = session.resource("s3")
    
        def create_bucket(self, bucket_name):
            try:
                self.s3_resource.create_bucket(Bucket=bucket_name, ObjectOwnership='ObjectWriter')
                self.s3_client.put_public_access_block(Bucket=bucket_name, 
                                                       PublicAccessBlockConfiguration={
                                                           'BlockPublicAcls': False,
                                                           'IgnorePublicAcls': False,
                                                           'BlockPublicPolicy': False,
                                                           'RestrictPublicBuckets': False
                                                       })
                self.s3_client.put_bucket_acl(ACL='public-read-write', Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' created successfully.")
            except ClientError as e:
                if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                    print(f"Bucket '{bucket_name}' already exists.")
                else:
                    print("Error:", e)
        
        def get_csv_from_specific_folder(self, bucket_name, folder_path):
            try:
                # List objects in the specified folder
                response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
                
                # Retrieve CSV file from the folder
                for obj in response.get('Contents', []):
                    key = obj['Key']
                    
                    # Check if the object is a CSV file
                    if key.endswith('.csv'):
                        # Download CSV file
                        csv_obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
                        csv_content = csv_obj['Body'].read().decode('utf-8')
                        csv_data = StringIO(csv_content)
                        
                        # Convert CSV content to DataFrame
                        dataframe = pd.read_csv(csv_data)
                        return dataframe
                
                print("No CSV file found in the specified folder.")
                return None
            except ClientError as e:
                print("Error:", e)
                return None
        
        def _list_csv_keys_under_prefix(self, bucket_name: str, prefix: str) -> list[str]:
            keys: list[str] = []
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".csv"):
                        keys.append(key)
            return keys

        def get_dataframe_from_specific_datetime(
            self,
            bucket_name,
            prefix,
            year=None,
            month=None,
            day=None,
            hour=None,
            minute=None,
            second=None,
        ):
            try:
                folder_path = build_s3_datetime_partition_prefix(
                    prefix,
                    year=year,
                    month=month,
                    day=day,
                    hour=hour,
                    minute=minute,
                    second=second,
                )
                print(f"Listing s3://{bucket_name}/{folder_path}")

                keys = self._list_csv_keys_under_prefix(bucket_name, folder_path)
                print(f"Found {len(keys)} CSV file(s) under prefix")

                if not keys:
                    print(
                        "No data found. Expected keys like: "
                        f"{folder_path}hour=HH/minute=MM/second=SS/format=csv/<id>.csv"
                    )
                    return None

                dataframes = []
                for index, key in enumerate(keys, start=1):
                    print(f"Downloading [{index}/{len(keys)}]: {key}")
                    csv_obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
                    dataframes.append(pd.read_csv(csv_obj["Body"]))

                return pd.concat(dataframes, ignore_index=True)
            except ClientError as e:
                print("Error:", e)
                return None
        
        def upload_file(self, bucket_name, local_file_path, s3_file_path):
            try:
                self.s3_client.upload_file(local_file_path, bucket_name, s3_file_path)
                print(f"File '{local_file_path}' uploaded to '{s3_file_path}' in bucket '{bucket_name}' successfully.")
            except ClientError as e:
                print("Error:", e)

        def upload_dataframe_to_csv(self, dataframe, bucket_name, file_name, prefix_path):
            # Convert the DataFrame to CSV format
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=False)
            
            # Check if the bucket exists
            print(f"Cheking if {bucket_name} bucket already exists...", end="", flush=True)
            try:
                self.s3_client.head_bucket(Bucket=bucket_name)
                print(f"{bucket_name} bucket already exists")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    # If the bucket doesn't exist, create it
                    print(f"creating {bucket_name} bucket")
                    self.create_bucket(bucket_name)
                else:
                    print("Error:", e)
                    return
            
            # Upload the CSV data to the specified S3 bucket
            folder_path = f"{prefix_path}/format=csv/"
            key = folder_path + f"{file_name}.csv"
            self.s3_client.put_object(Bucket=bucket_name, Key=key, ACL='public-read', 
                                      Body=csv_buffer.getvalue(), ContentType='text/csv')
            
            # Set the bucket ACL to allow public read access
            self.s3_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
            
            print(f"Data uploaded to S3 bucket '{bucket_name}' under folder '{key}'")
            
        def upload_dataframe_with_timestamp(self, dataframe, bucket_name, prefix_path, file_format):
            # Get current timestamp
            now = datetime.now()
            for index, row in dataframe.iterrows():
                # Extract datetime from the 'datetime' column
                datetime_str = row['date']
                
                # Convert datetime string to datetime object
                datetime_obj = pd.to_datetime(datetime_str)
                
                book = row['book']
                # Create folder structure based on the current timestamp
                # Sets folder structure to Year/Month/day
                folder_path = f"{prefix_path}/book={book.lower()}/year={datetime_obj.year}/month={datetime_obj.month:02}/day={datetime_obj.day:02}/format={file_format}/"
                
                # Check if the bucket exists
                try:
                    self.s3_client.head_bucket(Bucket=bucket_name)
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # If the bucket doesn't exist, create it
                        self.create_bucket(bucket_name)
                    else:
                        print("Error:", e)
                        return
                
                key=""
                if file_format == "csv":
                    # Convert the DataFrame to CSV format
                    csv_buffer = StringIO()
                    pd.DataFrame(row).T.to_csv(csv_buffer, index=False)

                    # Upload the CSV data to the specified S3 bucket under the folder structure
                    key = folder_path + f"{datetime_obj.year:02}{datetime_obj.month:02}{datetime_obj.day:02}-{book.lower()}.csv"
                    self.s3_client.put_object(Bucket=bucket_name, Key=key, ACL='public-read', 
                                              Body=csv_buffer.getvalue(), ContentType='text/csv')

                # Set the bucket ACL to allow public read access
                self.s3_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

                print(f"Data uploaded to S3 bucket '{bucket_name}' under folder '{key}'")
            print(f"Task finished: all files were succesfully uploaded to S3 bucket {bucket_name}")
            
        def upload_dataframe_with_datetime_subfolders(self, dataframe, bucket_name, prefix_path, file_format):
            dataframe = normalize_financial_news_datetime_column(dataframe)
            # Iterate through each row of the DataFrame
            for index, row in dataframe.iterrows():
                # Extract datetime from the 'datetime' column
                datetime_str = row['datetime']
                id_str = row['id']
                # Convert datetime string to datetime object
                datetime_obj = pd.to_datetime(datetime_str)
                
                # Create folder structure based on the datetime
                folder_path = f"{prefix_path}/year={datetime_obj.year}/month={datetime_obj.month:02}/day={datetime_obj.day:02}/hour={datetime_obj.hour:02}/minute={datetime_obj.minute:02}/second={datetime_obj.second:02}/format={file_format}/"
                
                # Convert the current row of DataFrame to CSV format
                csv_buffer = StringIO()
                pd.DataFrame(row).T.to_csv(csv_buffer, index=False)
                
                # Check if the bucket exists
                try:
                    self.s3_client.head_bucket(Bucket=bucket_name)
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # If the bucket doesn't exist, create it
                        self.create_bucket(bucket_name)
                    else:
                        print("Error:", e)
                        return
                
                # Upload the CSV data to the specified S3 bucket under the folder structure
                key = ""
                if file_format == "csv":
                    key = folder_path + f"{id_str}.csv"
                    self.s3_client.put_object(Bucket=bucket_name, Key=key, ACL='public-read', 
                                              Body=csv_buffer.getvalue(), ContentType='text/csv')
                # Set the bucket ACL to allow public read access
                self.s3_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
                
                print(f"Data for row {index} with id '{id_str}' uploaded to S3 bucket '{bucket_name}' under folder '{key}'")
            print(f"Task finished: all files were succesfully uploaded to S3 bucket {bucket_name}")

        def delete_bucket(self, bucket_name):
            try:
                # Delete all objects within the bucket first
                self.delete_all_objects_in_bucket(bucket_name)
                
                # Delete the bucket
                self.s3_resource.Bucket(bucket_name).delete()
                print(f"Bucket '{bucket_name}' deleted successfully.")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchBucket':
                    print(f"Bucket '{bucket_name}' does not exist.")
                else:
                    print("Error:", e)

        def delete_all_objects_in_bucket(self, bucket_name):
            try:
                # List all objects in the bucket
                objects = self.s3_client.list_objects(Bucket=bucket_name)
                
                # Check if objects exist
                if 'Contents' in objects:
                    # Delete each object
                    for obj in objects['Contents']:
                        self.s3_resource.Object(bucket_name, obj['Key']).delete()
                
                print(f"All objects deleted from bucket '{bucket_name}'.")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchBucket':
                    print(f"Bucket '{bucket_name}' does not exist.")
                else:
                    print("Error:", e)        

''' 
def create_bucket(bucket_name):
    s3 = boto3.client('s3')
    
    # Check if the bucket name is available
    try:
        response = s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # Create the bucket if it doesn't exist
            try:
                s3 = boto3.resource('s3')
                s3.create_bucket(Bucket=bucket_name,ObjectOwnership='ObjectWriter')
                s3.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration={'BlockPublicAcls': False,'IgnorePublicAcls': False,'BlockPublicPolicy': False,'RestrictPublicBuckets': False})
                s3.put_bucket_acl(ACL='public-read-write',Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' created successfully.")
            except ClientError as e:
                print("Error:", e)
        else:
            print("Error:", e)

def upload_file_to_s3(bucket_name, local_file_path, s3_file_path):
    s3 = boto3.client('s3')

    try:
        s3.upload_file(local_file_path, bucket_name, s3_file_path)
        print(f"File '{local_file_path}' uploaded to '{s3_file_path}' in bucket '{bucket_name}' successfully.")
    except ClientError as e:
        print("Error:", e)

        
def upload_dataframe_to_s3(dataframe, bucket_name, file_name):
    # Convert the DataFrame to CSV format
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    
    # Connect to Amazon S3
    s3_client = boto3.client('s3')
                                                                                  
    # Check if the bucket exists
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        # If the bucket doesn't exist, create it
        create_bucket(bucket_name)
    
    # Upload the CSV data to the specified S3 bucket
    s3_client.put_object(Bucket=bucket_name, Key=file_name, ACL='public-read', Body=csv_buffer.getvalue(), ContentType='text/csv')
    
    # Set the bucket ACL to allow public read access
    s3_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
    
    print(f"Data uploaded to S3 bucket '{bucket_name}' as '{file_name}'")
'''