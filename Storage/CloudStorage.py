import os
import boto3
import pandas as pd
from io import StringIO
from botocore.exceptions import ClientError
import json
from datetime import datetime

class CloudStorageProvider:
    
    class AWS:
        def __init__(self):
            self.s3_client = boto3.client('s3')
            self.s3_resource = boto3.resource('s3')
    
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
        
        def get_dataframe_from_specific_datetime(self, bucket_name, prefix, year=None, month=None, day=None, hour=None, minute=None):
            try:
                # Construct the folder path based on the specified datetime components
                folder_path = prefix + ""
                if year is not None:
                    folder_path += f"{year}/"
                if month is not None:
                    folder_path += f"{month:02}/"
                if day is not None:
                    folder_path += f"{day:02}/"
                if hour is not None:
                    folder_path += f"{hour:02}/"
                if minute is not None:
                    folder_path += f"{minute:02}/"
                
                # List objects in the specified folder
                response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
                dataframes = []
                
                # Retrieve data from each object within the folder
                for obj in response.get('Contents', []):
                    key = obj['Key']
                    
                    # Check if the object is a CSV file
                    if key.endswith('.csv'):
                        # Download CSV file and convert to DataFrame
                        csv_obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
                        dataframe = pd.read_csv(csv_obj['Body'])
                        dataframes.append(dataframe)
                
                # Concatenate all DataFrames into a single DataFrame
                if dataframes:
                    result = pd.concat(dataframes, ignore_index=True)
                    return result
                else:
                    print("No data found in the specified datetime.")
                    return None
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
            folder_path = f"{prefix_path}/"
            key = folder_path + f"{file_name}.csv"
            self.s3_client.put_object(Bucket=bucket_name, Key=key, ACL='public-read', 
                                      Body=csv_buffer.getvalue(), ContentType='text/csv')
            
            # Set the bucket ACL to allow public read access
            self.s3_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
            
            print(f"Data uploaded to S3 bucket '{bucket_name}' under folder '{key}'")
            
        def upload_dataframe_with_timestamp(self, dataframe, bucket_name, prefix_path):
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
                folder_path = f"{prefix_path}/{book.lower()}/{datetime_obj.year}/{datetime_obj.month:02}/{datetime_obj.day:02}/"
                
                # Convert the DataFrame to CSV format
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
                key = folder_path + f"{datetime_obj.year:02}{datetime_obj.month:02}{datetime_obj.day:02}-{book.lower()}.csv"
                self.s3_client.put_object(Bucket=bucket_name, Key=key, ACL='public-read', 
                                          Body=csv_buffer.getvalue(), ContentType='text/csv')

                # Set the bucket ACL to allow public read access
                self.s3_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

                print(f"Data uploaded to S3 bucket '{bucket_name}' under folder '{key}'")
            
        def upload_dataframe_with_datetime_subfolders(self, dataframe, bucket_name, prefix_path):
            # Iterate through each row of the DataFrame
            for index, row in dataframe.iterrows():
                # Extract datetime from the 'datetime' column
                datetime_str = row['datetime']
                id_str = row['id']
                # Convert datetime string to datetime object
                datetime_obj = pd.to_datetime(datetime_str)
                
                # Create folder structure based on the datetime
                folder_path = f"{prefix_path}/{datetime_obj.year}/{datetime_obj.month:02}/{datetime_obj.day:02}/{datetime_obj.hour:02}/{datetime_obj.minute:02}/{datetime_obj.second:02}/"
                
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
                key = folder_path + f"{id_str}.csv"
                self.s3_client.put_object(Bucket=bucket_name, Key=key, ACL='public-read', 
                                          Body=csv_buffer.getvalue(), ContentType='text/csv')
                
                # Set the bucket ACL to allow public read access
                self.s3_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
                
                print(f"Data for row {index} with id '{id_str}' uploaded to S3 bucket '{bucket_name}' under folder '{key}'")

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