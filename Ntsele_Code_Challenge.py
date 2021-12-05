#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  4 20:39:07 2021

@author: ntsele
"""

import os
import boto3
import pandas as pd
from botocore.exceptions import ClientError
#from boto3.session import Session
from botocore.exceptions import NoCredentialsError
#_____________________________________________Qustion 1. Data Extract__________________________________________________________________\-

"""
    - First thing we need to access the bucket using access key and secret key. 
      It not a good practice to   write the key in the python script therefore I’m going to store the keys in environment variables using ”aws configure”.
     
    - After running my script I got this error "ClientError: An error occurred (AccessDenied) when calling the SelectObjectContent operation: Access Denied".
      This error suggests that I don’t access to the bucket meaning the bucket is not publicly accessible.
     
    - I couldn’t tell exactly how long the script ran since I couldn’t access the bucket.

     
     """
     
#getting data from s3 bucket    
client = boto3.client('s3')
resp = client.select_object_content(
    Bucket='cct-ds-code-challenge-input-data',
    Key='city-hex-polygons-8.geojson.json',
    ExpressionType='SQL',
    Expression='select h.\"index"\ from s3object s where h.\"features_geometry_type"\ ="Polygon"',
    InputSerialization={'JSON': {'FileHeaderInfo': 'USE'},'JSON': {'Type': 'DOCUMENT'}},
    OutputSerialization={'CSV': {}},
)

#upack query response
coordinates = []
for event in resp['Payload']:
    if 'coordinates' in event:
        try:
            
            coordinates.append(event['coordinates']['Payload'])
            print("Data Extracted")
            
        except FileNotFoundError:
                print("File Not Found")
        except NoCredentialsError:
                print("Credentials not available")
        
        coordinates.to_csv('Ntsele_city-hex-polygons-8.csv')
        #print(coordinates)
        
        
#_____________________________________________Question 2. Data Transformation________________________________________________________________
"""
    - Since I couldn’t create the file I am going to use the files provided foe validation, however I will include my code on how I was going to do the task.
    
    - For performance please refer to summary note at the end this file
"""
sr = pd.read_csv('/Users/ntsele/Documents/Data_Engineer_COCT/ds_code_challenge/Ntsele_sr_hex.csv')
city = pd.read_csv('/Users/ntsele/Downloads/city-hex-polygons-8.geojson.json')


# replacing na values in college with No college
sr["Latitude"].fillna(0, inplace = True)
sr["Longitude"].fillna(0, inplace = True)

try:
    
    output=sr.append(city)
    print("File merged successfully")
except FileNotFoundError:
                print("File Not Found")
except NoCredentialsError:
                print("Credentials not available")
    
output.to_csv("Ntsele_sr_hex_own.csv")
#print(sr)




#_____________________________________________Question 5. Anonymises file_________________________________________________________________
   
"""
    - I don’t think I fully understand the question here, however what I’ve done I masked the “NotificationNumber” 
        because I believe notification number holds important personal information such as cellphone number, Name, Surname and home address
        of the person who open service request therefore it need to be masked not accessible to everyone, together with a OfficialSuburbs of a client. 

"""
def anony(df,cols):
    for col_name in cols:
        keys = {cats: i for i,cats in enumerate(df[col_name].unique())}
        df[col_name] =df[col_name].apply(lambda x: keys[x])
        
    return df
df=pd.read_csv('/Users/ntsele/Documents/Data_Engineer_COCT/ds_code_challenge/Ntselesr_hex_own.csv',index_col=[0])
cols=['NotificationNumber','OfficialSuburbs']
df = anony(df,cols)
df.to_csv("Ntsele_sr_hex_any.csv",index=False)

#_____________________________________________Question 6. Upload file to S3___________________________________________________________________
"""
   Agian I get access denied error meaning the the busket is not publicly accessable  
   
   S3UploadFailedError: Failed to upload Ntsele_T_DE_Challenge.csv to cct-ds-code-challenge-input-data/Ntsele_T_DE_Challenge.csv:
   An error occurred (AccessDenied) when calling the CreateMultipartUpload operation: Access Denied
   """
             
def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3')

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    
#This will upload Ntsele_sr_hex_any.csv to s3 and rename it to Ntsele_T_DE_Challenge.csv
uploaded = upload_to_aws('Ntsele_sr_hex_any.csv', 'cct-ds-code-challenge-input-data', 'Ntsele_T_DE_Challenge.csv')

#S3UploadFailedError: Failed to upload Ntsele_T_DE_Challenge.csv to cct-ds-code-challenge-input-data/Ntsele_T_DE_Challenge.csv: An error occurred (AccessDenied) when calling the CreateMultipartUpload operation: Access Denied
           

#_________________________________________________Summary Notes_____________________________________________________________________________________
"""
-	After running through the challenge, I’ve notice the scripts takes some to time to run, in my observation I’ve noticed the files sizes are close to 1GB with millions of records therefore it takes time to process.
-	The best way to improve performance is to use Spark. Why Spark ? spark is used to process millions of records or big files, spark allows parallel processing.
-	In this challenge I’ve convert csv files parquet file this breaks 9MB files into small blocks of 117MB file to allow parallel processing as a results it improves performance.(pyspark code available on request)

"""
