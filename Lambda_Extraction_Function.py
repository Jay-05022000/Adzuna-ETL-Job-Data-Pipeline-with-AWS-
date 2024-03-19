import json
import os
import requests 
import boto3
from datetime import datetime


def DE_Job_Data(data):
    
    print(data)
    Filename = "DE_Raw_" + str(datetime.now()) + ".json"
    connect = boto3.client('s3')
    connect.put_object(
        Bucket = "adzuna-etl-data-pipeline-project-jay-patel",
        Key = "Raw_Data/To_Processed/DE_Jobs_Raw_Data/ " + Filename,
        Body = json.dumps(data)
        )

    return f'{Filename} is successfully stored into s3 folder'
    

def DA_Job_Data(data):
    
    print(data)
    Filename = "DA_Raw_" + str(datetime.now()) + ".json"
    connect = boto3.client('s3')
    connect.put_object(
        Bucket = "adzuna-etl-data-pipeline-project-jay-patel",
        Key = "Raw_Data/To_Processed/DA_Jobs_Raw_Data/ " + Filename,
        Body = json.dumps(data)
        )
    
    return f'{Filename} is successfully stored into s3 folder'


def lambda_handler(event, context):
    
    App_ID = os.environ.get('Application_ID')   
    App_Key = os.environ.get('Application_Key')
   
    DE_url = f'https://api.adzuna.com/v1/api/jobs/ca/search/1?app_id={App_ID}&app_key={App_Key}&results_per_page=100&what=Data%20Engineering&what_or=Data%20Engineering&title_only=Data%20Engineering&max_days_old=30'
    DA_url = f'https://api.adzuna.com/v1/api/jobs/ca/search/1?app_id={App_ID}&app_key={App_Key}&results_per_page=100&what=Data%20Analytics&what_or=Data%20Analytics&title_only=Data%20Analytics&max_days_old=30'
    
 
    # Calling the API to get resptective Job data.
    
    DE_url_response = requests.get(DE_url).json()   # Storing response from the API.
    DA_url_response = requests.get(DA_url).json()
    
    DE_status = DE_Job_Data(DE_url_response)  # Function calling to store the fetched data into respective s3 folder.
    
    print(DE_status)   # Printing the status of storing operation.
    
    DA_status = DA_Job_Data(DA_url_response)
    
    print(DA_status)
    
     