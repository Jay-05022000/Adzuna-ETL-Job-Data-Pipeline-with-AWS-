import json
import boto3
import pandas as pd
from datetime import datetime 
from io import StringIO

# Define a function to extract job information from JSON files.

def Job_Info(file):
     DE_Jobs = []
     
     for record in file['results']:
          
          job_title = record['title']
          if "," in job_title:
               job_title = job_title.replace("," , ":")  # Replace comma with colon to avoid issues in Athena table.
          else:
               job_title = job_title
          
          try:
               company_name = record['company']['display_name']   
               if "," in company_name:
                    company_name = company_name.replace("," ," ")  # Replace comma with space to avoid issues in Athena table.
               else:
                    company_name = company_name
          except:
               company = "Not Provided"
          
          date_posted = record['created']   
          location = record['location']['area']
          job_url = record['redirect_url']  
          
          Job_Info={'Job_Title':job_title,'Company':company_name,'Date_Posted':date_posted,'Location':location,'URL':job_url}
          
          DE_Jobs.append(Job_Info)
          
          DF = pd.DataFrame(DE_Jobs)
         
          DF['Date_Posted'] = pd.to_datetime(DF['Date_Posted'])
          
          DF = DF.sort_values(by=['Date_Posted'],ascending=False).reset_index(drop=True)   
          
          DF['Country'] = DF['Location'].apply(lambda x: x[0] if len(x) > 0 else None)    # Normalize 'Location' column.
          
          DF['Province'] = DF['Location'].apply(lambda x: x[1] if len(x) > 1 else "-")
          
          DF['City'] = DF['Location'].apply(lambda x: x[-1] if len(x) > 1 else "-")
          
          DF = DF.iloc[:,[0,1,2,7,6,5,4,3]]  # Re-arrange column orders.
     
     return DF      

# Define a function to handle data engineering job data.

def DE_handler():
     
     s3_object = boto3.client('s3')   
     Bucket = "adzuna-etl-data-pipeline-project-jay-patel"
     Key = "Raw_Data/To_Processed/DE_Jobs_Raw_Data/"
     
     Files = s3_object.list_objects(Bucket = Bucket,Prefix = Key)    # Get the list of all available files in the defined s3 folder.
     
     DE_Job_Files = []
     DE_Job_File_Keys = []
     
     for File in Files['Contents']:
          File_Key = File['Key']
          
          if File_Key.split('.')[-1] == "json":    # Make sure to access only json files.
               File_Data = s3_object.get_object(Bucket = Bucket , Key = File_Key)  # Get file meta data.
               File_Content = File_Data['Body']                                    # Store the actual content of json file.
               json_object = json.loads(File_Content.read())                       # Read json data.
               DE_Job_Files.append(json_object)                                    # store data into list.
               DE_Job_File_Keys.append(File_Key)                                   # Store key of accessed file into the list.
     
     for file in DE_Job_Files:
          
          DE_Jobs_df = Job_Info(file)
          
          file_key = "Processed_Data/DE_Job_Postings/ " + "Transformed" + str(datetime.now()) + ".csv"   # Destination  of desired s3 folder to store file.
          file_buffer = StringIO()                 
          DE_Jobs_df.to_csv(file_buffer,index=False)                                                     # Convert dataframe into csv file.
          DE_Job_Data  = file_buffer.getvalue()
          
          s3_object.put_object(Bucket = "adzuna-etl-data-pipeline-project-jay-patel", Key = file_key, Body = DE_Job_Data)
          s3 = boto3.client('s3')
          path = 'Raw_Data/Already_Processed/DE_Jobs_Already_Processed/'
          
          for key in  DE_Job_File_Keys:
               copy_source = {'Bucket': 'adzuna-etl-data-pipeline-project-jay-patel','Key': key}          # Source location of s3 from where file will be copied.
               s3.copy_object(CopySource = copy_source,Bucket = Bucket, Key = path + key.split("/")[-1])  # Copy the file to destination folder.
               s3.delete_object(Bucket = Bucket,Key = key)                                                # Delete the file from source folder.
     
     return "DE operation is successfully done"     

# Define a function to handle data analyst job data.
def DA_handler():
     
     s3_object = boto3.client('s3')   
     Bucket = "adzuna-etl-data-pipeline-project-jay-patel"
     Key = "Raw_Data/To_Processed/DA_Jobs_Raw_Data/"
     
     Files = s3_object.list_objects(Bucket = Bucket,Prefix = Key)    
     
     DA_Job_Files = []
     DA_Job_File_Keys = []
     
     for File in Files['Contents']:
          File_Key = File['Key']
          
          if File_Key.split('.')[-1] == "json":
               File_Data = s3_object.get_object(Bucket = Bucket , Key = File_Key)
               File_Content = File_Data['Body']
               json_object = json.loads(File_Content.read())
               DA_Job_Files.append(json_object)
               DA_Job_File_Keys.append(File_Key)     
     
     for file in DA_Job_Files:
          
          DA_Jobs_df = Job_Info(file)
          file_key = "Processed_Data/DA_Job_Postings/ " + "Transformed" + str(datetime.now()) + ".csv"
          
          file_buffer = StringIO()
          DA_Jobs_df.to_csv(file_buffer,index=False)
          DA_Job_Data  = file_buffer.getvalue()
          
          s3_object.put_object(Bucket = "adzuna-etl-data-pipeline-project-jay-patel", Key = file_key, Body = DA_Job_Data)
          s3 = boto3.client('s3')
          path = 'Raw_Data/Already_Processed/DA_Jobs_Already_Processed/'
          for key in  DA_Job_File_Keys:
              copy_source = {'Bucket': 'adzuna-etl-data-pipeline-project-jay-patel','Key': key}    # Define s3 source destination from where file will be copied. 
              s3.copy_object(CopySource = copy_source,Bucket = Bucket, Key = path + key.split("/")[-1])  # Copy the file to destination folder.
              s3.delete_object(Bucket = Bucket,Key = key)  # Delete the file from source folder.
    
     return "DA operation is successfully done"     

# Define the main lambda handler function.

def lambda_handler(event, context):
    DE = DE_handler()
    DA = DA_handler()
    print('Operation is done successfully')

    
    
               
          
          
          
         