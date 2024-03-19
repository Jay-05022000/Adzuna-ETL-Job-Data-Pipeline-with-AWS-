#!/usr/bin/env python
# coding: utf-8

# This is a end to end data pipeline project in which data is fetched from adzuna api.

# In[1]:


# Importing necessary libraries.

import requests   # To make API calls.
import pandas as pd
import urllib.parse


# In[2]:


# Constructing variables.
# Request format = https://api.adzuna.com/v1/api/jobs/gb/search/1?app_id={YOUR_APP_ID}&app_key={YOUR_APP_KEY}

Application_ID = "0c9ff2ac"
Application_Keys = "28e625f9770d7cfcf94c07da7b5ca85d"
Base_URL = "https://api.adzuna.com/v1/api"
                         


# Fetch up to 100 recent job listings in the field of data engineering posted within the last 30 days in Canada.

# In[3]:


# Calling the API.

Data = requests.get('https://api.adzuna.com/v1/api/jobs/ca/search/1?app_id=0c9ff2ac&app_key=28e625f9770d7cfcf94c07da7b5ca85d&results_per_page=100&what=Data%20Engineering&what_or=Data%20Engineering&title_only=Data%20Engineering').json()
   


# In[4]:


Data


# Challenge Solution

# improvised_records = []
# 
# for record in Data['results']:
# 
#     job_title = record['title']
#     
#     if "," in job_title:
#         job_title = job_title.replace(",",":")
#         improvised_records.append(Data['results'].index(record))
#     else:
#         job_title = job_title
# print(improvised_records)   

# In[5]:


DE_Jobs = []

for record in Data['results']:

    job_title = record['title']
    
    if "," in job_title:
        job_title = job_title.replace("," , ":")
    
    else:
        job_title = job_title
             
    company_name = record['company']['display_name']  
    
    if "," in company_name:
        company_name = company_name.replace("," , " ")
    else:
        company_name = company_name
        
    job_description = record['description']   # Do not that it is a partial description.It is beacause of API's character limitation.
    date_posted = record['created']   
    location = record['location']['area']
    job_url = record['redirect_url']  
    
    Job_Info={'Job_Title':job_title,'Company':company_name,'Description':job_description,'Date_Posted':date_posted,'Location':location,'URL':job_url}
    
    DE_Jobs.append(Job_Info)


# In[6]:


DE_Jobs_df=pd.DataFrame(DE_Jobs)    # Create a dataframe.


# In[7]:


DE_Jobs_df


# In[8]:


DE_Jobs_df.info()


# # Transformation

# In[9]:


# Converting Date_Posted column into a datetime object.

DE_Jobs_df['Date_Posted'] = pd.to_datetime(DE_Jobs_df['Date_Posted'])


# In[10]:


# Sort dataframe by descending order of posting date (latest should come first).

DE_Jobs_df = DE_Jobs_df.sort_values(by=['Date_Posted'],ascending=False).reset_index(drop=True)   


# In[11]:


# Normalizing 'Location' column.

DE_Jobs_df['Country'] = DE_Jobs_df['Location'].apply(lambda x: x[0] if len(x) > 0 else None)
DE_Jobs_df['Province'] = DE_Jobs_df['Location'].apply(lambda x: x[1] if len(x) > 1 else "-")
DE_Jobs_df['City'] = DE_Jobs_df['Location'].apply(lambda x: x[-1] if len(x) > 1 else "-")


# In[12]:


# Re-arranging column orders.

DE_Jobs_df = DE_Jobs_df.iloc[:,[0,1,2,3,7,6,5,4,8]]
DE_Jobs_df


# Fetch up to 100 recent job listings in the field of data analytics posted within the last 30 days in Canada. 

# In[13]:


DA = requests.get('https://api.adzuna.com/v1/api/jobs/ca/search/1?app_id=0c9ff2ac&app_key=28e625f9770d7cfcf94c07da7b5ca85d&results_per_page=100&what=Data%20Analytics&what_or=Data%20Analytics&title_only=Data%20Analytics').json()
 


# In[14]:


DA['results'][5] 


# In[15]:


DA_Jobs=[]

for row in DA['results']:
    
    title = row['title']
    
    if "," in title:
        title = title.replace("," , ":")
    else:
        title = title
    
    try:                            # few job postings might don't have company name.
        company = row['company']['display_name']    
    
        if "," in company:
            company = company.replace("," , ":")
        else:
            company = company 
    except:
        company = "Not Provided"
     
    
    date = row['created']   
    description = row['description']
    area = row['location']['area']   
    url = row['redirect_url']    
    
    Jobs={'Job_Title':title,'Company':company,'Description':description,'Date_Posted':date,'Location':area,'URL':url}
    DA_Jobs.append(Jobs)


# In[16]:


len(DA_Jobs)


# In[17]:


DA_Jobs_df = pd.DataFrame(DA_Jobs)
DA_Jobs_df


# In[18]:


# Converting 'Date_Posted' column into timestamp odject.

DA_Jobs_df['Date_Posted'] = pd.to_datetime(DA_Jobs_df['Date_Posted'])


# In[19]:


# Sort the dataframe by descending order of posting date (latest should come first).

DA_Jobs_df = DA_Jobs_df.sort_values(by=['Date_Posted'],ascending=False).reset_index(drop=True) 


# In[20]:


# Normalizing 'Location' column.

DA_Jobs_df['Country'] = DA_Jobs_df['Location'].apply(lambda x: x[0] if len(x) > 0 else None)
DA_Jobs_df['Province'] = DA_Jobs_df['Location'].apply(lambda x: x[1] if len(x) > 1 else "-")
DA_Jobs_df['City'] = DA_Jobs_df['Location'].apply(lambda x: x[-1] if len(x) > 1 else "-")


# In[21]:


# Re-arranginf the columns of dataframe.

DA_Jobs_df = DA_Jobs_df.iloc[:,[0,1,2,3,8,7,6,5,4]]

