                                     Adzuna ETL Job Data Pipeline with AWS
      	
       
       
![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=for-the-badge&logo=jupyter&logoColor=white)    ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)  ![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)

Aim: Establish a data pipeline using AWS Cloud Services and Python to retrieve details on data engineering and data analytics job opportunities in Canada through the Adzuna API.The designed pipeline can be configured to retrieve details of any job from any location globally.The project aims to provide users with timely and relevant job postings for personal job application needs and data analysis insights into various industries. 

Tools and Technologies Used: 

1.	Python 
2.	Jupyter Notebook: For local development.
3.	AWS Lambda: To develop and deploy Data Extraction and Data Transformation scripts into the cloud.
4.	AWS s3: Simple storage service (Data Lake) to store raw Json as well as transformed CSV files. 
5.	AWS Glue: To create crawlers and develop ETL job for incremental loading into the Redshift.
6.	AWS Redshift: A Data Warehouse service used to store transformed data centrally and perform data analysis using SQL.


Process : The comprehensive operational flow is outlined as follows.

![image](https://github.com/Jay-05022000/Adzuna-ETL-Job-Data-Pipeline-with-AWS-/assets/110780565/49f5680f-803c-47bf-adc2-f60040ea0830)


Extension of project via Incremental Data Loading:

To enhance the project, an incremental data loading process has been implemented using AWS Glue ETL service to load data into Amazon Redshift. This extension ensures that only new or updated job postings are processed and loaded, optimizing the overall ETL process and reducing redundant data processing.

Use cases: 

1. Job Seeker Analytics Dashboard: 
Leveraging the Adzuna End-To-End ETL Data-Pipeline Project, users gain insights into job opportunities worldwide. The pipeline can be configured to retrieve details of any job from any location globally, providing users with timely and relevant job postings to enhance their job search strategies and career paths.

2. Advanced Analytics with Redshift:
With the implementation of incremental data loading into Amazon Redshift, users can perform advanced analytics on a large volume of data. Redshift's powerful querying capabilities enable deeper insights and more comprehensive analysis of job market trends. This allows users to gain access to up-to-date and relevant job postings, identify trends and patterns, streamline the job search process, and stay competitive in the job market by aligning skills and qualifications with industry demand.

 
