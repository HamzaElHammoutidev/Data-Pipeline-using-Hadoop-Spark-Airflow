
# Data Pipeline using HDFS, Spark And Airflow   
Using data engineering techniques to create a data pipeline,starting by fetching NYC taxi driver data from a dataset, cleansing data, modeling and fitting into well organized data model to data partitioned by pickup hours because we want to update our pipeline every hour and lastly exported to parquet  
# Building Steps:     
- Loading Data from Hadoop Cluster     
 - Data Exploration     
 - Use Case Objective     
 - Data Processing and Cleansing    
 - Data Modeling     
 - Data Fitting into Model     
 - Data Loading     
 - Data Quality Checks     
    
# Steps :     
   
- [x] Connecting to Hadoop using Spark and Loading Data from HDFS  
 - [x] Data Sampling to 100 random rows for Data Exploration   
 - [x] Defining Data Pipeline End Use Case : Detailed Trip Information  
 - [x] Deduplication, Missing Values   
 - [x] Selecting Only Needed Columns for our Model  
 - [x] Loading Data from Spark to our Model and write it into parquet format  
 - [x] Data Quality Checks
 - [ ] Orchestrate Data pipeline with Airflow