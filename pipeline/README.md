# Pipeline

## Description

An S3 pipeline and a Kafka consume script based in Python that takes Museum data. Both ETL pipelines collect and process data into an Amazon Relational Database (RDS).

### S3 - pipeline: Reading data from an Amazon S3 bucket, processing and uploading it to the RDS of choice.

'extract.py' script:

1. Connect to an S3 bucket using a client.
2. Retrieve relevant objects and download them.
3. Download S3 objects ('.csv' and '.json files').
4. Combine multiple '.csv' files.

'pipeline.py' script

1. Connect to a local/AWS RDS.
2. Carry out extract actions - (load and clean data).
3. Format data and upload it to connected database.

### Kafka Cluster - pipeline: Reading data from a Kafka cluster, processing and uploading it to the RDS of choice.

1. Create a consumer that takes data from a Kafka topic.
2. Clean and process the data.
3. Format data and upload it to connected database.

## S3 - pipeline: Installation/Setup Instructions

*Python3* dependant script

1. Create and activate a new virtual environment.
2. Run 'pip3 install -r requirements.txt' to install dependencies.
3. Create a '.env' file to store the necessary keys to connect to AWS RDS and S3 Bucket.
    For the S3 Bucket:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - MUSEUM_BUCKET

    For the AWS RDS:
    - 'DATABASE_NAME'
    - 'DATABASE_USERNAME'
    - 'DATABASE_PASSWORD'
    - 'DATABASE_IP'
    - 'DATABASE_PORT'

    For a local RDS:
    - 'LOCAL_DB'
    - 'LOCAL_HOST'
    - 'LOCAL_PORT'

4. Create a 'terraform.tfvars' file - check terraform readme for more information.

## Kafka Cluster - pipeline: Installation/Setup Instructions

1. Create and activate a new virtual environment.
2. Run 'pip3 install -r requirement.txt' to install dependencies.
3. Create a '.env' file to store the necessary keys to connect to AWS RDS and Kafka Cluster.
    For the AWS RDS connection:
    - 'DATABASE_NAME'
    - 'DATABASE_USERNAME'
    - 'DATABASE_PASSWORD'
    - 'DATABASE_IP'
    - 'DATABASE_PORT'

    For the Kafka Cluster Consumer Client:
    - 'BOOTSTRAP_SERVERS'
    - 'SECURITY_PROTOCOL'
    - 'SASL_MECHANISM'
    - 'USERNAME'
    - 'PASSWORD'
    - 'TOPIC'
    - 'GROUP'
    
4. Create a 'terraform.tfvars' file - check terraform readme for more information.
5. Run the 'consume.py' script to obtain constant data from the Kafka Cluster.

### S3 - Pipeline: Functions

| Function name                | Description                                                              |
| ---------------------------- | ------------------------------------------------------------------------ |
| get_bucket_names             | Returns a list of available bucket names.                                |
| get_bucket_objects           | Return a list of available objects in a bucket.                          |
| download_all_files           | Downloads all files from a bucket to a named folder.                     |
| download_specific_files      | Downloads specific files from a bucket to a named folder.                |
| merge_csv_to_file            | Merge multiple csvs downloaded into one combined csv.                    |
| delete_csv_files             | Remove csvs files that were combined.                                    |
| get_db_connection            | Gets a connection to the specified AWS database.                         |
| get_local_db_connection      | Returns a connection to the database; all rows are returned as dicts.    |
| get_cursor                   | Gets a cursor to browse database.                                        |
| load_kiosk_data              | Loads the merged csv file generated by kiosks.                           |
| get_rating_instances         | Get a list of rating instances.                                          |
| format_rating_instances      | Format the rating instances to return sql-friendly data.                 |
| upload_rating_instances      | Upload the formatted rating instances to a database,given n rows.        |
| get_support_instances        | Get a list of support instances.                                         |
| format_support_instances     | Format the support instances to return sql-friendly data.                |
| upload_support_instances     | Upload the formatted support instances to a database, given n rows.      |
| main                         | Run the pipeline using the associated functions                          |

### Kafka Cluster - Cleaning: Functions

| Function name                | Description                                                              |
| ---------------------------- | ------------------------------------------------------------------------ |
| clean_data                   | Cleans the Kiosk data from the museum.                                   |
| invalid_values               | Check for invalid values in message.                                     |
| missing_values               | Check for missing values in message.                                     |


### Kafka Cluster - Pipeline: Functions

| Function name                | Description                                                              |
| ---------------------------- | ------------------------------------------------------------------------ |
| get_db_connection            | Gets a connection to the specified AWS database.                         |
| get_cursor                   | Gets a cursor to browse database.                                        |
| format_support_instance      | Obtain each key value for support instance to be inputted in RDS.        |
| upload_support_instance      | Upload support instance cleaned data to an AWS RDS.                      |
| format_rating_instance       | Obtain each key value for rating instance to be inputted in RDS.         |
| upload_rating_instance       | Upload rating instance cleaned data to an AWS RDS.                       |
| select_data_upload           | Select upload function to upload message to AWS RDS.                     |
| consume_messages             | Intake messages from a Kafka cluster.                                    |
| main                         | Run the pipeline using the associated functions                          |


### S3 - Pipeline: Command Line Arguments

| Argument                    | Definition                                                                                                      |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------|
| --bucket, -b                | Optional positional argument for the AWS bucket you are accessing. Default will access environ bucket variable. |
| --num_rows, -nr             | Optional positional argument for the number of instance rows to be uploaded to database. Default will be None.  |
| --log, -l                   | Optional positional argument for the boolean argument to set log to output in console or file. Default is False.|