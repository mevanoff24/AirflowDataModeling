# Background Information 
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

# Solution 
I have created a high grade data pipeline that is dynamic and built from reusable tasks. This can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse. Therefore I have built a data quality checker to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.



## How to run 
- Update the config file (`dl.cfg`) with your AWS credentails and Redshift cluster details
- Run `python create_tables.py` from this root directory to create the tables
- Run `python etl.py` to load the staging tables and insert data into tables


### Files
- `udac_example_dag.py` - python file to create tunable Airflow Dags. 

- The operators folder with operator templates
 - `data_quality.py` - python file to validate data quality
 - `load_dimension.py` - python file to load dimensional tables
 - `load_fact.py` - python file to load dimensional tables
 - `stage_redshift.py` - python file to stage data to RedShift cluster

- The `helpers` folder 
 - A helper class for the SQL transformations

- `create_tables.sql` - SQL queires 



## Dependencies
- `airflow`
- `datetime`
