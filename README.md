# Data Pipeline with Airflow and AWS 

Building Automated Data Pipeline using: 
 - S3 as Data Storage 
 - Redshift as Data Warehouse 
 - Airflow as Automation Workflow 
 - SQL Star-Schema
 
## Custom Build Operators 
 - *CreateTablesOperator* : Create Data Warehouse tables based on a script file 
 - *StageS3ToRedshiftOperator*: Load Data from S3 files into Redshift Staging Tables 
 - *LoadFactOperator* : Data Processing, Normalization and Loading into Redshift Star-Schema Fact Table using SQL Only
 - *LoadDimensionOperator* : Data Processing, Normalization and Loading into Redshift Star-Schema Dimensions Tables using SQL Only
 - *DataQualityOperator* : Data Quality Checks of Ingested data into Redshift Data Warehouse

## Data Quality Checks 
Examples of Data Quality Requirements
- Data must be a certain size
- Data must be accurate to some margin of error
- Data must arrive within a given timeframe from the start of (SLA)
- Pipelines must run on a schedule
- Data must not contain any sensitive information



## Data Lineage
![Data Lineage](https://github.com/HamzaElHammoutidev/Data-Pipeline-With-Airflow/blob/master/img/datalineage_diagram.png)



