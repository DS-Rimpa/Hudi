# Hudi
## Overview:
Hudi is an open-source storage management framework that provides incremental data processing primitives for data lakes.

With Apache Hudi, you can perform record-level inserts, updates, and deletes on Amazon S3, consume real-time streams and change data captures, reinstate late-arriving data, and track history and rollbacks.

It includes built-in integrations with Apache Spark, Apache Hive, and Apache Presto, which enables you to query Apache Hudi datasets using the same tools with near-real-time access to data.

2. Storage Types:
An Apache Hudi dataset can be one of the following table types:

Copy On Write  (COW):

Data is stored in a columnar format (Parquet), and each update creates a new version of files during a write. CoW is the default storage type.
Merge On Read (MOR):
Data is stored using a combination of columnar (Parquet) and row-based (Avro) formats. Updates are logged to row-based delta files and are compacted as needed to create new versions of the columnar files.
With CoW datasets, each time there is an update to a record, the file that contains the record is rewritten with the updated values. With a MoR dataset, each time there is an update, Hudi writes only the row for the changed record.

MoR is better suited for write- or change-heavy workloads with fewer reads.

CoW is better suited for read-heavy workloads on data that changes less frequently.

3. Views/Queries Types:
Apache Hudi provides three logical views for accessing data:

Read-optimized view – Provides the latest committed dataset from CoW tables and the latest compacted dataset from MoR tables.

Incremental view – Provides a change stream between two actions out of a CoW dataset to feed downstream jobs and extract, transform, load (ETL) workflows.

Real-time view – Provides the latest committed data from a MoR table by merging the columnar and row-based files inline.

4. Demo:
5. Steps :
Store the raw data in the S3 bucket (S3 Raw bucket).
Transform/Clean the raw data and store it in Apache Hudi tables ( Analytics S3 bucket) using Apache Spark on Glue.
Query the Analytics data on Analytics S3 bucket via Athena on Read-optimized/Incremental/Real time view by Hudi.
Do incremental changes on the existing data in Analytics S3 bucket.
Query the updated Analytics data on Analytics S3 bucket via Athena on Read-optimized/Incremental/Real time view by Hudi.

In order to create the connector, go to AWS Glue Studio -> Create Custom connector.

Connector S3 URL: 'hudi-spark-bundle_2.11-0.5.3-rc2' Jar from Maven Repository: org.apache.hudi » hudi-spark-bundle (mvnrepository.com) 

Connector Type: Spark

Class Name: org.apache.hudi.DefaultSource.

Also while creating your Glue job using a custom connector, include the Avro-Schema jar (Apache Avro™ Releases) as a dependent jar.

Once the 'HudiExecuteGlueHudiJobRole' is created , grant 'Create database' and 'Create table' permission via AWS Lake formation.

Note: You can grant access to your data by using AWS Glue methods or by using AWS Lake Formation grants. The AWS Glue methods use AWS Identity and Access Management (IAM) policies to achieve access control.

Lake Formation uses a simpler GRANT/REVOKE permissions model similar to the GRANT/REVOKE commands in a relational database system.

