# AWS Glue Studio Notebook
##### You are now running a AWS Glue Studio notebook; To start using your notebook you need to start an AWS Glue Interactive Session.

'''
Need to create a glue catalog db
%%tags
{
    some_tags
}
'''
import sys
import timeit
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import multiprocessing
import pandas
import concurrent.futures as cf
from awsglue.dynamicframe import DynamicFrame
glueContext = GlueContext(SparkContext.getOrCreate())
sparksession = glueContext.spark_session

spark= sparksession.builder\
    .appName('hudi-datasource')\
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')\
    .config('spark.sql.hive.convertMetastoreParquet', 'false')\
    .getOrCreate()
job = Job(glueContext)

inputDynamicframe = glueContext.create_dynamic_frame_from_options(connection_type ="s3",connection_options = {"paths": ["s3://lc-aug1-2023/hudi/source/"], "recurse": True },format = "csv", format_options={"withHeader": True}, transformation_ctx ="dyf")
inputDf = inputDynamicframe.toDF()
inputDf.show()

hudi_options = {
    'hoodie.table.name': 'hudi_data',
    'hoodie.datasource.write.recordkey.field': 'a',
    'hoodie.datasource.write.partitionpath.field': 'a',
    'hoodie.datasource.write.table.name': 'hudi_data',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'a',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}
'''
inputDf.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save('s3://poc-aug1-2023/hudi/output
'''
commonConfig = {'className' : 'org.apache.hudi', 'hoodie.datasource.hive_sync.use_jdbc':'false', 'hoodie.datasource.write.precombine.field': 'a', 'hoodie.datasource.write.recordkey.field': 'a', 'hoodie.table.name': 'hudi_poc', 'hoodie.consistency.check.enabled': 'true', 'hoodie.datasource.hive_sync.database': 'hudipoc', 'hoodie.datasource.hive_sync.table': 'a', 'hoodie.datasource.hive_sync.enable': 'true', 'path': 's3://lc-aug1-2023/hudi/output'}
unpartitionDataConfig = {'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor', 'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'}
incrementalLoadConfig = {'hoodie.upsert.shuffle.parallelism': 10, 'hoodie.datasource.write.operation': 'upsert','hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 'hoodie.cleaner.commits.retained': 10}
deleteConfig = {'hoodie.datasource.write.operation': 'delete'}
combinedConf_upsert = {**commonConfig, **unpartitionDataConfig, **incrementalLoadConfig} 
combinedConf_delete = {**commonConfig, **unpartitionDataConfig, **deleteConfig} 

glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(inputDf, glueContext, "inputDf"), connection_type = "custom.spark", connection_options = combinedConf_upsert)
job.commit()