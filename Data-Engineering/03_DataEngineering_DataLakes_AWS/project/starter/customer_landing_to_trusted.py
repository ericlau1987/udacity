import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1707370430543 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="AmazonS3_node1707370430543",
)

# Script generated for node Filter
Filter_node1707370467034 = Filter.apply(
    frame=AmazonS3_node1707370430543,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="Filter_node1707370467034",
)

# Script generated for node Amazon S3
AmazonS3_node1707370550276 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1707370467034,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://ericliu-udacity-lake-house/customer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1707370550276",
)

job.commit()
