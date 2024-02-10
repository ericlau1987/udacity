import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1707370837223 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1707370837223",
)

# Script generated for node S3 - Accelerometer Landing
S3AccelerometerLanding_node1707358406993 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://ericliu-udacity-lake-house/accelerometer/landing/"],
            "recurse": True,
        },
        transformation_ctx="S3AccelerometerLanding_node1707358406993",
    )
)

# Script generated for node Join
Join_node1707370852604 = Join.apply(
    frame1=S3AccelerometerLanding_node1707358406993,
    frame2=AWSGlueDataCatalog_node1707370837223,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1707370852604",
)

# Script generated for node Drop Fields
DropFields_node1707370901110 = DropFields.apply(
    frame=Join_node1707370852604,
    paths=["email", "phone"],
    transformation_ctx="DropFields_node1707370901110",
)

# Script generated for node Amazon S3
AmazonS3_node1707370945606 = glueContext.getSink(
    path="s3://ericliu-udacity-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1707370945606",
)
AmazonS3_node1707370945606.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1707370945606.setFormat("json")
AmazonS3_node1707370945606.writeFrame(DropFields_node1707370901110)
job.commit()
