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

# Script generated for node accelerometer_landing
accelerometer_landing_node1707982950352 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1707982950352",
)

# Script generated for node customer_trusted
customer_trusted_node1707983022210 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1707983022210",
)

# Script generated for node Join
Join_node1707983042303 = Join.apply(
    frame1=customer_trusted_node1707983022210,
    frame2=accelerometer_landing_node1707982950352,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1707983042303",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1707983058835 = glueContext.getSink(
    path="s3://ericliu-udacity-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1707983058835",
)
accelerometer_trusted_node1707983058835.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1707983058835.setFormat("json")
accelerometer_trusted_node1707983058835.writeFrame(Join_node1707983042303)
job.commit()