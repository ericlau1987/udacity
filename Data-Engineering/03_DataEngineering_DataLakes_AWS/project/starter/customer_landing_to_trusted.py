import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1707981030556 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ericliu-udacity-lake-house/customers/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1707981030556",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT *
FROM customer_landing
where sharewithresearchasofdate != 0
"""
SQLQuery_node1707981583436 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": CustomerLanding_node1707981030556},
    transformation_ctx="SQLQuery_node1707981583436",
)

# Script generated for node Amazon S3
AmazonS3_node1707981677461 = glueContext.getSink(
    path="s3://ericliu-udacity-lake-house/customers/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1707981677461",
)
AmazonS3_node1707981677461.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
AmazonS3_node1707981677461.setFormat("json")
AmazonS3_node1707981677461.writeFrame(SQLQuery_node1707981583436)
job.commit()
