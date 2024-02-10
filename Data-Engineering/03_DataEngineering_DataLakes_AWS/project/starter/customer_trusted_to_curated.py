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

# Script generated for node S3 - Customer Trusted
S3CustomerTrusted_node1707371931856 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="S3CustomerTrusted_node1707371931856",
)

# Script generated for node S3 - Accelerometer Trusted
S3AccelerometerTrusted_node1707372105561 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="S3AccelerometerTrusted_node1707372105561",
    )
)

# Script generated for node SQL Query
SqlQuery0 = """
select * 
from customer_trusted a 
inner join 
( 
    select distinct user
    from accelerometer_trusted
) b 
on a.email = b.user
"""
SQLQuery_node1707376916777 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": S3CustomerTrusted_node1707371931856,
        "accelerometer_trusted": S3AccelerometerTrusted_node1707372105561,
    },
    transformation_ctx="SQLQuery_node1707376916777",
)

# Script generated for node Drop Fields
DropFields_node1707372162076 = DropFields.apply(
    frame=SQLQuery_node1707376916777,
    paths=["z", "user", "y", "x", "timestamp"],
    transformation_ctx="DropFields_node1707372162076",
)

# Script generated for node S3 - Customers Curated
S3CustomersCurated_node1707372174271 = glueContext.getSink(
    path="s3://ericliu-udacity-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3CustomersCurated_node1707372174271",
)
S3CustomersCurated_node1707372174271.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
S3CustomersCurated_node1707372174271.setFormat("json")
S3CustomersCurated_node1707372174271.writeFrame(DropFields_node1707372162076)
job.commit()
