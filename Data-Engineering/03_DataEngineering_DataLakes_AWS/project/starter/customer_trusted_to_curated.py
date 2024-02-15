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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1707983376672 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1707983376672",
)

# Script generated for node customer_trusted
customer_trusted_node1707983360836 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1707983360836",
)

# Script generated for node SQL Query
SqlQuery0 = """
select a.* 
from customer_trusted a 
inner join 
( 
    select distinct user
    from accelerometer_trusted
) b 
on a.email = b.user
"""
SQLQuery_node1707983398919 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": accelerometer_trusted_node1707983376672,
        "customer_trusted": customer_trusted_node1707983360836,
    },
    transformation_ctx="SQLQuery_node1707983398919",
)

# Script generated for node customer_curated
customer_curated_node1707983474157 = glueContext.getSink(
    path="s3://ericliu-udacity-lake-house/customers/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1707983474157",
)
customer_curated_node1707983474157.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
customer_curated_node1707983474157.setFormat("json")
customer_curated_node1707983474157.writeFrame(SQLQuery_node1707983398919)
job.commit()