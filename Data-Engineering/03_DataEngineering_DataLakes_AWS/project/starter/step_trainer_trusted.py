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

# Script generated for node customer_curated
customer_curated_node1707983801913 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1707983801913",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1707983822889 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1707983822889",
)

# Script generated for node SQL Query
SqlQuery0 = """
select a.*
from step_trainer_landing a 
inner join (
    select distinct serialNumber
    from customer_curated
) b 
on a.serialNumber=b.serialnumber
"""
SQLQuery_node1707983853700 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_landing": step_trainer_landing_node1707983822889,
        "customer_curated": customer_curated_node1707983801913,
    },
    transformation_ctx="SQLQuery_node1707983853700",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1707983923558 = glueContext.getSink(
    path="s3://ericliu-udacity-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1707983923558",
)
step_trainer_trusted_node1707983923558.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1707983923558.setFormat("json")
step_trainer_trusted_node1707983923558.writeFrame(SQLQuery_node1707983853700)
job.commit()
