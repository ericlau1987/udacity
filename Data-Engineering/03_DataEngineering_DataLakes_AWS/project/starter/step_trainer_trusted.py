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

# Script generated for node Customer Curated
CustomerCurated_node1707568730605 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1707568730605",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1707561313456 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1707561313456",
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
SQLQuery_node1707568062975 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node1707561313456,
        "customer_curated": CustomerCurated_node1707568730605,
    },
    transformation_ctx="SQLQuery_node1707568062975",
)

# Script generated for node Amazon S3
AmazonS3_node1707561940102 = glueContext.getSink(
    path="s3://ericliu-udacity-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1707561940102",
)
AmazonS3_node1707561940102.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1707561940102.setFormat("json")
AmazonS3_node1707561940102.writeFrame(SQLQuery_node1707568062975)
job.commit()
