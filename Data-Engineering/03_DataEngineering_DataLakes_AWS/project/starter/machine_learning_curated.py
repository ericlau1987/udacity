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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1707570258171 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1707570258171",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707569771843 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1707569771843",
)

# Script generated for node SQL Query
SqlQuery0 = """
select a.* ,
    coalesce(distancefromobject,0) as distancefromobject
from accelerometer_trusted a 
left join step_trainer_trusted b 
on a.timestamp = b.sensorreadingtime
"""
SQLQuery_node1707571405360 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1707569771843,
        "step_trainer_trusted": StepTrainerTrusted_node1707570258171,
    },
    transformation_ctx="SQLQuery_node1707571405360",
)

# Script generated for node Amazon S3
AmazonS3_node1707570829378 = glueContext.getSink(
    path="s3://ericliu-udacity-lake-house/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1707570829378",
)
AmazonS3_node1707570829378.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
AmazonS3_node1707570829378.setFormat("json")
AmazonS3_node1707570829378.writeFrame(SQLQuery_node1707571405360)
job.commit()
