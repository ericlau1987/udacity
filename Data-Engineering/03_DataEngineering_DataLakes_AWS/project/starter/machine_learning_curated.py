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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1707984173680 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1707984173680",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1707984193836 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1707984193836",
)

# Script generated for node SQL Query
SqlQuery0 = """
select a.* ,
    coalesce(distancefromobject,0) as distancefromobject
from accelerometer_trusted a 
left join step_trainer_trusted b 
on a.timestamp = b.sensorreadingtime
"""
SQLQuery_node1707984211244 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": accelerometer_trusted_node1707984193836,
        "step_trainer_trusted": step_trainer_trusted_node1707984173680,
    },
    transformation_ctx="SQLQuery_node1707984211244",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1707984809585 = glueContext.getSink(
    path="s3://ericliu-udacity-lake-house/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1707984809585",
)
machine_learning_curated_node1707984809585.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1707984809585.setFormat("json")
machine_learning_curated_node1707984809585.writeFrame(SQLQuery_node1707984211244)
job.commit()
