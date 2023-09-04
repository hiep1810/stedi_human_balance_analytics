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

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1693814598484 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="step_trainer_trusted",
        transformation_ctx="StepTrainerTrustedZone_node1693814598484",
    )
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1693800881735 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedZone_node1693800881735",
    )
)

# Script generated for node Inner Join
InnerJoin_node1693814662495 = Join.apply(
    frame1=AccelerometerTrustedZone_node1693800881735,
    frame2=StepTrainerTrustedZone_node1693814598484,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="InnerJoin_node1693814662495",
)

# Script generated for node Machine Learning Curated Zone
MachineLearningCuratedZone_node1693802363644 = (
    glueContext.write_dynamic_frame.from_options(
        frame=InnerJoin_node1693814662495,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://stedi-lake-house-1810/machine_learning/curated/",
            "partitionKeys": [],
        },
        transformation_ctx="MachineLearningCuratedZone_node1693802363644",
    )
)

job.commit()
