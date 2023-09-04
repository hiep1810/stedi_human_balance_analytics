import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1693814598484 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_landing",
        transformation_ctx="AccelerometerLandingZone_node1693814598484",
    )
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1693800881735 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1693800881735",
)

# Script generated for node Accelerometer - Distinct User
AccelerometerDistinctUser_node1693827801164 = DynamicFrame.fromDF(
    AccelerometerLandingZone_node1693814598484.toDF().dropDuplicates(["user"]),
    glueContext,
    "AccelerometerDistinctUser_node1693827801164",
)

# Script generated for node Inner Join
InnerJoin_node1693814662495 = Join.apply(
    frame1=CustomerTrustedZone_node1693800881735,
    frame2=AccelerometerDistinctUser_node1693827801164,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="InnerJoin_node1693814662495",
)

# Script generated for node Drop Fields
DropFields_node1693814722753 = DropFields.apply(
    frame=InnerJoin_node1693814662495,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1693814722753",
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1693802363644 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1693814722753,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-1810/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCuratedZone_node1693802363644",
)

job.commit()
