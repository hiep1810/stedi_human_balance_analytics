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

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1693814598484 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="step_trainer_landing",
        transformation_ctx="StepTrainerLandingZone_node1693814598484",
    )
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1693800881735 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCuratedZone_node1693800881735",
)

# Script generated for node Inner Join
InnerJoin_node1693814662495 = Join.apply(
    frame1=CustomerCuratedZone_node1693800881735,
    frame2=StepTrainerLandingZone_node1693814598484,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="InnerJoin_node1693814662495",
)

# Script generated for node Drop Fields
DropFields_node1693814722753 = DropFields.apply(
    frame=InnerJoin_node1693814662495,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
        "`.serialnumber`",
    ],
    transformation_ctx="DropFields_node1693814722753",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1693802363644 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1693814722753,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-1810/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrustedZone_node1693802363644",
)

job.commit()
