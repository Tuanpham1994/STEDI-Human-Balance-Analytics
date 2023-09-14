import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_landing
customer_landing_node1689410475126 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tuanpa40-lake-house/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1689410475126",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tuanpa40-lake-house/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1",
)

# Script generated for node step_trainer
step_trainer_node1689415508332 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tuanpa40-lake-house/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_node1689415508332",
)

# Script generated for node FilterPrivacy
FilterPrivacy_node1689410514293 = Filter.apply(
    frame=customer_landing_node1689410475126,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="FilterPrivacy_node1689410514293",
)

# Script generated for node Accelerometer_filtered
Accelerometer_filtered_node1689410600401 = Join.apply(
    frame1=FilterPrivacy_node1689410514293,
    frame2=accelerometer_landing_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Accelerometer_filtered_node1689410600401",
)

# Script generated for node Renamed keys for step_trainer_trusted
Renamedkeysforstep_trainer_trusted_node1689415744490 = ApplyMapping.apply(
    frame=Accelerometer_filtered_node1689410600401,
    mappings=[
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("phone", "string", "right_phone", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("registrationDate", "bigint", "right_registrationDate", "bigint"),
        ("lastUpdateDate", "bigint", "right_lastUpdateDate", "bigint"),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "right_shareWithResearchAsOfDate",
            "bigint",
        ),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "right_shareWithPublicAsOfDate",
            "bigint",
        ),
        (
            "shareWithFriendsAsOfDate",
            "bigint",
            "right_shareWithFriendsAsOfDate",
            "bigint",
        ),
        ("user", "string", "right_user", "string"),
        ("timeStamp", "bigint", "right_timeStamp", "bigint"),
        ("x", "double", "right_x", "double"),
        ("y", "double", "right_y", "double"),
        ("z", "double", "right_z", "double"),
    ],
    transformation_ctx="Renamedkeysforstep_trainer_trusted_node1689415744490",
)

# Script generated for node step_trainer_join
step_trainer_join_node1689415663410 = Join.apply(
    frame1=step_trainer_node1689415508332,
    frame2=Renamedkeysforstep_trainer_trusted_node1689415744490,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="step_trainer_join_node1689415663410",
)

# Script generated for node accelerometer_trusted_zone
accelerometer_trusted_zone_node1689411827421 = (
    glueContext.write_dynamic_frame.from_options(
        frame=Accelerometer_filtered_node1689410600401,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://tuanpa40-lake-house/curated/",
            "partitionKeys": [],
        },
        transformation_ctx="accelerometer_trusted_zone_node1689411827421",
    )
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1689420193604 = glueContext.write_dynamic_frame.from_catalog(
    frame=step_trainer_join_node1689415663410,
    database="tuanpa40",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1689420193604",
)

# Script generated for node Curated_Trusted_Zone
Curated_Trusted_Zone_node1689415943775 = glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_join_node1689415663410,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tuanpa40-lake-house/curated_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Curated_Trusted_Zone_node1689415943775",
)

job.commit()
