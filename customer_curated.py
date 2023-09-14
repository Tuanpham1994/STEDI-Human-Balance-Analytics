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

# Script generated for node customers_curated
customers_curated_node1689418890499 = glueContext.write_dynamic_frame.from_catalog(
    frame=Accelerometer_filtered_node1689410600401,
    database="tuanpa40",
    table_name="customer_curated",
    transformation_ctx="customers_curated_node1689418890499",
)

job.commit()
