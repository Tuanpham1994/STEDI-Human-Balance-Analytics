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
customer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tuanpa40-lake-house/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1",
)

# Script generated for node FilterPrivacy
FilterPrivacy_node1689406596484 = Filter.apply(
    frame=customer_landing_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="FilterPrivacy_node1689406596484",
)

# Script generated for node customer_trusted_zone
customer_trusted_zone_node1689407035694 = glueContext.write_dynamic_frame.from_options(
    frame=FilterPrivacy_node1689406596484,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tuanpa40-lake-house/customer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_trusted_zone_node1689407035694",
)

job.commit()
