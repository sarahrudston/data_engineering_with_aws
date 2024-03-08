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

# Script generated for node Customer Landing
CustomerLanding_node1699982383179 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sarahr-test-760123/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1699982383179",
)

# Script generated for node Share With Research
ShareWithResearch_node1699982431703 = Filter.apply(
    frame=CustomerLanding_node1699982383179,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ShareWithResearch_node1699982431703",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699982585562 = glueContext.getSink(
    path="s3://sarahr-test-760123/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1699982585562",
)
CustomerTrusted_node1699982585562.setCatalogInfo(
    catalogDatabase="sarah-test", catalogTableName="customer_trusted"
)
CustomerTrusted_node1699982585562.setFormat("json")
CustomerTrusted_node1699982585562.writeFrame(ShareWithResearch_node1699982431703)
job.commit()