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

# Script generated for node Accelerator Landing
AcceleratorLanding_node1699997396772 = glueContext.create_dynamic_frame.from_catalog(
    database="sarah-test",
    table_name="accelerometer_landing",
    transformation_ctx="AcceleratorLanding_node1699997396772",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1699997567086 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sarahr-test-760123/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1699997567086",
)

# Script generated for node Join Customer
JoinCustomer_node1699997693362 = Join.apply(
    frame1=CustomerTrustedZone_node1699997567086,
    frame2=AcceleratorLanding_node1699997396772,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node1699997693362",
)

# Script generated for node Drop Fields
DropFields_node1700061969091 = ApplyMapping.apply(
    frame=JoinCustomer_node1699997693362,
    mappings=[
        ("z", "double", "z", "double"),
        ("user", "string", "user", "string"),
        ("y", "double", "y", "double"),
        ("x", "double", "x", "double"),
        ("timestamp", "long", "timestamp", "long"),
    ],
    transformation_ctx="DropFields_node1700061969091",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1700061817115 = glueContext.getSink(
    path="s3://sarahr-test-760123/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1700061817115",
)
AccelerometerTrusted_node1700061817115.setCatalogInfo(
    catalogDatabase="sarah-test", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1700061817115.setFormat("json")
AccelerometerTrusted_node1700061817115.writeFrame(DropFields_node1700061969091)
job.commit()
