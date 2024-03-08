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

# Script generated for node Accelerometer Landed
AccelerometerLanded_node1700127615223 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sarahr-test-760123/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanded_node1700127615223",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1700127593124 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sarahr-test-760123/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1700127593124",
)

# Script generated for node Customer Privacy
CustomerPrivacy_node1700127645637 = Join.apply(
    frame1=AccelerometerLanded_node1700127615223,
    frame2=CustomerTrusted_node1700127593124,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacy_node1700127645637",
)

# Script generated for node Drop Fields
DropFields_node1700127689784 = ApplyMapping.apply(
    frame=CustomerPrivacy_node1700127645637,
    mappings=[
        ("serialnumber", "string", "serialnumber", "string"),
        ("birthday", "string", "birthday", "string"),
        ("registrationdate", "long", "registrationdate", "long"),
        ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"),
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("phone", "string", "phone", "string"),
        ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"),
        ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="DropFields_node1700127689784",
)

# Script generated for node Select distinct
SqlQuery0 = """
select distinct * from myDataSource
"""
Selectdistinct_node1700139028053 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": DropFields_node1700127689784},
    transformation_ctx="Selectdistinct_node1700139028053",
)

# Script generated for node Customer Curated
CustomerCurated_node1700127981626 = glueContext.getSink(
    path="s3://sarahr-test-760123/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1700127981626",
)
CustomerCurated_node1700127981626.setCatalogInfo(
    catalogDatabase="sarah-test", catalogTableName="customer_curated"
)
CustomerCurated_node1700127981626.setFormat("json")
CustomerCurated_node1700127981626.writeFrame(Selectdistinct_node1700139028053)
job.commit()
