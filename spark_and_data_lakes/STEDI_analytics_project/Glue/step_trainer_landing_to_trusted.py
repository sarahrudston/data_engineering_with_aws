import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
CustomerCurated_node1700135816953 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sarahr-test-760123/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1700135816953",
)

# Script generated for node Step Trainer Landed
StepTrainerLanded_node1700127102085 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sarahr-test-760123/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanded_node1700127102085",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1700137090091 = ApplyMapping.apply(
    frame=StepTrainerLanded_node1700127102085,
    mappings=[
        ("sensorreadingtime", "long", "right_sensorreadingtime", "long"),
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("distancefromobject", "int", "right_distancefromobject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1700137090091",
)

# Script generated for node Join
CustomerCurated_node1700135816953DF = CustomerCurated_node1700135816953.toDF()
RenamedkeysforJoin_node1700137090091DF = RenamedkeysforJoin_node1700137090091.toDF()
Join_node1700136854482 = DynamicFrame.fromDF(
    CustomerCurated_node1700135816953DF.join(
        RenamedkeysforJoin_node1700137090091DF,
        (
            CustomerCurated_node1700135816953DF["serialnumber"]
            == RenamedkeysforJoin_node1700137090091DF["right_serialnumber"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1700136854482",
)

# Script generated for node Drop fields
Dropfields_node1700137023905 = ApplyMapping.apply(
    frame=Join_node1700136854482,
    mappings=[
        ("right_sensorreadingtime", "long", "right_sensorreadingtime", "long"),
        ("right_serialnumber", "string", "right_serialnumber", "string"),
        ("right_distancefromobject", "int", "right_distancefromobject", "int"),
    ],
    transformation_ctx="Dropfields_node1700137023905",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1700137434536 = glueContext.getSink(
    path="s3://sarahr-test-760123/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1700137434536",
)
StepTrainerTrusted_node1700137434536.setCatalogInfo(
    catalogDatabase="sarah-test", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1700137434536.setFormat("json")
StepTrainerTrusted_node1700137434536.writeFrame(Dropfields_node1700137023905)
job.commit()
