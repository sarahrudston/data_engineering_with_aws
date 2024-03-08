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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1700147366230 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sarahr-test-760123/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1700147366230",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1700147547175 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sarahr-test-760123/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1700147547175",
)

# Script generated for node Join
Join_node1700147583735 = Join.apply(
    frame1=AccelerometerTrusted_node1700147366230,
    frame2=StepTrainerTrusted_node1700147547175,
    keys1=["timestamp"],
    keys2=["right_sensorreadingtime"],
    transformation_ctx="Join_node1700147583735",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1700147694705 = glueContext.getSink(
    path="s3://sarahr-test-760123/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1700147694705",
)
MachineLearningCurated_node1700147694705.setCatalogInfo(
    catalogDatabase="sarah-test", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1700147694705.setFormat("json")
MachineLearningCurated_node1700147694705.writeFrame(Join_node1700147583735)
job.commit()
