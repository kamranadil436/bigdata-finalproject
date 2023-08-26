import sys
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

# Read CSV data directly into a DataFrame
data_frame = spark.read.csv(
    "s3://my-raw-data-bucket-bdpro/dataset.csv",
    header=True,
    inferSchema=True,
)

# Perform null value removal
data_frame_no_nulls = data_frame.dropna()

# Write the result in Parquet format to Amazon S3
data_frame_no_nulls.write.parquet(
    "s3://my-analytics-bucket-bdpro/dataset.parquet",
    mode="overwrite",
    compression="snappy",
)

job.commit()
