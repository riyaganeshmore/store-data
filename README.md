# store-data
Sales Data 
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load raw sales data from S3
sales_df = spark.read.csv("s3://your-bucket/sales/", header=True, inferSchema=True)

# Load store metadata from S3
store_meta_df = spark.read.csv("s3://your-bucket/store_metadata/", header=True, inferSchema=True)

# Join on Store_ID
joined_df = sales_df.join(store_meta_df, on="Store_ID", how="inner")

# Filter invalid transactions
valid_df = joined_df.filter((joined_df.Quantity > 0) & (joined_df.Unit_Price > 0))

# Calculate total sales
valid_df = valid_df.withColumn("Total_Sales", valid_df.Quantity * valid_df.Unit_Price)

# Group by store and product
aggregated_df = valid_df.groupBy("Store_ID", "Store_Name", "Product_ID", "Product_Name") \
    .agg(
        {"Quantity": "sum", "Total_Sales": "sum"}
    ) \
    .withColumnRenamed("sum(Quantity)", "Total_Quantity") \
    .withColumnRenamed("sum(Total_Sales)", "Total_Sales")

# Write result back to S3
aggregated_df.write.mode("overwrite").csv("s3://your-bucket/output/aggregated_sales/", header=True)

job.commit()
