from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os
# --- Spark Session Setup ---
spark = SparkSession.builder \
    .appName("FileStreamProcessor") \
    .getOrCreate()

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# --- Define the Schema of your Kafka Messages (as they appear in the file) ---
# This is crucial for parsing the JSON from the files.
# Make sure this matches the structure of messages your Kafka producer sends.
schema = StructType([
    # The "_id" field is a nested structure (another StructType)
    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ]), True),    
    # The "stock" field is a string
    StructField("stock", StringType(), True),    
    # The "type" field is a string
    StructField("type", StringType(), True),    
    # The "price" field is a string in your JSON, but we define it as a Double
    # You would need to cast the string to a Double when reading the data
    StructField("price", StringType(), True)
])

# --- Read from the File System as a Streaming Source ---
# Spark will monitor this directory for new files.
# When new files appear, it will read them and process their contents.
INPUT_DIR = 'kafka_data_stream' # Must match the OUTPUT_DIR in your Python script
CHECKPOINT_LOCATION = 'checkpoint' # Spark needs this for fault tolerance

# Ensure checkpoint directory exists
os.makedirs(CHECKPOINT_LOCATION, exist_ok=True)

file_stream_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("path", INPUT_DIR) \
    .option("maxFilesPerTrigger", 1) \
    .load()

# --- Process the Data ---
# Convert the timestamp column (assuming it's a Unix timestamp string)
processed_df = file_stream_df.withColumn(
    "transaction_id", 
    col("_id.$oid")
).withColumn(
    "price",
    col("price").cast("double")
).select(
    "transaction_id",
    "stock",
    "type",
    "price"
)
# --- Write the streaming data to the console ---
query = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

# Wait for the termination of the query
query.awaitTermination()