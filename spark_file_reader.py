from pyspark.sql import SparkSession
from pyspark.sql.functions import  col,  min, max, count, max_by
from pyspark.sql.types import StructType, StructField, StringType
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
    StructField("price", StringType(), True),
    StructField("received_time", StringType(), True)
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
    .load()

# --- Process the Data ---
# Convert the timestamp column (assuming it's a Unix timestamp string)
processed_df = file_stream_df.withColumn(
    "price_double",
    col("price").cast("double")
)

# 2. THE NEW AGGREGATION LOGIC:
#    Group the data by 'stock' and calculate min, max, and the last seen price.
stock_summary_df = processed_df.groupBy("stock").agg(
    count("stock").alias("total_transactions"),
    min("price_double").alias("min_price"),
    max("price_double").alias("max_price"),
    max_by(col("price_double"), col("received_time")).alias("most_recent_price"),
    # Get the latest event_timestamp itself for the 'last updated' time
    max("received_time").alias("last_updated_time")
)

# --- Write the streaming data to the console ---
# THE NEW TRIGGER AND OUTPUT MODE:
# Trigger is set to a fixed interval of 30 seconds.
# This ensures that Spark will run a micro-batch every 30 seconds.
query = stock_summary_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .trigger(processingTime='30 seconds') \
    .start()
# Wait for the termination of the query
query.awaitTermination()