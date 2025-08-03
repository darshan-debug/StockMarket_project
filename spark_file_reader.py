from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col,  min, max, count, max_by
from pyspark.sql.types import StructType, StructField, StringType
import os
import json
from collections import defaultdict
import threading
import time
from flask import Flask, render_template # Import render_template

stock_trade_data=defaultdict(dict)
stock_data_lock = threading.Lock() # mutex lock for global var: "stock_trade_data" , to ensure thread-safe access
app = Flask(__name__, template_folder='templates') # Specify template folder

# --- Web Page Endpoint (Flask will render HTML directly) ---
@app.route('/', methods=['GET'])
def index():
    # Get the overall last updated time (e.g., from the latest stock update)
    overall_last_updated = "N/A"  # Default value if no data is available
    # Convert the defaultdict values to a list for Jinja2 template
    # Also, format numeric values and percentage here for easier templating
    display_data = []
    with stock_data_lock: # NEW: Use 'with' statement for automatic lock acquisition/release
        for stock_Name, data in stock_trade_data.items():
            # Ensure numbers are floats before formatting
            current_nav = data.get('most_recent_price')
            min_p = data.get('min_price')
            max_p = data.get('max_price')        
            last_updated_time=data.get('last_updated_time', 'N/A') # Already ISO format string
            if(overall_last_updated == "N/A"):
                overall_last_updated = last_updated_time  # Set the first available time as overall last updated
            else:
                overall_last_updated = max(overall_last_updated, last_updated_time)  # Keep the latest time
            display_data.append({
                'stock': stock_Name,
                'most_recent_price': f"{current_nav:.2f}" if current_nav is not None else 'N/A',
                'min_price': f"{min_p:.2f}" if min_p is not None else 'N/A',
                'max_price': f"{max_p:.2f}" if max_p is not None else 'N/A',
                'total_transactions': data.get('total_transactions', 0),
                'last_updated_time': f"{last_updated_time}" if last_updated_time is not None else 'N/A',
            })
    
    # Sort data by stock symbol for consistent display
    display_data.sort(key=lambda x: x['stock'])
    return render_template('index.html', stocks=display_data, last_updated=overall_last_updated)


# --- Custom foreachBatch function to update global dictionary ---
def run_spark_stream_processor():
    global stock_trade_data,stock_data_lock
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

    def write_batch_to_single_json(df, epoch_id):
        global stock_trade_data,stock_data_lock
        updated_stocks=df.collect()
        # Only write if the DataFrame for this batch is not empty
        with stock_data_lock: # NEW: Use 'with' statement for automatic lock acquisition/release
            if updated_stocks:
                for row in updated_stocks:
                    stock_trade_data[row['stock']] = {
                        'total_transactions': row['total_transactions'],
                        'min_price': row['min_price'],
                        'max_price': row['max_price'],
                        'most_recent_price': row['most_recent_price']                
                    }
                print(stock_trade_data)
            else:
                print(f"Batch {epoch_id}: No updates, skipping update to global var: stock_trade_data")
        
    # --- Write the streaming data to the console ---
    # THE NEW TRIGGER AND OUTPUT MODE:
    # Trigger is set to a fixed interval of 30 seconds.
    # This ensures that Spark will run a micro-batch every 30 seconds.
    query = stock_summary_df \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(write_batch_to_single_json) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime='50 seconds') \
        .start()
    #.format("console")
    # Wait for the termination of the query
    query.awaitTermination()
if __name__ == '__main__':
    #run_spark_stream_processor()

    # Start Spark Streaming in a separate thread
    spark_thread = threading.Thread(target=run_spark_stream_processor)
    spark_thread.daemon = True #do not wait for this thread to finish, and continue with the main thread
    spark_thread.start()

    # Give Spark a moment to initialize (optional, but good practice)
    time.sleep(10) 
    # flask app will run in the main thread
    # Start Flask app in the main thread
    print("Starting Flask web server on http://127.0.0.1:5000")
    app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)