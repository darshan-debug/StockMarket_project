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
import builtins # Import builtins for max function, note: max of spark is colliding with max of python builtins, thus importing builtins explicitly
stock_trade_data=defaultdict(dict)
stock_data_lock = threading.Lock() # mutex lock for global var: "stock_trade_data" , to ensure thread-safe access
app = Flask(__name__, template_folder='templates') # Specify template folder


@app.route('/', methods=['GET'])
def index():
    
    overall_last_updated = "N/A"  # Default value if no data is available
    # Convert the defaultdict values to a list for Jinja2 template
    
    display_data = []
    with stock_data_lock: # 'with' statement for automatic lock acquisition/release
        for stock_Name, data in stock_trade_data.items():           
            current_nav = data.get('most_recent_price')
            min_p = data.get('min_price')
            max_p = data.get('max_price')        
            last_updated_time=data.get('last_updated_time', 'N/A') 
            last_day_price=data.get('last_day_price', 0) 
            if(overall_last_updated == "N/A"):
                overall_last_updated = last_updated_time  # Set the first available time as overall last updated
            else:
                overall_last_updated = builtins.max(overall_last_updated, last_updated_time)  # Keep the latest time,had to use builtins.max to avoid conflict with spark max function
            display_data.append({
                'stock': stock_Name,
                'most_recent_price': f"{current_nav:.2f}" if current_nav is not None else 'N/A',
                'last_day_price': f"{last_day_price:.2f}" if last_day_price is not None else 0,
                'min_price': f"{min_p:.2f}" if min_p is not None else 'N/A',
                'max_price': f"{max_p:.2f}" if max_p is not None else 'N/A',
                'total_transactions': data.get('total_transactions', 0),
                'last_updated_time': f"{last_updated_time}" if last_updated_time is not None else 'N/A',
                'change_percent': f"{(((current_nav - last_day_price) / last_day_price) * 100):.2f}%" if current_nav is not None and last_day_price > 0 else 'N/A'
            })
    
    # Sorting data by stock name, but NSE doesn't follow this
    display_data.sort(key=lambda x: x['stock'])
    return render_template('index.html', stocks=display_data, last_updated=overall_last_updated)


# below function is for running the spark stream processor code, idea is to run it over a separate thread
def run_spark_stream_processor():
    global stock_trade_data,stock_data_lock
    # --- Spark Session Setup ---
    spark = SparkSession.builder \
        .appName("FileStreamProcessor") \
        .getOrCreate()

    
    spark.sparkContext.setLogLevel("WARN")

   
    schema = StructType([
        # The "_id" field has a nested structure
        StructField("_id", StructType([
            StructField("$oid", StringType(), True)
        ]), True), 
        StructField("previousPrice", StringType(), True),      
        StructField("stock", StringType(), True),            
        StructField("type", StringType(), True),            
        StructField("price", StringType(), True),
        StructField("received_time", StringType(), True)
    ])

  
    
    INPUT_DIR = 'kafka_data_stream' # kafka msgs are unloaded in this dir, spark will loook for new file addition in this dir
    CHECKPOINT_LOCATION = 'checkpoint' # checkoint for spark stream
    
    # Ensure checkpoint directory exists
    os.makedirs(CHECKPOINT_LOCATION, exist_ok=True)

    file_stream_df = spark.readStream \
        .format("json") \
        .schema(schema) \
        .option("path", INPUT_DIR) \
        .load()

    # cast current price and last day NAV to double
    
    processed_df = file_stream_df.withColumn(
        "price_double",
        col("price").cast("double")
    ).withColumn(
        "previousPrice_double",
        col("previousPrice").cast("double")
    )

    # 2. THE NEW AGGREGATION LOGIC:
    #    Grouping the data by 'stock' to calculate min, max, and the last seen price.
    stock_summary_df = processed_df.groupBy("stock").agg(
        count("stock").alias("total_transactions"),
        min("price_double").alias("min_price"),
        max("price_double").alias("max_price"),
        max("previousPrice_double").alias("last_day_price"),
        max_by(col("price_double"), col("received_time")).alias("most_recent_price"),        
        max("received_time").alias("last_updated_time")
    )

    def write_batch_to_single_json(df, epoch_id):
        global stock_trade_data,stock_data_lock
        updated_stocks=df.collect()
        
        with stock_data_lock: #  Using 'with' statement for automatic lock acquisition/release
            if updated_stocks:
                for row in updated_stocks:
                    stock_trade_data[row['stock']] = {
                        'total_transactions': row['total_transactions'],
                        'min_price': row['min_price'],
                        'max_price': row['max_price'],
                        'last_day_price': row['last_day_price'],
                        'most_recent_price': row['most_recent_price']  ,
                        'last_updated_time': row['last_updated_time']          
                    }
                print(stock_trade_data)
            else:
                print(f"Batch {epoch_id}: No updates, skipping update to global var: stock_trade_data")
        

    #  Spark will run a micro-batch every 50 seconds.
    query = stock_summary_df \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(write_batch_to_single_json) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime='50 seconds') \
        .start()
    #.format("console") alternative of .foreachBatch() for debug
    
    query.awaitTermination()
if __name__ == '__main__':
    #run_spark_stream_processor()

    # Start Spark Streaming in a separate thread
    spark_thread = threading.Thread(target=run_spark_stream_processor)
    spark_thread.daemon = True #program shld not wait for this thread to finish, and continue with the main thread
    spark_thread.start()

    # spark thread gets dedicated time to start
    time.sleep(10) 
    # flask app will run in the main thread
    
    print("Starting Flask web server on http://127.0.0.1:5000")
    app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)