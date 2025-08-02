from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("StockMarketTracker").getOrCreate()

kafka_df=spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "quickstart-events.stockmarket.transaction").option("startingOffsets", "earliest").option("kafka.group.id", "stockmarket_liveWebsite_CG1").load()

query=kafka_df.writeStream.format("console").outputMode("append").option("truncate", "false").start()

query.awaitTermination()
spark.stop()
# to verify,if variable for spark are correctly setup: run this command in terminal,after activating venv: $env:PYSPARK_PYTHON
# below command, will initially download the required packages and start the spark session
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 main.py