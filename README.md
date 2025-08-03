
# 📈 Real-time Stock Market Dashboard with Spark Streaming Analytics

## ✨ Introduction

Developed a **real-time stock market dashboard** simulating live National Stock Exchange (NSE) data, simulated from live stock trades. This comprehensive project showcases robust expertise in distributed stream processing, dynamic web development, and intuitive data visualization, providing a tangible demonstration of building end-to-end data solutions.

## 🚀 Key Contributions

* **Data Ingestion & Streaming Backbone:**
  Raw stock transaction data originates from **MongoDB** 📊, serving as the primary source of historical and new trade records (acting as operatonal DB). This data is seamlessly ingested into **Apache Kafka** 🔗, leveraging its capabilities for robust, high-throughput, and fault-tolerant streaming. The integration is achieved via a specialized **MongoDB Kafka Connector**, which efficiently captures changes from MongoDB and publishes them as events to Kafka. Data is meticulously organized into dedicated Kafka **topics** 🏷️, enabling clear separation and efficient consumption by downstream services. This foundational layer ensures that all market activities are captured and made available for immediate processing.

* **Streaming ETL & Real-time Aggregation:**
  A powerful **Apache Spark Structured Streaming** application forms the analytical core of this dashboard. It performs continuous ETL (Extract, Transform, Load) operations and real-time aggregation on the incoming data streams from Kafka. This fault-tolerant processing computes critical stock metrics on the fly, including: the total number of transactions 🔢 for each stock, its minimum and maximum prices 📉📈 observed over the stream, and crucially, the most recent Net Asset Value (NAV) 💰. The design explicitly ensures deterministic results through precise event-time ordering, handling potential out-of-order data arrivals to maintain accuracy.

* **In-Memory Data Persistence & Future Integration:**
  For immediate, low-latency display on the dashboard, the current architecture leverages **in-memory storage** within the application. This provides rapid access to the latest aggregated data. However, the underlying system is thoughtfully engineered for seamless integration with more robust, persistent stores like **MongoDB** 💾 for long-term data retention. A planned future enhancement involves utilizing **Kafka Connect for MongoDB** to efficiently sink aggregated data from specific Kafka topics directly into MongoDB, enabling comprehensive historical analysis, trend identification, and enhanced data durability beyond the application's runtime.

* **Dynamic Web-based Visualization:**
  The user interface is powered by a **Python Flask** web application 🌐, serving as the interactive dashboard. This application directly accesses Spark's continuously updated, in-memory, and thread-safe aggregated data. It dynamically renders a live HTML interface that provides up-to-the-minute market insights. Users can obtain the latest data by simply refreshing their browser, retrieving the most current state from the Flask server.

* **Concurrency & Optimized Performance:**
  Robust concurrency management is a cornerstone of this system, achieved through the strategic use of Python's `threading` module 🧵. This ensures impeccable data consistency and integrity by synchronizing access to shared in-memory data structures between the high-volume Spark processing thread and the responsive Flask web-serving thread. This optimized, multi-threaded architecture delivers a fluid, near real-time view of dynamic market data, minimizing latency and maximizing responsiveness.

## 🌟 Skills Demonstrated

* **End-to-End Data Pipeline Development** 🏗️: From data source to interactive dashboard.

* **Real-time Data Stream Handling** 🌊: Processing continuous flows of information efficiently.

* **Distributed Computing (Apache Spark)** ⚡: Leveraging Spark for scalable, fault-tolerant analytics.

* **Message Queuing (Apache Kafka)** 📧: Building reliable, high-throughput data ingestion and distribution.

* **Web Application Development (Flask)** 💻: Crafting dynamic and responsive web interfaces.

* **Concurrency & Thread Safety** 🔒: Managing shared resources in multi-threaded environments.

* **Data Visualization & UI/UX Principles** ✨: Presenting complex data clearly and effectively.
```

project configuration:


one time configurations:
1) pip install requirements
2) download mongoDB kafka connector, from : https://www.confluent.io/hub/mongodb/kafka-connect-mongodb
install mongoDB : MongoDB Community Edition, with gui tool: compass
ensure the mongoDB service is running in "services" for your PC
open mongodb compass( or mongo db shell inside compass, I used compass),create  a connection to localhost: mongodb://localhost:27017/
create database: stockmarket  collection: stock
add another collection: transaction

steps to run :
0) 
i)cleanup these directories:
ii) ensure mongoDB service is running in your PC ( search for "services" in start and find mongoDB)

1) execute this command:
cd C:\kafka\kafka_2.13-3.9.1\
.\bin\windows\kafka-storage.bat format --standalone -t 4n2aTYG0Tn-ike55mt7i3Q -c config\kraft\server.properties

2) start kafka broker(server) with this command:
cd C:\kafka\kafka_2.13-3.9.1\
.\bin\windows\kafka-server-start.bat config\kraft\server.properties

3) start kafka connect worker , using this command:
cd C:\kafka\kafka_2.13-3.9.1\
.\bin\windows\connect-standalone.bat config\connect-standalone.properties connector_config\MongoSourceConnector.json

4) trigger code file: kafka_consumer.py

5) trigger code file: spark_file_reader.py

6) add/ modify records in transaction collection, of stockmarket DB in mongoDB, aggregated changes will reflect in website , after a  batch process, which happens every 1 min: 