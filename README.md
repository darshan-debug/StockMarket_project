intro:

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