# Real Time Ecommerce ETL Pipeline
- Streaming dataset to kafka cluster
- kafka clusters (Data streaming): Broker 1 and Broker 2
- Broadcast data from brokers to spark (Data Processing)
- Establish streamline pipeline between Spark and Elasticsearch (Data Storage)
- Visualize data from Elasticsearch in Kibana (Data Visualization)

## Kafka:
- We are gonna use 2 brokers to manage high throughput data streaming

## Spark:
- We are gonna use spark for data preprocessing and analysis

## Elasticsearch:
- We are gonna use elasticsearch to index, store, and retrieve data for analysis

## Kibana:
- We are gonna use Kibana for visualize the data from Elasticsearch into an actinable insights

## Docker: 
- We are gonna use Docker for orchestrating data stream from kafka to spark to elasticsearch.
- We will isolate the whole ETL pipeline in an isolated container.