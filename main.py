import json
import os
import vertezml as vz
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import DataFrame

load_dotenv()

kafka_broker = os.getenv("KAFKA_BROKER", "broker:9092")
kafka_producer = KafkaProducer(bootstrap_servers=[kafka_broker], value_serialize=lambda x: json.dumps(x).encode("utf-8"))

# Data Pre-processing: Date for InvoiceDate
read_dat = pd.read_csv("./data/data.csv", encoding="latin1")
read_dat["InvoiceDate"] = pd.to_datetime(read_dat["InvoiceDate"])
read_dat["InvoiceDate"] = read_dat["InvoiceDate"].dt.date
print(read_dat.head())

kafka_send = kafka_producer.send("ecommerce_etl", value=read_dat)

# Init PySpark App
spark = SparkSession.builder.appName("Real time Ecommerce ETL").getOrCreate()
spark.sparkContext.setLogLevel("Error")

# Set kafka configuration
kafka_bootstrap = "broker1:29092,broker2:29094"

# Spark Schema
dataSchema = StructType([
    StructField("InvoiceNo", IntegerType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(),True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

# Stream from kafka to spark
dataDf = (spark.readStream.format("kafka").option("kafka.bootstrap_servers", kafka_bootstrap).option("subscribe", "ecommerce_etl").option("startingOffsets", "earliest").load().selectExpr("CAST(value AS STRING)").select(from_json("value", dataSchema).alias("data")).select("data.*"))
get_last_date = read_dat.agg(max(col("InvoiceDate")).alias("last_invoice_date"))
last_date = get_last_date.collect()[0]["last_invoice_date"]
# Spark analysis for finding last invoice date
dataDf = dataDf.withWatermark("InvoiceDate", f"{last_date} hours")
dataAnalysisDF = (dataDf.groupby(window(col("InvoiceDate"), f"{last_date}")))


# write data from spark to elasticsearch
def write_dat_elastic(df, index_name):
    def write_logs(batch: DataFrame, batch_id: int):
        try:
            if not batch.isEmpty():
                batch.write.format("org.elasticsearch.spark.sql").option("checkpointLocation", f"/opt/bitnami/spark/checkpoint/{index_name}/{batch_id}").option("es.resource", f"{index_name}/doc").option("es.nodes","elasticsearch").option("es.port","9200").option("es.nodes.wan.only", "true").save()
            else:
                print(f"Batch {batch_id} is empty")
        except Exception as e:
            print(f"Error writing to Elasticsearch: {e}")

    return df.writeStream.outputMode("append").foreachBatch(write_logs).start()


write_dat_elastic(dataAnalysisDF, "ecommerce_etl_analysis")
spark.streams.awaitAnyTermination()





