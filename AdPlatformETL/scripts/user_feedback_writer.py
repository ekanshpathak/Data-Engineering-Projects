import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def readFeedBack(kafka_host,kafka_port,kafka_topic):
    spark = SparkSession  \
        .builder  \
        .appName("AA")  \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

#### Read data from Kafka Topic ####
    kafka_bootstrap_server = kafka_host+':'+kafka_port
    kafka_msg = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers",kafka_bootstrap_server)  \
        .option("subscribe",kafka_topic)  \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("stopGracefullyOnShutdown","true") \
        .load()
        
    kafka_msg.printSchema()
#### Define the Schema for the Data ####
    schema = StructType(
        [
            StructField("campaign_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("request_id", StringType(), True),
            StructField("click", IntegerType(), True),
            StructField("view", IntegerType(), True),
            StructField("acquisition",IntegerType(), True),
            StructField("auction_cpm",DoubleType(),True),
            StructField("auction_cpc", DoubleType(), True),
            StructField("auction_cpa", DoubleType(), True),
            StructField("target_age_range", StringType(), True),
            StructField("target_location", StringType(), True),
            StructField("target_gender", StringType(), True),
            StructField("target_income_bucket", StringType(), True),
            StructField("target_device_type", StringType(), True),
            StructField("campaign_start_time", StringType(), True),
            StructField("campaign_end_time",StringType(),True),
            StructField("action",StringType(),True),
            StructField("expenditure",DoubleType(),True),
            StructField("timestamp",TimestampType(),True)
        ]
      )

#### Casting the value from Message to String and select different Columns ####
    ads_df = kafka_msg.selectExpr("CAST(value AS STRING) as value") \
      .select(from_json("value",schema).alias("df")) \
      .select(
                       "df.campaign_id",
                       "df.user_id",
                       "df.request_id",
                       "df.click",
                       "df.view",
                       "df.acquisition",
                       "df.auction_cpm",
                       "df.auction_cpc",
                       "df.auction_cpa",
                       "df.target_age_range",
                       "df.target_location",
                       "df.target_gender",
                       "df.target_income_bucket",
                       "df.target_device_type",
                       "df.campaign_start_time",
                       "df.campaign_end_time",
                       "df.action",
                       "df.expenditure",
                       "df.timestamp")
    
#### Writing the data to Console ####
    ads_console = ads_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .start()

#### Writing the data to a CSV File ####
    ads_csv = ads_df.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("format", "append") \
        .option("truncate", "false") \
        .option("path", "capstone/feedback") \
        .option("checkpointLocation", "ad-platform-project/checkpoint") \
        .trigger(processingTime="1 minute") \
        .start()
        
#### Terminating both Write Streams of Console & CSV ####
    ads_console.awaitTermination()
    ads_csv.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Not enough parameters")
        exit(-1)
    
    kafka_host = sys.argv[1]
    kafka_port = sys.argv[2]
    kafka_topic = sys.argv[3]
    try:
        readFeedBack(kafka_host,kafka_port,kafka_topic) 
    except KeyboardInterrupt:
       print('KeyboardInterrupt, exiting...')        
    

