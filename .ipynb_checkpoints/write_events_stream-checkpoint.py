#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

import pyspark.sql.functions as F
from datetime import datetime
from datetime import date


def purchase_events_schema():
    """
    @function: This function provides the table schema for purchase events (knife, sword, shield)
    @param: None 
    @return: Returns the table schema for purchase events
    """  
    return StructType(
    [
        StructField('Accept', StringType(), True),
        StructField('Host', StringType(), True),
        StructField('User-Agent', StringType(), True),
        StructField('price', LongType(), True),
        StructField('n_purchased', LongType(), True),
        StructField('strength', LongType(), True),
        StructField('name', StringType(), True),
        StructField('event_type', StringType(), True),
        StructField('userid', StringType(), True)
    ]
)

# LongType()

def guild_events_schema():
    """
    @function: This function provides the table schema for  guild events
    @param: None 
    @return: Returns the table schema for guild events 
    """  
    return StructType(
    [
        StructField('Accept', StringType(), True),
        StructField('Host', StringType(), True),
        StructField('User-Agent', StringType(), True),
        StructField('price', LongType(), True),
        StructField('n_purchased', LongType(), True),
        StructField('strength', LongType(), True),
        StructField('name', StringType(), True),
        StructField('event_type', StringType(), True),
        StructField('userid', StringType(), True)
    ]
)

def fight_events_schema():
    """
    @function: This function provides the table schema for  fight events
    @param: None 
    @return: Returns the table schema for fight events 
    """  
    return StructType(
    [
        StructField('Accept', StringType(), True),
        StructField('Host', StringType(), True),
        StructField('User-Agent', StringType(), True),
        StructField('score', LongType(), True),
        StructField('win_status', StringType(), True),
        StructField('event_type', StringType(), True),
        StructField('userid', StringType(), True)
    ]
)


@udf('boolean')
def is_purchase(event_as_json):
    """
    @function: This function uses a json to filter out records by purchase event type (knife, sword, shield)
    @param: Takes in extracted json data as a string
    @return: Returns a boolean value
    """    
    event = json.loads(event_as_json)
    if 'purchase' in event['event_type']:
        return True
    return False

@udf('boolean')
def is_join_guild(event_as_json):
    """
    @function: This function uses a json to filter out records by guild event type
    @param: Takes in extracted json data as a string
    @return: Returns a boolean value
    """   
    event = json.loads(event_as_json)
    if event['event_type'] == 'join_guild':
        return True
    return False

@udf('boolean')
def is_fight_event(event_as_json):
    """
    @function: This function uses a json to filter out records by fight event type
    @param: Takes in extracted json data as a string
    @return: Returns a boolean value
    """   
    event = json.loads(event_as_json)
    if event['event_type'] == 'fight_event':
        return True
    return False

def main():

    """
    @main function: This is a main function that executes a spark job - extracting string, parsing json using a provided schema, etc.
    @param: none, uses previously defined functions
    @return: none, lands tables via streaming on HDFS
    """ 
    
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .load()

    purchases = raw_events \
        .filter(is_purchase(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          purchase_events_schema()).alias('json')) \
        .select('timestamp', 'json.*') \
        .select( \
#                   F.from_utc_timestamp(F.col('timestamp'),'GMT').alias('event_ts') \
                  F.col('timestamp').alias('event_ts') \
                 ,F.col('userid') \
                 ,F.col('Host') \
                 ,F.col('event_type') \
                 ,F.col('name') \
                 ,F.col('strength') \
                 ,F.col('n_purchased') \
                 ,F.col('price') \
                ) \
        .distinct()
    
    guild = raw_events \
        .filter(is_join_guild(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          guild_events_schema()).alias('json')) \
        .select('timestamp', 'json.*') \
        .select( \
#                   F.from_utc_timestamp(F.col('timestamp'),'GMT').alias('event_ts') \
                  F.col('timestamp').alias('event_ts') \
                 ,F.col('userid') \
                 ,F.col('Host') \
                 ,F.col('event_type') \
                 ,F.col('name') \
                 ,F.col('strength') \
                 ,F.col('n_purchased') \
                 ,F.col('price') \
                ) \
        .distinct()

    fight = raw_events \
        .filter(is_fight_event(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          fight_events_schema()).alias('json')) \
        .select('timestamp', 'json.*') \
        .select( \
                  F.col('timestamp').alias('event_ts') \
                 ,F.col('userid') \
                 ,F.col('Host') \
                 ,F.col('event_type') \
                 ,F.col('score') \
                 ,F.col('win_status') \
                ) \
        .distinct()
    
    
    sink_purchases = purchases \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_purchase_events") \
        .option("path", "/tmp/purchase_events") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    sink_guild = guild \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_guild_events") \
        .option("path", "/tmp/guild_events") \
        .trigger(processingTime="10 seconds") \
        .start()

    sink_fight = fight \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_fight_events") \
        .option("path", "/tmp/fight_events") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    sink_purchases.awaitTermination()
    sink_guild.awaitTermination()
    sink_fight.awaitTermination()


if __name__ == "__main__":
    main()
