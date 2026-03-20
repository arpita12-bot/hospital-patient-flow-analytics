from pyspark.sql.functions import *
from pyspark.sql.types import *

#Azure Event Hub Configurations
event_hub_namespace = "<<NAMESPACE_HOSTNAME>>"
event_hub_name = "<<EVENT_HUB_NAME>>"
event_hub_conn_str = "<<NAMESPACE_CONNECTION_STRING>>" 



kafka_options = {
    'kafka.bootstrap.servers': f'{event_hub_namespace}.servicebus.windows.net:9093',
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required          username="$ConnectionString" password="{event_hub_conn_str}";', 
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

#Read from eventhub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
        )


#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

#ADLS configuration
spark.conf.set(
    "fs.azure.account.key.<<storageaccount_name>>.dfs.core.windows.net",
    "<<storage_account_accesskey>>  
)

# Connection to storage account (Bronze)
bronze_path = "abfss://<<container>>@<<storageaccount_name>>.dfs.core.windows.net/<<path>>"


# Write stream to bronze (we always use checkpoint when we read/write stream)
# Due to some reason, if system got stuck then we have to rerun again and when we rerun the code . But we don't want the code to process the past data again. So we have to use checkpoint(so that whenever system starts it won't process the past data again, it will start from the checkpoint where it stopped)
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation","abfss://bronze@hospitalstorageacct.dfs.core.windows.net/_checkpoints/patient_flow")
    .start(bronze_path)
)