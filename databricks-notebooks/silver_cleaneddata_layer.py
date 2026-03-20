from pyspark.sql.types import *
from pyspark.sql.functions import *


#ADLS configuration
spark.conf.set(
    "fs.azure.account.key.<<storageaccount_name>>.dfs.core.windows.net",
    "<<storage_account_accesskey>>  
)

bronze_path = "abfss://<<container>>@<<storageaccount_name>>.dfs.core.windows.net/<path>"
silver_path = "abfss://<<container>>@<<storageaccount_name>>.dfs.core.windows.net/patient_flow"

# Read from bronze
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# Define Schema

schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", LongType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", LongType()),
    StructField("hospital_id", LongType())
])

# Parse it to dataframe

parsed_df = bronze_df.withColumn("data", from_json(col("raw_json"), schema)).select("data.*")

# Convert type to Timestamp

clean_df = parsed_df.withColumn("admission_time", to_timestamp("admission_time"))
clean_df = clean_df.withColumn("discharge_time", to_timestamp("discharge_time"))

# Invalid admission_times

clean_df = clean_df.withColumn(
    "admission_time",
    when(
        col("admission_time").isNull() | (col("admission_time") > current_timestamp()),
        current_timestamp()
    ).otherwise(col("admission_time"))
)

# Handle Invalid Age
clean_df = clean_df.withColumn(
    "age",
    when(col("age") > 100, floor(rand() * 90 + 1).cast("long"))
    .otherwise(col("age"))
)

# Schema evolution
expected_cols = ["patient_id", "gender", "age", "department", "admission_time", "discharge_time", "bed_id", "hospital_id"]

# Fix: schema is a StructType, not a dict, so use .fieldNames() and .fields for type lookup
schema_field_types = {field.name: field.dataType for field in schema.fields}

for col_name in expected_cols:
    if col_name not in clean_df.columns:
        clean_df = clean_df.withColumn(col_name, lit(None).cast(schema_field_types[col_name]))

(
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", silver_path + "/_checkpoint")
    .start(silver_path)
)