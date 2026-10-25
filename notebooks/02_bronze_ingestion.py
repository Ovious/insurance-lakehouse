# Databricks notebook source
# MAGIC %md
# MAGIC ## Starting off with the foundation for the data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

data = [
    ("CLM001", "POL001", 1500.00, "OPEN"),
    ("CLM002", "POL002", 2500.00, "CLOSED"),
    ("CLM003", "POL001", 750.00, "OPEN"),
    ("CLM004", "POL003", 3200.00, "PENDING"),
    ("CLM005", "POL004", 1000.00, "OPEN")
]


columns = ["claim_id", "policy_id", "claim_amount", "status"]

df = spark.createDataFrame(data, columns)


bronze_df = (
    df
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_system", F.lit("claims_api"))
    .withColumn("batch_id", F.lit("batch_001"))
)

display(bronze_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Moving to saving the dataframe onto my Catalog

# COMMAND ----------

catalog = "insurance_company"
schema = "insurance_bronze"
table = "claims"

table_name = f"{catalog}.{schema}.{table}"

"""
# Following part was used only once, while putting initial data into the table
# Future updates on the bronze table shall be an appned to the table
(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)
"""

# COMMAND ----------

#spark.table(table_name).show()

spark.sql(f"DESCRIBE HISTORY {table_name}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step is gonna be a simple validation, trying to see if there are any incorrect rows of data

# COMMAND ----------

invalid_df = bronze_df.filter(
    F.col("claim_id").isNull() |
    F.col("claim_amount").isNull() |
    (F.col("claim_amount") <= 0)
)

display(invalid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding this time incorrect data on purpose to test out the validation step against it

# COMMAND ----------

incorrect_data = [
    ("CLM00X", "POL00X", 1500.00, "OPEN"),
    ("CLM007", "POL007", 0.0, "CLOSED"),
    ("CLM008", "POL008", -750.00, "OPEN")
]

incorrect_df = spark.createDataFrame(
    incorrect_data, 
    columns
)

bronze_df = bronze_df.unionByName(
    incorrect_df
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_system", F.lit("claims_api"))
    .withColumn("batch_id", F.lit("batch_001")))

display(bronze_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Test of different approach where, during creation of dataframe, it's schema is properly described

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("policy_id", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

incorrect_data = [
    (None, "POL00X", 1500.00, "OPEN"),
    ("CLM007", "POL007", 0.0, "CLOSED"),
    ("CLM008", "POL008", -750.00, "OPEN")
]

incorrect_df = spark.createDataFrame(
    incorrect_data,
    schema=schema
)

#incorrect_df.printSchema()
display(incorrect_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update bronze data for the sake of consistency with exercises in 03_pyspark_transformations
# MAGIC This operation is supposed to reflect a real life scenario of another batch coming to Databricks, providing updates and wrong data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("policy_id", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

batch_002_data = [
    ("CLM001", "POL001", 1800.0, "PENDING"),
    ("CLM003", "POL001", 900.0, "CLOSED"),
    ("CLM006", "POL999", 1200.0, "OPEN"),

    # Invalid records for data quality exercises
    ("CLM001", "POL001", -1800.0, "PENDING"),
    ("CLM005", "POL004", 900.0, "CANC")
]

batch_002_df = spark.createDataFrame(
    batch_002_data,
    schema=schema
)

bronze_batch_002 = (
    batch_002_df
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_system", F.lit("claims_api"))
    .withColumn("batch_id", F.lit("batch_002"))
)


table_name = "insurance_company.insurance_bronze.claims"
"""
# Second batch of data, created during day 2 tests
# In the future it is gonna be transformed into possibly automated batches of new data
(
    bronze_batch_002.write
    .format("delta")
    .mode("append")
    .saveAsTable(table_name)
)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updating the schema and values, to take in approach of having source_event_timestamp as a part of Bronze data, that comes with batches

# COMMAND ----------

table_name = "insurance_company.insurance_bronze.claims"

'''
spark.sql(f"""
    ALTER TABLE {table_name}
    ADD COLUMNS (
        source_event_timestamp TIMESTAMP
    )
""")
'''

spark.sql(f"""
    UPDATE {table_name}
    SET source_event_timestamp =
        CASE claim_id
            WHEN 'CLM001' THEN TIMESTAMP '2026-10-20 10:00:00'
            WHEN 'CLM002' THEN TIMESTAMP '2026-10-20 11:00:00'
            WHEN 'CLM003' THEN TIMESTAMP '2026-10-21 09:00:00'
            WHEN 'CLM004' THEN TIMESTAMP '2026-10-21 12:00:00'
            WHEN 'CLM005' THEN TIMESTAMP '2026-10-22 08:00:00'
        END
    WHERE batch_id = 'batch_001'
""")


spark.sql("""
    UPDATE insurance_company.insurance_bronze.claims
    SET source_event_timestamp =
        CASE
            WHEN claim_id = 'CLM001' AND claim_amount = 1800.0
                THEN TIMESTAMP '2026-10-22 14:00:00'
            WHEN claim_id = 'CLM001' AND claim_amount = -1800.0
                THEN TIMESTAMP '2026-10-22 09:00:00'
            WHEN claim_id = 'CLM003'
                THEN TIMESTAMP '2026-10-23 10:00:00'
            WHEN claim_id = 'CLM006'
                THEN TIMESTAMP '2026-10-23 11:00:00'
            WHEN claim_id = 'CLM005' AND status = 'CANC'
                THEN TIMESTAMP '2026-10-21 19:00:00'
            ELSE source_event_timestamp
        END
    WHERE batch_id = 'batch_002'
""")

display(
    spark.sql("""
        SELECT
            claim_id,
            claim_amount,
            status,
            source_event_timestamp,
            batch_id
        FROM insurance_company.insurance_bronze.claims
        WHERE batch_id = 'batch_002'
        ORDER BY source_event_timestamp
    """)
)