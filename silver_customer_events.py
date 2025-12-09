# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Customer Events Enriched
# MAGIC 
# MAGIC **Purpose:** Flatten and enrich customer events from offer_silver_clean with:
# MAGIC - User identity resolution
# MAGIC - Event classification
# MAGIC - Struct flattening
# MAGIC - Temporal enrichment
# MAGIC - Data quality scoring
# MAGIC 
# MAGIC **Source:** offer_silver_clean
# MAGIC **Target:** silver_customer_events_enriched
# MAGIC **Schedule:** Every 30 minutes (aligned with source ingestion)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import hashlib

# Configuration
SOURCE_TABLE = "centraldata_prod.minion_event.offer_silver_clean"
TARGET_TABLE = "centraldata_sandbox.minion.silver_customer_events_enriched"
CHECKPOINT_TABLE = "silver_customer_events_watermark"


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define Helper Functions

# COMMAND ----------

# UDF for generating event primary key
def generate_event_pk(offer_trace_id, timestamp):
    """Generate unique event identifier using SHA256"""
    if offer_trace_id and timestamp:
        composite = f"{offer_trace_id}_{timestamp}"
        return hashlib.sha256(composite.encode()).hexdigest()
    return None

generate_event_pk_udf = F.udf(generate_event_pk, StringType())

# UDF for data quality scoring
def calculate_data_quality_score(row_dict):
    """Calculate data quality score based on field completeness"""
    critical_fields = [
        'customer_key', 'event_timestamp', 'campaign_id', 
        'device_type', 'event_type'
    ]
    important_fields = [
        'advertiser_id', 'creative_id', 'country', 'browser_name'
    ]
    
    critical_score = sum(1 for f in critical_fields if row_dict.get(f) is not None) / len(critical_fields)
    important_score = sum(1 for f in important_fields if row_dict.get(f) is not None) / len(important_fields)
    
    # Weighted score: 70% critical, 30% important
    return (critical_score * 0.7) + (important_score * 0.3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Get Watermark for Incremental Processing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create watermark table if not exists
# MAGIC CREATE TABLE IF NOT EXISTS silver_customer_events_watermark (
# MAGIC   table_name STRING,
# MAGIC   last_processed_timestamp TIMESTAMP,
# MAGIC   last_processed_date DATE,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# Get last processed timestamp
watermark_df = spark.sql(f"""
    SELECT last_processed_timestamp 
    FROM {CHECKPOINT_TABLE} 
    WHERE table_name = '{TARGET_TABLE}'
""")

if watermark_df.count() > 0:
    last_watermark = watermark_df.collect()[0]['last_processed_timestamp']
else:
    # First run - process last 7 days
    last_watermark = datetime.now() - timedelta(days=7)

print(f"Processing events since: {last_watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read Source Data with Incremental Filter

# COMMAND ----------

source_df = spark.sql(f"""
    SELECT *
    FROM {SOURCE_TABLE}
    WHERE createDate >= '{last_watermark}'
        AND _corrupt_record IS NULL
        AND offerTraceId IS NOT NULL
    ORDER BY createDate
""")

print(f"Source records to process: {source_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. User Identity Resolution

# COMMAND ----------

# Resolve customer identity from multiple sources
events_with_identity = source_df.withColumn(
    "customer_key",
    F.coalesce(
        F.col("profile.id"),
        F.col("fluentId"),
        F.col("emailSha256"),
        F.concat(F.lit("ANON_"), F.col("sessionId"))  # Fallback for anonymous
    )
).withColumn(
    "fluent_id",
    F.col("fluentId")
).withColumn(
    "profile_id", 
    F.col("profile.id")
).withColumn(
    "email_sha256",
    F.col("emailSha256")
).withColumn(
    "email_md5",
    F.col("emailMd5")
).withColumn(
    "device_advertising_id",
    F.col("device.advertisingId")
).withColumn(
    "is_identified_user",
    F.when(F.col("profile.id").isNotNull(), True).otherwise(False)
).withColumn(
    "is_anonymous_user",
    F.when(F.col("profile.id").isNull(), True).otherwise(False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Event Classification and Typing

# COMMAND ----------

events_classified = events_with_identity.withColumn(
    "event_type",
    F.when(F.col("campaignData.conversions").isNotNull(), "conversion")
     .when(F.col("campaignData.orderId").isNotNull(), "transaction")
     .when(F.col("campaignData.clickId").isNotNull(), "click")
     .otherwise("view")
).withColumn(
    "product_scope",
    F.coalesce(F.col("productScope"), F.lit("unknown"))
).withColumn(
    "is_p1_view",
    F.when(F.col("campaignData.position") == 1, True).otherwise(False)
).withColumn(
    "is_conversion_event",
    F.when(F.col("campaignData.conversions").isNotNull(), True).otherwise(False)
).withColumn(
    "is_transaction_event",
    F.when(F.col("campaignData.orderId").isNotNull(), True).otherwise(False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Flatten Campaign Data Structures

# COMMAND ----------

events_campaign_flattened = events_classified.withColumn(
    "advertiser_id",
    F.col("campaignData.advertiserId")
).withColumn(
    "advertiser_name",
    F.col("campaignData.advertiserName")
).withColumn(
    "campaign_id",
    F.col("campaignData.campaignId")
).withColumn(
    "campaign_name",
    F.col("campaignData.campaignName")
).withColumn(
    "campaign_type",
    F.col("campaignData.campaignType")
).withColumn(
    "campaign_vertical",
    F.col("campaignData.vertical")
).withColumn(
    "campaign_sub_vertical",
    F.col("campaignData.subVertical")
).withColumn(
    "adgroup_id",
    F.col("campaignData.adgroupId")
).withColumn(
    "adgroup_name",
    F.col("campaignData.adgroupName")
).withColumn(
    "creative_id",
    F.col("campaignData.creativeId")
).withColumn(
    "creative_name",
    F.col("campaignData.creativeName")
).withColumn(
    "creative_type",
    F.col("campaignData.creativeType")
).withColumn(
    "tracking_id",
    F.col("campaignData.trackingId")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Flatten Conversion and Transaction Data

# COMMAND ----------

# Extract first conversion from array if exists
events_conversion_flattened = events_campaign_flattened.withColumn(
    "conversion_array",
    F.col("campaignData.conversions")
).withColumn(
    "conversion_type",
    F.when(F.size(F.col("conversion_array")) > 0, 
           F.col("conversion_array")[0].getField("conversionType"))
     .otherwise(None)
).withColumn(
    "conversion_goal_id",
    F.when(F.size(F.col("conversion_array")) > 0,
           F.col("conversion_array")[0].getField("conversionGoalId"))
     .otherwise(None)
).withColumn(
    "order_id",
    F.coalesce(
        F.col("campaignData.orderId"),
        F.when(F.size(F.col("conversion_array")) > 0,
               F.col("conversion_array")[0].getField("orderId"))
         .otherwise(None)
    )
).withColumn(
    "transaction_id",
    F.col("campaignData.transactionId")
).withColumn(
    "transaction_value",
    F.col("campaignData.transactionValue").cast(DecimalType(19,4))
).withColumn(
    "revenue",
    F.col("campaignData.revenue").cast(DecimalType(19,4))
).withColumn(
    "gross_revenue",
    F.col("campaignData.grossRevenue").cast(DecimalType(19,4))
).withColumn(
    "adjusted_revenue",
    F.col("campaignData.adjustedRevenue").cast(DecimalType(19,4))
).withColumn(
    "sale_amount",
    F.col("campaignData.saleAmount").cast(DecimalType(19,4))
).withColumn(
    "currency",
    F.col("campaignData.currency")
).withColumn(
    "conversion_days_to_complete",
    F.when(F.size(F.col("conversion_array")) > 0,
           F.col("conversion_array")[0].getField("daysToComplete"))
     .otherwise(None)
).drop("conversion_array")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Flatten Device and Context Data

# COMMAND ----------

events_device_flattened = events_conversion_flattened.withColumn(
    "device_type",
    F.col("device.inferredDeviceType")
).withColumn(
    "device_os",
    F.col("device.osName")
).withColumn(
    "device_os_version",
    F.col("device.osVersion")
).withColumn(
    "device_brand",
    F.col("device.brandName")
).withColumn(
    "device_model",
    F.col("device.deviceModel")
).withColumn(
    "browser_name",
    F.col("device.clientName")
).withColumn(
    "browser_version",
    F.col("device.clientVersion")
).withColumn(
    "is_mobile",
    F.when(F.lower(F.col("device.inferredDeviceType")).isin(["smartphone", "mobile"]), True).otherwise(False)
).withColumn(
    "is_tablet",
    F.when(F.lower(F.col("device.inferredDeviceType")) == "tablet", True).otherwise(False)
).withColumn(
    "is_desktop",
    F.when(F.lower(F.col("device.inferredDeviceType")).isin(["desktop", "pc"]), True).otherwise(False)
).withColumn(
    "is_bot",
    F.coalesce(F.col("device.isBot"), F.lit(False))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Flatten Geographic Data

# COMMAND ----------

events_geo_flattened = events_device_flattened.withColumn(
    "country",
    F.coalesce(
        F.col("profile.address.country"),
        F.col("device.countryCode"),
        F.col("geo.country")
    )
).withColumn(
    "state",
    F.coalesce(
        F.col("profile.address.state"),
        F.col("geo.state")
    )
).withColumn(
    "city",
    F.coalesce(
        F.col("profile.address.city"),
        F.col("geo.city")
    )
).withColumn(
    "zip",
    F.coalesce(
        F.col("profile.address.zip"),
        F.col("geo.postalCode")
    )
).withColumn(
    "ip_resolved_country",
    F.col("device.countryCode")
).withColumn(
    "local_time_offset",
    F.col("device.localTimeOffset")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Flatten Traffic Source Data

# COMMAND ----------

events_traffic_flattened = events_geo_flattened.withColumn(
    "partner_id",
    F.col("trafficSource.partnerId")
).withColumn(
    "partner_name",
    F.col("trafficSource.partnerName")
).withColumn(
    "source_id",
    F.col("trafficSource.sourceId")
).withColumn(
    "traffic_partner_type",
    F.col("trafficSource.trafficPartnerType")
).withColumn(
    "subaff1",
    F.col("trafficSource.subAff1")
).withColumn(
    "subaff2",
    F.col("trafficSource.subAff2")
).withColumn(
    "subaff3",
    F.col("trafficSource.subAff3")
).withColumn(
    "subaff4",
    F.col("trafficSource.subAff4")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Temporal Enrichment

# COMMAND ----------

# Convert timestamps to EST and extract temporal attributes
events_temporal_enriched = events_traffic_flattened.withColumn(
    "event_timestamp",
    F.col("timestamp").cast(TimestampType())
).withColumn(
    "event_timestamp_est",
    F.from_utc_timestamp(F.col("event_timestamp"), "America/New_York")
).withColumn(
    "event_date_est",
    F.to_date(F.col("event_timestamp_est"))
).withColumn(
    "event_hour_est",
    F.hour(F.col("event_timestamp_est"))
).withColumn(
    "local_hour_of_day",
    F.when(F.col("device.localTimeOffset").isNotNull(),
           F.hour(F.col("event_timestamp") + F.expr("INTERVAL " + F.col("device.localTimeOffset") + " HOURS")))
     .otherwise(F.col("event_hour_est"))
).withColumn(
    "day_of_week",
    F.date_format(F.col("event_date_est"), "EEEE")
).withColumn(
    "is_business_hours",
    F.when((F.col("event_hour_est") >= 9) & (F.col("event_hour_est") < 17), True).otherwise(False)
).withColumn(
    "is_weekend",
    F.when(F.dayofweek(F.col("event_date_est")).isin([1, 7]), True).otherwise(False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Extract Profile Attributes

# COMMAND ----------

events_profile_enriched = events_temporal_enriched.withColumn(
    "profile_gender",
    F.col("profile.gender")
).withColumn(
    "profile_home_owner",
    F.col("profile.homeOwner")
).withColumn(
    "profile_has_children",
    F.when(F.col("profile.hasChildren") == "true", True)
     .when(F.col("profile.hasChildren") == "false", False)
     .otherwise(None)
).withColumn(
    "profile_owns_car",
    F.col("profile.ownsCar")
).withColumn(
    "profile_first_visit",
    F.col("profile.firstVisit").cast(TimestampType())
).withColumn(
    "profile_last_visit",
    F.col("profile.lastVisit").cast(TimestampType())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Add Data Quality Scoring

# COMMAND ----------

# Add data quality flags
events_quality_scored = events_profile_enriched.withColumn(
    "has_corrupt_record",
    F.when(F.col("_corrupt_record").isNotNull(), True).otherwise(False)
).withColumn(
    "referer",
    F.col("referer")
).withColumn(
    "user_agent",
    F.col("userAgent")
).withColumn(
    "session_id",
    F.col("sessionId")
).withColumn(
    "source_reference_id",
    F.col("sourceReferenceId")
).withColumn(
    "source_reference",
    F.col("sourceReference")
)

# Calculate completeness-based quality score
events_quality_scored = events_quality_scored.withColumn(
    "data_quality_score",
    # Critical fields check (customer, timestamp, campaign, device)
    (F.when(F.col("customer_key").isNotNull(), 0.25).otherwise(0.0) +
     F.when(F.col("event_timestamp").isNotNull(), 0.25).otherwise(0.0) +
     F.when(F.col("campaign_id").isNotNull(), 0.20).otherwise(0.0) +
     F.when(F.col("device_type").isNotNull(), 0.15).otherwise(0.0) +
     # Important fields check (advertiser, creative, geo, browser)
     F.when(F.col("advertiser_id").isNotNull(), 0.05).otherwise(0.0) +
     F.when(F.col("creative_id").isNotNull(), 0.05).otherwise(0.0) +
     F.when(F.col("country").isNotNull(), 0.03).otherwise(0.0) +
     F.when(F.col("browser_name").isNotNull(), 0.02).otherwise(0.0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Generate Event Primary Key and Deduplication

# COMMAND ----------

events_with_pk = events_quality_scored.withColumn(
    "event_pk",
    F.sha2(F.concat_ws("_", F.col("offerTraceId"), F.col("timestamp")), 256)
).withColumn(
    "offer_trace_id",
    F.col("offerTraceId")
)

# Flag duplicates using window function
window_dup = Window.partitionBy("event_pk").orderBy(F.col("createDate").desc())

events_deduped = events_with_pk.withColumn(
    "row_num",
    F.row_number().over(window_dup)
).withColumn(
    "is_duplicate",
    F.when(F.col("row_num") > 1, True).otherwise(False)
).drop("row_num")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Add Metadata and Select Final Schema

# COMMAND ----------

events_final = events_deduped.withColumn(
    "source_file",
    F.input_file_name()
).withColumn(
    "ingestion_timestamp",
    F.col("createDate")
).withColumn(
    "processing_timestamp",
    F.current_timestamp()
).withColumn(
    "silver_load_date",
    F.current_date()
).withColumn(
    "date_est",  # Partition column
    F.col("event_date_est")
).withColumn(
    "hour_est",  # Partition column
    F.col("event_hour_est")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Apply Data Quality Filters

# COMMAND ----------

# Filter out low quality and invalid records
events_filtered = events_final.filter(
    (F.col("is_bot") == False) &  # Remove bot traffic
    (F.col("data_quality_score") >= 0.5) &  # Minimum quality threshold
    (F.col("event_timestamp") >= "2020-01-01") &  # Valid date range
    (F.col("event_timestamp") <= F.current_timestamp()) &  # Not future dates
    (~F.lower(F.coalesce(F.col("campaign_name"), F.lit(""))).like("%test%"))  # Remove test campaigns
)

print(f"Records after quality filters: {events_filtered.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Select Final Schema for Target Table

# COMMAND ----------

silver_events_schema = events_filtered.select(
    # Primary Keys & Identifiers
    "event_pk",
    "offer_trace_id",
    "session_id",
    "source_reference_id",
    "source_reference",
    
    # User Identity
    "customer_key",
    "fluent_id",
    "profile_id",
    "email_sha256",
    "email_md5",
    "device_advertising_id",
    "is_identified_user",
    "is_anonymous_user",
    
    # Timestamps
    "event_timestamp",
    "event_date_est",
    "event_hour_est",
    "local_hour_of_day",
    "day_of_week",
    "is_business_hours",
    "is_weekend",
    
    # Event Classification
    "event_type",
    "product_scope",
    "is_p1_view",
    "is_conversion_event",
    "is_transaction_event",
    
    # Campaign Dimensions
    "advertiser_id",
    "advertiser_name",
    "campaign_id",
    "campaign_name",
    "campaign_type",
    "campaign_vertical",
    "campaign_sub_vertical",
    "adgroup_id",
    "adgroup_name",
    "creative_id",
    "creative_name",
    "creative_type",
    "tracking_id",
    
    # Conversion Data
    "conversion_type",
    "conversion_goal_id",
    "order_id",
    "transaction_id",
    "transaction_value",
    "revenue",
    "gross_revenue",
    "adjusted_revenue",
    "sale_amount",
    "currency",
    "conversion_days_to_complete",
    
    # Device & Context
    "device_type",
    "device_os",
    "device_os_version",
    "device_brand",
    "device_model",
    "browser_name",
    "browser_version",
    "is_mobile",
    "is_tablet",
    "is_desktop",
    "is_bot",
    
    # Geographic Data
    "country",
    "state",
    "city",
    "zip",
    "ip_resolved_country",
    "local_time_offset",
    
    # Traffic Source
    "partner_id",
    "partner_name",
    "source_id",
    "traffic_partner_type",
    "subaff1",
    "subaff2",
    "subaff3",
    "subaff4",
    
    # Session Context
    "referer",
    "user_agent",
    
    # Profile Attributes
    "profile_gender",
    "profile_home_owner",
    "profile_has_children",
    "profile_owns_car",
    "profile_first_visit",
    "profile_last_visit",
    
    # Data Quality
    "has_corrupt_record",
    "is_duplicate",
    "data_quality_score",
    
    # Metadata
    "source_file",
    "ingestion_timestamp",
    "processing_timestamp",
    "silver_load_date",
    
    # Partitioning
    "date_est",
    "hour_est"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19. Write to Silver Table with MERGE

# COMMAND ----------

# Check if target table exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

if not target_exists:
    # First time - create table
    print(f"Creating new table: {TARGET_TABLE}")
    
    silver_events_schema.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("date_est", "hour_est") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TARGET_TABLE)
    
    print(f"Table created successfully with {silver_events_schema.count()} records")
else:
    # Incremental MERGE
    print(f"Performing incremental MERGE into {TARGET_TABLE}")
    
    # Register temp view for merge
    silver_events_schema.createOrReplaceTempView("silver_events_updates")
    
    # MERGE statement
    merge_sql = f"""
    MERGE INTO {TARGET_TABLE} target
    USING silver_events_updates source
    ON target.event_pk = source.event_pk
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    """
    
    spark.sql(merge_sql)
    print(f"MERGE completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 20. Optimize Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize with Z-ORDER on key columns
# MAGIC OPTIMIZE silver_customer_events_enriched
# MAGIC ZORDER BY (customer_key, session_id, event_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update table statistics
# MAGIC ANALYZE TABLE silver_customer_events_enriched COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 21. Update Watermark

# COMMAND ----------

# Get max timestamp from processed data
max_timestamp = silver_events_schema.agg(F.max("event_timestamp")).collect()[0][0]
max_date = silver_events_schema.agg(F.max("event_date_est")).collect()[0][0]

# Update watermark table
spark.sql(f"""
    MERGE INTO {CHECKPOINT_TABLE} target
    USING (
        SELECT 
            '{TARGET_TABLE}' as table_name,
            CAST('{max_timestamp}' AS TIMESTAMP) as last_processed_timestamp,
            CAST('{max_date}' AS DATE) as last_processed_date,
            current_timestamp() as updated_at
    ) source
    ON target.table_name = source.table_name
    WHEN MATCHED THEN
        UPDATE SET 
            last_processed_timestamp = source.last_processed_timestamp,
            last_processed_date = source.last_processed_date,
            updated_at = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT *
""")

print(f"Watermark updated: {max_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 22. Data Quality Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary statistics
# MAGIC SELECT
# MAGIC     date_est,
# MAGIC     COUNT(*) as total_events,
# MAGIC     COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC     COUNT(DISTINCT session_id) as unique_sessions,
# MAGIC     SUM(CASE WHEN is_identified_user THEN 1 ELSE 0 END) as identified_events,
# MAGIC     SUM(CASE WHEN is_conversion_event THEN 1 ELSE 0 END) as conversion_events,
# MAGIC     SUM(CASE WHEN is_transaction_event THEN 1 ELSE 0 END) as transaction_events,
# MAGIC     SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END) as duplicate_events,
# MAGIC     AVG(data_quality_score) as avg_quality_score,
# MAGIC     SUM(COALESCE(revenue, 0)) as total_revenue
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY date_est
# MAGIC ORDER BY date_est DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Device type distribution
# MAGIC SELECT
# MAGIC     device_type,
# MAGIC     COUNT(*) as event_count,
# MAGIC     COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_total
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY device_type
# MAGIC ORDER BY event_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event type distribution
# MAGIC SELECT
# MAGIC     event_type,
# MAGIC     COUNT(*) as event_count,
# MAGIC     SUM(COALESCE(revenue, 0)) as total_revenue,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_total
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY event_type
# MAGIC ORDER BY event_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 23. Job Completion

# COMMAND ----------

# Summary statistics for logging
total_processed = silver_events_schema.count()
unique_customers = silver_events_schema.select("customer_key").distinct().count()
unique_sessions = silver_events_schema.select("session_id").distinct().count()

print("=" * 80)
print("SILVER LAYER - CUSTOMER EVENTS ENRICHED - JOB COMPLETED")
print("=" * 80)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Watermark: {last_watermark} -> {max_timestamp}")
print(f"Total Events Processed: {total_processed:,}")
print(f"Unique Customers: {unique_customers:,}")
print(f"Unique Sessions: {unique_sessions:,}")
print(f"Processing Timestamp: {datetime.now()}")
print("=" * 80)

# Return success
dbutils.notebook.exit(f"Success: Processed {total_processed} events")