MAGIC %md
MAGIC # Silver Layer: Customer Events Enriched (Production Version)
MAGIC
MAGIC Purpose: Flatten and enrich customer events from offer_silver_clean
MAGIC
MAGIC Key Features:
MAGIC - Enhanced user identity resolution (profile.id, fluentId, emailSha256, emailMd5, phone)
MAGIC - Comprehensive conversion detection logic
MAGIC - Event classification and typing
MAGIC - Struct flattening with fallback logic
MAGIC - Temporal enrichment with timezone handling
MAGIC - Data quality scoring
MAGIC
MAGIC Source: offer_silver_clean
MAGIC Target: silver_customer_events_enriched
MAGIC Schedule: Every 30 minutes (aligned with source ingestion)
COMMAND ----------
MAGIC %md
MAGIC ## 1. Setup and Configuration
COMMAND ----------
Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import hashlib
Configuration parameters
SOURCE_TABLE = "offer_silver_clean"
TARGET_TABLE = "silver_customer_events_enriched"
CHECKPOINT_TABLE = "silver_customer_events_watermark"
DATABASE = "customer_analytics"
Create database if not exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")
print(f"Configuration loaded successfully")
print(f"  Source: {SOURCE_TABLE}")
print(f"  Target: {TARGET_TABLE}")
print(f"  Database: {DATABASE}")
COMMAND ----------
MAGIC %md
MAGIC ## 2. Define Helper Functions
COMMAND ----------
UDF for phone number normalization
def normalize_phone(phone):
"""
Normalize phone number by removing non-digits and standardizing format
CopyArgs:
    phone: Raw phone number string

Returns:
    Normalized 10-digit phone string or None
"""
if phone:
    # Remove all non-digit characters
    digits_only = ''.join(c for c in str(phone) if c.isdigit())
    
    # Remove leading 1 for US numbers if present and length is 11
    if len(digits_only) == 11 and digits_only.startswith('1'):
        return digits_only[1:]
    
    # Return only if we have valid 10+ digit number
    return digits_only if len(digits_only) >= 10 else None
return None
normalize_phone_udf = F.udf(normalize_phone, StringType())
UDF for generating phone hash
def hash_phone(phone):
"""
Generate SHA256 hash of normalized phone number
CopyArgs:
    phone: Raw phone number

Returns:
    SHA256 hash string or None
"""
normalized = normalize_phone(phone)
if normalized:
    return hashlib.sha256(normalized.encode()).hexdigest()
return None
hash_phone_udf = F.udf(hash_phone, StringType())
print("Helper functions registered successfully")
COMMAND ----------
MAGIC %md
MAGIC ## 3. Initialize Watermark for Incremental Processing
COMMAND ----------
MAGIC %sql
MAGIC -- Create watermark table if not exists
MAGIC CREATE TABLE IF NOT EXISTS silver_customer_events_watermark (
MAGIC   table_name STRING,
MAGIC   last_processed_timestamp TIMESTAMP,
MAGIC   last_processed_date DATE,
MAGIC   updated_at TIMESTAMP
MAGIC )
MAGIC USING DELTA;
COMMAND ----------
Get last processed timestamp for incremental load
watermark_df = spark.sql(f"""
SELECT last_processed_timestamp
FROM {CHECKPOINT_TABLE}
WHERE table_name = '{TARGET_TABLE}'
""")
if watermark_df.count() > 0:
last_watermark = watermark_df.collect()[0]['last_processed_timestamp']
print(f"Incremental load - processing since: {last_watermark}")
else:
# First run - process last 7 days
last_watermark = datetime.now() - timedelta(days=7)
print(f"Initial load - processing last 7 days since: {last_watermark}")
COMMAND ----------
MAGIC %md
MAGIC ## 4. Read Source Data with Incremental Filter
COMMAND ----------
Read from offer_silver_clean with incremental filter
source_df = spark.sql(f"""
SELECT *
FROM {SOURCE_TABLE}
WHERE createDate >= '{last_watermark}'
AND _corrupt_record IS NULL
AND offerTraceId IS NOT NULL
ORDER BY createDate
""")
source_count = source_df.count()
print(f"Source records to process: {source_count:,}")
Stop if no records to process
if source_count == 0:
print("No new records to process. Exiting.")
dbutils.notebook.exit("Success: No new records")
COMMAND ----------
MAGIC %md
MAGIC ## 5. User Identity Resolution (Multi-Source PII)
COMMAND ----------
Extract all available identity fields from nested structures
events_with_identity_fields = source_df.withColumn(
"profile_id_clean",
F.col("profile.id")
).withColumn(
"fluent_id_clean",
F.col("fluentId")
).withColumn(
"email_sha256_clean",
F.col("emailSha256")
).withColumn(
"email_md5_clean",
F.col("emailMd5")
).withColumn(
# Extract phone from multiple possible locations
"phone_raw",
F.coalesce(
F.col("profile.phone"),
F.col("profile.phoneNumber"),
F.col("phone"),
F.col("phoneNumber")
)
).withColumn(
# Normalize phone number to standard format
"phone_normalized",
normalize_phone_udf(F.col("phone_raw"))
).withColumn(
# Hash normalized phone for identity matching
"phone_sha256",
hash_phone_udf(F.col("phone_raw"))
)
print("Identity fields extracted successfully")
COMMAND ----------
Resolve customer_key with priority hierarchy
Priority: profile.id > fluentId > emailSha256 > emailMd5 > phone_sha256 > sessionId (anonymous)
events_with_identity = events_with_identity_fields.withColumn(
"customer_key",
F.coalesce(
F.col("profile_id_clean"),
F.col("fluent_id_clean"),
F.col("email_sha256_clean"),
F.col("email_md5_clean"),
F.col("phone_sha256"),
F.concat(F.lit("ANON_"), F.col("sessionId"))  # Anonymous fallback
)
).withColumn(
# Track which field was used for customer_key (for data quality monitoring)
"customer_key_source",
F.when(F.col("profile_id_clean").isNotNull(), "profile_id")
.when(F.col("fluent_id_clean").isNotNull(), "fluent_id")
.when(F.col("email_sha256_clean").isNotNull(), "email_sha256")
.when(F.col("email_md5_clean").isNotNull(), "email_md5")
.when(F.col("phone_sha256").isNotNull(), "phone_sha256")
.otherwise("session_id")
).withColumn(
# Preserve individual identity fields for identity stitching
"fluent_id",
F.col("fluent_id_clean")
).withColumn(
"profile_id",
F.col("profile_id_clean")
).withColumn(
"email_sha256",
F.col("email_sha256_clean")
).withColumn(
"email_md5",
F.col("email_md5_clean")
).withColumn(
"phone_sha256_hash",
F.col("phone_sha256")
).withColumn(
"phone_normalized_value",
F.col("phone_normalized")
).withColumn(
# Device advertising ID from device struct
"device_advertising_id",
F.col("device.advertisingId")
).withColumn(
# Flag: User is identified if we have any PII
"is_identified_user",
F.when(
F.col("profile_id_clean").isNotNull() |
F.col("fluent_id_clean").isNotNull() |
F.col("email_sha256_clean").isNotNull() |
F.col("email_md5_clean").isNotNull() |
F.col("phone_sha256").isNotNull(),
True
).otherwise(False)
).withColumn(
# Flag: User is anonymous (no PII available)
"is_anonymous_user",
F.when(
F.col("profile_id_clean").isNull() &
F.col("fluent_id_clean").isNull() &
F.col("email_sha256_clean").isNull() &
F.col("email_md5_clean").isNull() &
F.col("phone_sha256").isNull(),
True
).otherwise(False)
).withColumn(
# Individual PII presence flags (for analytics)
"has_profile_id",
F.col("profile_id_clean").isNotNull()
).withColumn(
"has_email_identifier",
F.col("email_sha256_clean").isNotNull() | F.col("email_md5_clean").isNotNull()
).withColumn(
"has_phone_identifier",
F.col("phone_sha256").isNotNull()
).drop(
"profile_id_clean", "fluent_id_clean", "email_sha256_clean",
"email_md5_clean", "phone_raw", "phone_normalized", "phone_sha256"
)
print("Customer identity resolution completed")
COMMAND ----------
MAGIC %md
MAGIC ## 5a. Identity Resolution Statistics (Data Quality Check)
COMMAND ----------
Display identity resolution breakdown
print("=" * 80)
print("IDENTITY RESOLUTION STATISTICS")
print("=" * 80)
identity_stats = events_with_identity.groupBy("customer_key_source").agg(
F.count("*").alias("event_count"),
F.countDistinct("customer_key").alias("unique_customers")
).orderBy(F.col("event_count").desc())
identity_stats.show(truncate=False)
Show PII coverage metrics
pii_coverage = events_with_identity.agg(
F.count("*").alias("total_events"),
F.sum(F.when(F.col("has_profile_id"), 1).otherwise(0)).alias("events_with_profile_id"),
F.sum(F.when(F.col("has_email_identifier"), 1).otherwise(0)).alias("events_with_email"),
F.sum(F.when(F.col("has_phone_identifier"), 1).otherwise(0)).alias("events_with_phone"),
F.sum(F.when(F.col("is_identified_user"), 1).otherwise(0)).alias("identified_events"),
F.sum(F.when(F.col("is_anonymous_user"), 1).otherwise(0)).alias("anonymous_events")
)
print("\nPII COVERAGE:")
pii_coverage.show(vertical=True)
print("=" * 80)
COMMAND ----------
MAGIC %md
MAGIC ## 6. Advanced Conversion Detection Logic
COMMAND ----------
Extract conversion array from campaignData struct
events_with_conversions = events_with_identity.withColumn(
"conversion_array",
F.col("campaignData.conversions")
).withColumn(
# Check if conversion array exists and has elements
"has_conversion_array",
F.when(F.size(F.col("conversion_array")) > 0, True).otherwise(False)
)
Extract conversion details from first element in array
events_conversion_extracted = events_with_conversions.withColumn(
# Conversion type (e.g., 'sale', 'lead', 'registration')
"conversion_type",
F.when(F.col("has_conversion_array"),
F.col("conversion_array")[0].getField("conversionType"))
.otherwise(None)
).withColumn(
# Conversion goal ID
"conversion_goal_id",
F.when(F.col("has_conversion_array"),
F.col("conversion_array")[0].getField("conversionGoalId"))
.otherwise(None)
).withColumn(
# Order ID - check multiple sources with priority
"order_id",
F.coalesce(
F.col("campaignData.orderId"),
F.when(F.col("has_conversion_array"),
F.col("conversion_array")[0].getField("orderId"))
.otherwise(None)
)
).withColumn(
# Days to complete conversion
"conversion_days_to_complete",
F.when(F.col("has_conversion_array"),
F.col("conversion_array")[0].getField("daysToComplete"))
.otherwise(None)
).withColumn(
# Transaction ID from campaign data
"transaction_id",
F.col("campaignData.transactionId")
).withColumn(
# Extract revenue fields with proper type casting
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
).drop("conversion_array", "has_conversion_array")
print("Conversion data extraction completed")
COMMAND ----------
MAGIC %md
MAGIC ## 7. Event Classification
COMMAND ----------
Classify events based on data presence
events_classified = events_conversion_extracted.withColumn(
# Primary event type classification
"event_type",
F.when(F.col("conversion_type").isNotNull(), "conversion")
.when(F.col("order_id").isNotNull(), "transaction")
.when(F.col("campaignData.clickId").isNotNull(), "click")
.otherwise("view")
).withColumn(
# Product scope (Adflow, Syndication, etc.)
"product_scope",
F.coalesce(F.col("productScope"), F.lit("unknown"))
).withColumn(
# Position 1 (top position) view flag
"is_p1_view",
F.when(F.col("campaignData.position") == 1, True).otherwise(False)
).withColumn(
# Conversion event flag
"is_conversion_event",
F.when(F.col("conversion_type").isNotNull(), True).otherwise(False)
).withColumn(
# Transaction event flag
"is_transaction_event",
F.when(F.col("order_id").isNotNull(), True).otherwise(False)
)
print("Event classification completed")
COMMAND ----------
MAGIC %md
MAGIC ## 8. Flatten Campaign Dimensions
COMMAND ----------
Extract campaign hierarchy fields
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
).withColumn(
# Campaign position
"campaign_position",
F.col("campaignData.position")
).withColumn(
# Click ID if present
"click_id",
F.col("campaignData.clickId")
)
print("Campaign dimensions flattened")
COMMAND ----------
MAGIC %md
MAGIC ## 9. Flatten Device and Browser Data
COMMAND ----------
Extract device characteristics
events_device_flattened = events_campaign_flattened.withColumn(
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
# Device type boolean flags
"is_mobile",
F.when(F.lower(F.col("device.inferredDeviceType")).isin(["smartphone", "mobile"]), True).otherwise(False)
).withColumn(
"is_tablet",
F.when(F.lower(F.col("device.inferredDeviceType")) == "tablet", True).otherwise(False)
).withColumn(
"is_desktop",
F.when(F.lower(F.col("device.inferredDeviceType")).isin(["desktop", "pc"]), True).otherwise(False)
).withColumn(
# Bot detection flag
"is_bot",
F.coalesce(F.col("device.isBot"), F.lit(False))
)
print("Device data flattened")
COMMAND ----------
MAGIC %md
MAGIC ## 10. Flatten Geographic Data
COMMAND ----------
Extract geographic information from multiple sources
events_geo_flattened = events_device_flattened.withColumn(
# Country - check profile, device, and geo in priority order
"country",
F.coalesce(
F.col("profile.address.country"),
F.col("device.countryCode"),
F.col("geo.country")
)
).withColumn(
# State
"state",
F.coalesce(
F.col("profile.address.state"),
F.col("geo.state")
)
).withColumn(
# City
"city",
F.coalesce(
F.col("profile.address.city"),
F.col("geo.city")
)
).withColumn(
# ZIP/Postal code
"zip",
F.coalesce(
F.col("profile.address.zip"),
F.col("geo.postalCode")
)
).withColumn(
# IP-resolved country (for comparison with profile country)
"ip_resolved_country",
F.col("device.countryCode")
).withColumn(
# Local time offset from UTC
"local_time_offset",
F.col("device.localTimeOffset")
)
print("Geographic data flattened")
COMMAND ----------
MAGIC %md
MAGIC ## 11. Flatten Traffic Source Data
COMMAND ----------
Extract traffic source and attribution data
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
# Sub-affiliate tracking parameters
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
print("Traffic source data flattened")
COMMAND ----------
MAGIC %md
MAGIC ## 12. Temporal Enrichment with Timezone Handling
COMMAND ----------
Convert timestamps to EST and extract temporal attributes
events_temporal_enriched = events_traffic_flattened.withColumn(
# Event timestamp (UTC)
"event_timestamp",
F.col("timestamp").cast(TimestampType())
).withColumn(
# Convert to EST for business reporting
"event_timestamp_est",
F.from_utc_timestamp(F.col("event_timestamp"), "America/New_York")
).withColumn(
# Event date in EST
"event_date_est",
F.to_date(F.col("event_timestamp_est"))
).withColumn(
# Hour of day in EST (0-23)
"event_hour_est",
F.hour(F.col("event_timestamp_est"))
).withColumn(
# Local hour based on user's timezone offset
"local_hour_of_day",
F.when(F.col("device.localTimeOffset").isNotNull(),
F.hour(F.col("event_timestamp") + F.expr("INTERVAL " + F.col("device.localTimeOffset") + " HOURS")))
.otherwise(F.col("event_hour_est"))
).withColumn(
# Day of week (Monday, Tuesday, etc.)
"day_of_week",
F.date_format(F.col("event_date_est"), "EEEE")
).withColumn(
# Business hours flag (9am-5pm EST)
"is_business_hours",
F.when((F.col("event_hour_est") >= 9) & (F.col("event_hour_est") < 17), True).otherwise(False)
).withColumn(
# Weekend flag (Saturday, Sunday)
"is_weekend",
F.when(F.dayofweek(F.col("event_date_est")).isin([1, 7]), True).otherwise(False)
)
print("Temporal enrichment completed")
COMMAND ----------
MAGIC %md
MAGIC ## 13. Extract Profile Demographic Data
COMMAND ----------
Extract user profile attributes
events_profile_enriched = events_temporal_enriched.withColumn(
"profile_gender",
F.col("profile.gender")
).withColumn(
"profile_home_owner",
F.col("profile.homeOwner")
).withColumn(
# Parse boolean string to actual boolean
"profile_has_children",
F.when(F.lower(F.col("profile.hasChildren")) == "true", True)
.when(F.lower(F.col("profile.hasChildren")) == "false", False)
.otherwise(None)
).withColumn(
"profile_owns_car",
F.col("profile.ownsCar")
).withColumn(
# Cast visit timestamps
"profile_first_visit",
F.col("profile.firstVisit").cast(TimestampType())
).withColumn(
"profile_last_visit",
F.col("profile.lastVisit").cast(TimestampType())
)
print("Profile attributes extracted")
COMMAND ----------
MAGIC %md
MAGIC ## 14. Session Context and Referrer Data
COMMAND ----------
Extract session-level context
events_with_context = events_profile_enriched.withColumn(
"session_id",
F.col("sessionId")
).withColumn(
"referer",
F.col("referer")
).withColumn(
"user_agent",
F.col("userAgent")
).withColumn(
"source_reference_id",
F.col("sourceReferenceId")
).withColumn(
"source_reference",
F.col("sourceReference")
)
print("Session context extracted")
COMMAND ----------
MAGIC %md
MAGIC ## 15. Data Quality Scoring
COMMAND ----------
Calculate comprehensive data quality score
events_quality_scored = events_with_context.withColumn(
# Flag for corrupt records
"has_corrupt_record",
F.when(F.col("_corrupt_record").isNotNull(), True).otherwise(False)
).withColumn(
# Composite data quality score (0.0 to 1.0)
"data_quality_score",
(
# Critical fields (50% of score)
F.when(F.col("customer_key").isNotNull(), 0.20).otherwise(0.0) +
F.when(F.col("event_timestamp").isNotNull(), 0.20).otherwise(0.0) +
F.when(F.col("campaign_id").isNotNull(), 0.10).otherwise(0.0) +
Copy    # Identity quality (15% of score - higher for better identifiers)
    F.when(F.col("customer_key_source") == "profile_id", 0.15)
     .when(F.col("customer_key_source") == "fluent_id", 0.12)
     .when(F.col("customer_key_source") == "email_sha256", 0.10)
     .when(F.col("customer_key_source") == "email_md5", 0.08)
     .when(F.col("customer_key_source") == "phone_sha256", 0.08)
     .otherwise(0.0) +
    
    # Important fields (35% of score)
    F.when(F.col("device_type").isNotNull(), 0.10).otherwise(0.0) +
    F.when(F.col("advertiser_id").isNotNull(), 0.07).otherwise(0.0) +
    F.when(F.col("creative_id").isNotNull(), 0.07).otherwise(0.0) +
    F.when(F.col("country").isNotNull(), 0.06).otherwise(0.0) +
    F.when(F.col("browser_name").isNotNull(), 0.05).otherwise(0.0)
)
)
print("Data quality scoring completed")
COMMAND ----------
MAGIC %md
MAGIC ## 16. Generate Event Primary Key and Deduplication
COMMAND ----------
Create unique event identifier
events_with_pk = events_quality_scored.withColumn(
# SHA256 hash of offerTraceId + timestamp
"event_pk",
F.sha2(F.concat_ws("_", F.col("offerTraceId"), F.col("timestamp")), 256)
).withColumn(
"offer_trace_id",
F.col("offerTraceId")
)
Deduplicate events using window function
window_dup = Window.partitionBy("event_pk").orderBy(F.col("createDate").desc())
events_deduped = events_with_pk.withColumn(
"row_num",
F.row_number().over(window_dup)
).withColumn(
# Flag duplicates (keep most recent by createDate)
"is_duplicate",
F.when(F.col("row_num") > 1, True).otherwise(False)
).drop("row_num")
print("Event deduplication completed")
COMMAND ----------
MAGIC %md
MAGIC ## 17. Add Processing Metadata
COMMAND ----------
Add metadata columns
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
# Partition columns
"date_est",
F.col("event_date_est")
).withColumn(
"hour_est",
F.col("event_hour_est")
)
print("Metadata added")
COMMAND ----------
MAGIC %md
MAGIC ## 18. Apply Data Quality Filters
COMMAND ----------
Filter out low quality and invalid records
events_filtered = events_final.filter(
(F.col("is_bot") == False) &                                     # Remove bot traffic
(F.col("data_quality_score") >= 0.5) &                          # Minimum quality threshold
(F.col("event_timestamp") >= "2020-01-01") &                    # Valid date range
(F.col("event_timestamp") <= F.current_timestamp()) &           # Not future dates
(~F.lower(F.coalesce(F.col("campaign_name"), F.lit(""))).like("%test%"))  # Remove test campaigns
)
filtered_count = events_filtered.count()
print(f"Records after quality filters: {filtered_count:,}")
print(f"Records filtered out: {(source_count - filtered_count):,}")
COMMAND ----------
MAGIC %md
MAGIC ## 19. Select Final Schema
COMMAND ----------
Select final schema for target table
silver_events_schema = events_filtered.select(
# Primary Keys & Identifiers
"event_pk",
"offer_trace_id",
"session_id",
"source_reference_id",
"source_reference",
Copy# User Identity
"customer_key",
"customer_key_source",
"fluent_id",
"profile_id",
"email_sha256",
"email_md5",
"phone_sha256_hash",
"phone_normalized_value",
"device_advertising_id",
"is_identified_user",
"is_anonymous_user",
"has_profile_id",
"has_email_identifier",
"has_phone_identifier",

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
"campaign_position",
"click_id",

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
print("Final schema selected")
COMMAND ----------
MAGIC %md
MAGIC ## 20. Write to Silver Table with MERGE
COMMAND ----------
Check if target table exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)
if not target_exists:
# First time - create table
print(f"Creating new table: {TARGET_TABLE}")
Copysilver_events_schema.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date_est", "hour_est") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

table_count = silver_events_schema.count()
print(f"✓ Table created successfully with {table_count:,} records")
else:
# Incremental MERGE
print(f"Performing incremental MERGE into {TARGET_TABLE}")
Copy# Register temp view for merge
silver_events_schema.createOrReplaceTempView("silver_events_updates")

# Execute MERGE statement
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
print(f"✓ MERGE completed successfully")
COMMAND ----------
MAGIC %md
MAGIC ## 21. Optimize Table
COMMAND ----------
MAGIC %sql
MAGIC -- Optimize with Z-ORDER on key query columns
MAGIC OPTIMIZE silver_customer_events_enriched
MAGIC ZORDER BY (customer_key, customer_key_source, session_id, event_timestamp);
COMMAND ----------
MAGIC %sql
MAGIC -- Update table statistics for query optimization
MAGIC ANALYZE TABLE silver_customer_events_enriched COMPUTE STATISTICS FOR ALL COLUMNS;
COMMAND ----------
print("✓ Table optimization completed")
COMMAND ----------
MAGIC %md
MAGIC ## 22. Update Watermark
COMMAND ----------
Get max timestamp from processed data
max_timestamp = silver_events_schema.agg(F.max("event_timestamp")).collect()[0][0]
max_date = silver_events_schema.agg(F.max("event_date_est")).collect()[0][0]
Update watermark table
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
print(f"✓ Watermark updated: {max_timestamp}")
COMMAND ----------
MAGIC %md
MAGIC ## 23. Data Quality Checks and Reporting
COMMAND ----------
MAGIC %sql
MAGIC -- Daily summary statistics with identity breakdown
MAGIC SELECT
MAGIC   date_est,
MAGIC   COUNT(*) as total_events,
MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
MAGIC   COUNT(DISTINCT session_id) as unique_sessions,
MAGIC
MAGIC   -- Identity resolution breakdown
MAGIC   SUM(CASE WHEN customer_key_source = 'profile_id' THEN 1 ELSE 0 END) as profile_id_events,
MAGIC   SUM(CASE WHEN customer_key_source = 'fluent_id' THEN 1 ELSE 0 END) as fluent_id_events,
MAGIC   SUM(CASE WHEN customer_key_source = 'email_sha256' THEN 1 ELSE 0 END) as email_sha256_events,
MAGIC   SUM(CASE WHEN customer_key_source = 'email_md5' THEN 1 ELSE 0 END) as email_md5_events,
MAGIC   SUM(CASE WHEN customer_key_source = 'phone_sha256' THEN 1 ELSE 0 END) as phone_events,
MAGIC   SUM(CASE WHEN customer_key_source = 'session_id' THEN 1 ELSE 0 END) as anonymous_events,
MAGIC
MAGIC   -- Event types
MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as view_events,
MAGIC   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as click_events,
MAGIC   SUM(CASE WHEN is_conversion_event THEN 1 ELSE 0 END) as conversion_events,
MAGIC   SUM(CASE WHEN is_transaction_event THEN 1 ELSE 0 END) as transaction_events,
MAGIC
MAGIC   -- Quality metrics
MAGIC   SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END) as duplicate_events,
MAGIC   ROUND(AVG(data_quality_score), 3) as avg_quality_score,
MAGIC
MAGIC   -- Revenue
MAGIC   SUM(COALESCE(revenue, 0)) as total_revenue
MAGIC FROM silver_customer_events_enriched
MAGIC WHERE date_est >= CURRENT_DATE - 7
MAGIC GROUP BY date_est
MAGIC ORDER BY date_est DESC;
COMMAND ----------
MAGIC %sql
MAGIC -- Conversion funnel metrics
MAGIC SELECT
MAGIC   date_est,
MAGIC   COUNT(DISTINCT CASE WHEN event_type = 'view' THEN customer_key END) as viewers,
MAGIC   COUNT(DISTINCT CASE WHEN event_type = 'click' THEN customer_key END) as clickers,
MAGIC   COUNT(DISTINCT CASE WHEN is_conversion_event THEN customer_key END) as converters,
MAGIC   COUNT(DISTINCT CASE WHEN is_transaction_event THEN customer_key END) as transactors,
MAGIC
MAGIC   -- Conversion rates
MAGIC   ROUND(
MAGIC     COUNT(DISTINCT CASE WHEN event_type = 'click' THEN customer_key END) * 100.0 /
MAGIC     NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'view' THEN customer_key END), 0),
MAGIC     2
MAGIC   ) as view_to_click_rate,
MAGIC
MAGIC   ROUND(
MAGIC     COUNT(DISTINCT CASE WHEN is_conversion_event THEN customer_key END) * 100.0 /
MAGIC     NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'click' THEN customer_key END), 0),
MAGIC     2
MAGIC   ) as click_to_conversion_rate
MAGIC FROM silver_customer_events_enriched
MAGIC WHERE date_est >= CURRENT_DATE - 7
MAGIC GROUP BY date_est
MAGIC ORDER BY date_est DESC;
COMMAND ----------
MAGIC %md
MAGIC ## 24. Create Identity Resolution Audit Table
COMMAND ----------
MAGIC %sql
MAGIC -- Create audit table for identity resolution monitoring
MAGIC CREATE OR REPLACE TABLE silver_identity_resolution_audit
MAGIC USING DELTA
MAGIC AS
MAGIC SELECT
MAGIC   date_est as audit_date,
MAGIC   customer_key_source,
MAGIC   COUNT(*) as event_count,
MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
MAGIC   COUNT(DISTINCT session_id) as unique_sessions,
MAGIC   ROUND(AVG(data_quality_score), 3) as avg_quality_score,
MAGIC   SUM(CASE WHEN is_conversion_event THEN 1 ELSE 0 END) as conversions,
MAGIC   SUM(COALESCE(revenue, 0)) as total_revenue,
MAGIC   current_timestamp() as audit_timestamp
MAGIC FROM silver_customer_events_enriched
MAGIC WHERE date_est >= CURRENT_DATE - 30
MAGIC GROUP BY date_est, customer_key_source;
COMMAND ----------
print("✓ Identity resolution audit table created")
COMMAND ----------
MAGIC %md
MAGIC ## 25. Job Completion Summary
COMMAND ----------
Calculate summary statistics
total_processed = silver_events_schema.count()
unique_customers = silver_events_schema.select("customer_key").distinct().count()
unique_sessions = silver_events_schema.select("session_id").distinct().count()
Identity resolution breakdown
identity_breakdown = silver_events_schema.groupBy("customer_key_source").count().collect()
Conversion metrics
conversion_stats = silver_events_schema.agg(
F.sum(F.when(F.col("is_conversion_event"), 1).otherwise(0)).alias("total_conversions"),
F.sum(F.when(F.col("is_transaction_event"), 1).otherwise(0)).alias("total_transactions"),
F.sum(F.coalesce(F.col("revenue"), F.lit(0))).alias("total_revenue")
).collect()[0]
Print completion summary
print("=" * 80)
print("SILVER LAYER - CUSTOMER EVENTS ENRICHED - JOB COMPLETED")
print("=" * 80)
print(f"Source Table: {SOURCE_TABLE}")
print(f"Target Table: {TARGET_TABLE}")
print(f"Watermark: {last_watermark} -> {max_timestamp}")
print("")
print(f"Total Events Processed: {total_processed:,}")
print(f"Unique Customers: {unique_customers:,}")
print(f"Unique Sessions: {unique_sessions:,}")
print("")
print("IDENTITY RESOLUTION BREAKDOWN:")
for row in identity_breakdown:
source = row['customer_key_source']
count = row['count']
pct = (count / total_processed) * 100
print(f"  {source}: {count:>10,} ({pct:>5.1f}%)")
print("")
print("CONVERSION METRICS:")
print(f"  Total Conversions: {conversion_stats['total_conversions']:,}")
print(f"  Total Transactions: {conversion_stats['total_transactions']:,}")
print(f"  Total Revenue: ${conversion_stats['total_revenue']:,.2f}")
print("")
print(f"Processing Timestamp: {datetime.now()}")
print("=" * 80)
Return success
identity_summary = {row['customer_key_source']: row['count'] for row in identity_breakdown}
dbutils.notebook.exit(f"Success: Processed {total_processed:,} events. Identity: {identity_summary}")