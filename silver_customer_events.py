# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Customer Events Enriched
# MAGIC
# MAGIC **Purpose:** Flatten and enrich customer events from offer_silver_clean with:
# MAGIC - Enhanced user identity resolution (profile.id, fluentId, emailSha256, emailMd5, phone)
# MAGIC - Conversion detection logic aligned with existing fact tables (sourceReference = 'offer-convert')
# MAGIC - Event classification and struct flattening
# MAGIC - Temporal enrichment and data quality scoring
# MAGIC
# MAGIC **Source:** offer_silver_clean  
# MAGIC **Target:** silver_customer_events_enriched  
# MAGIC **Schedule:** Every 30 minutes (aligned with source ingestion)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

# Configuration
SOURCE_TABLE = "centraldata_prod.minion_event.offer_silver_clean"
TARGET_TABLE = "centraldata_sandbox.test.silver_customer_events_enriched"
CHECKPOINT_TABLE = "centraldata_sandbox.test.silver_customer_events_watermark"
DATABASE = "customer_analytics"

# =============================================================================
# RUN MODE CONFIGURATION
# =============================================================================
# Set RUN_MODE to control how the notebook processes data:
#   - "INCREMENTAL": Process only new data since last watermark (default for hourly runs)
#   - "FULL_REFRESH": Overwrite entire table (use for historical loads/backfills)
#
# For FULL_REFRESH mode, set START_DATE and END_DATE to define the date range.
# For INCREMENTAL mode, these are ignored and watermark is used instead.
# =============================================================================

# Create widgets for parameterized runs
dbutils.widgets.dropdown("run_mode", "INCREMENTAL", ["INCREMENTAL", "FULL_REFRESH"])
dbutils.widgets.text("start_date", "")  # Format: YYYY-MM-DD
dbutils.widgets.text("end_date", "")    # Format: YYYY-MM-DD

# Get parameters
RUN_MODE = dbutils.widgets.get("run_mode").upper()
START_DATE = dbutils.widgets.get("start_date") or None
END_DATE = dbutils.widgets.get("end_date") or None

print(f"=" * 60)
print(f"RUN MODE: {RUN_MODE}")
if RUN_MODE == "FULL_REFRESH":
    print(f"START_DATE: {START_DATE or 'Not specified (will use default)'}")
    print(f"END_DATE: {END_DATE or 'Not specified (will use current)'}")
print(f"=" * 60)


# COMMAND ----------

spark.sql("USE CATALOG centraldata_sandbox")
spark.sql("USE SCHEMA test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Watermark Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create watermark table for incremental processing
# MAGIC CREATE TABLE IF NOT EXISTS centraldata_sandbox.test.silver_customer_events_watermark (
# MAGIC   table_name STRING,
# MAGIC   last_processed_timestamp TIMESTAMP,
# MAGIC   last_processed_date DATE,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Get Last Watermark

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Initialize watermark if not exists
# MAGIC MERGE INTO centraldata_sandbox.test.silver_customer_events_watermark target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     'silver_customer_events_enriched' as table_name,
# MAGIC     CAST('2025-01-01 00:00:00' AS TIMESTAMP) as last_processed_timestamp,
# MAGIC     CAST('2025-01-01' AS DATE) as last_processed_date,
# MAGIC     current_timestamp() as updated_at
# MAGIC ) source
# MAGIC ON target.table_name = source.table_name
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# Determine date range based on RUN_MODE
if RUN_MODE == "FULL_REFRESH":
    # Full refresh mode - use provided date range or defaults
    if START_DATE:
        start_ts = f"CAST('{START_DATE} 00:00:00' AS TIMESTAMP)"
    else:
        # Default: process last 365 days for full refresh
        start_ts = "CURRENT_TIMESTAMP - INTERVAL 365 DAYS"

    if END_DATE:
        end_ts = f"CAST('{END_DATE} 23:59:59' AS TIMESTAMP)"
    else:
        end_ts = "CURRENT_TIMESTAMP"

    # Create watermark view for FULL_REFRESH with date range
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW last_watermark AS
        SELECT
            {start_ts} as watermark_ts,
            {end_ts} as end_watermark_ts
    """)
    print(f"FULL REFRESH MODE: Processing data from {START_DATE or 'last 365 days'} to {END_DATE or 'now'}")

else:
    # Incremental mode - use watermark table
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW last_watermark AS
        SELECT
            COALESCE(last_processed_timestamp, CURRENT_TIMESTAMP - INTERVAL 1 DAYS) as watermark_ts,
            CAST(NULL AS TIMESTAMP) as end_watermark_ts
        FROM silver_customer_events_watermark
        WHERE table_name = 'silver_customer_events_enriched'
    """)
    print("INCREMENTAL MODE: Using watermark table for processing window")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Enhanced Identity Resolution with UDF for Phone Normalization

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib

# UDF for phone number normalization
@F.udf(StringType())
def normalize_phone(phone):
    """Normalize phone number by removing non-digits and handling US format"""
    if phone:
        # Remove all non-digit characters
        digits_only = ''.join(c for c in str(phone) if c.isdigit())
        # Remove leading 1 for US numbers if present and length is 11
        if len(digits_only) == 11 and digits_only.startswith('1'):
            return digits_only[1:]
        return digits_only if len(digits_only) >= 10 else None
    return None

# UDF for generating phone hash
@F.udf(StringType())
def hash_phone(phone):
    """Generate SHA256 hash of normalized phone"""
    if phone:
        normalized = ''.join(c for c in str(phone) if c.isdigit())
        if len(normalized) == 11 and normalized.startswith('1'):
            normalized = normalized[1:]
        if len(normalized) >= 10:
            return hashlib.sha256(normalized.encode()).hexdigest()
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Main Transformation - SQL-Based with Conversion Logic

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT watermark_ts FROM last_watermark

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Main transformation: Flatten, enrich, and apply conversion logic
# MAGIC CREATE OR REPLACE TEMP VIEW silver_customer_events_transformed AS
# MAGIC
# MAGIC WITH source_filtered AS (
# MAGIC   -- Filter source data based on run mode (incremental or full refresh with date range)
# MAGIC   SELECT *
# MAGIC   FROM centraldata_prod.minion_event.offer_silver_clean
# MAGIC   WHERE createDate >= (SELECT watermark_ts FROM last_watermark)
# MAGIC     AND (
# MAGIC       (SELECT end_watermark_ts FROM last_watermark) IS NULL  -- Incremental: no end date
# MAGIC       OR createDate <= (SELECT end_watermark_ts FROM last_watermark)  -- Full refresh: apply end date
# MAGIC     )
# MAGIC     AND _corrupt_record IS NULL
# MAGIC ),
# MAGIC
# MAGIC identity_resolved AS (
# MAGIC   -- Step 1: Extract and resolve customer identity
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     -- Extract identity fields from nested structures
# MAGIC     profile.id as profile_id_raw,
# MAGIC     fluentId as fluent_id_raw,
# MAGIC     profile.emailSha256 as email_sha256_raw,
# MAGIC     profile.emailMd5 as email_md5_raw,
# MAGIC     profile.telephone as phone_raw,
# MAGIC     
# MAGIC     -- Determine customer_key with priority hierarchy
# MAGIC     COALESCE(
# MAGIC       profile.id,           -- Priority 1: Profile ID (most reliable)
# MAGIC       fluentId,             -- Priority 2: Fluent ID
# MAGIC       profile.emailSha256,          -- Priority 3: Email SHA256
# MAGIC       profile.emailMd5,             -- Priority 4: Email MD5
# MAGIC       -- Note: phone_sha256 would be priority 5, handled in Python UDF
# MAGIC       CONCAT('ANON_', sessionId)  -- Fallback: Anonymous session
# MAGIC     ) as customer_key_preliminary,
# MAGIC     
# MAGIC     -- Identify source of customer_key
# MAGIC     CASE 
# MAGIC       WHEN profile.id IS NOT NULL THEN 'profile_id'
# MAGIC       WHEN fluentId IS NOT NULL THEN 'fluent_id'
# MAGIC       WHEN profile.emailSha256 IS NOT NULL THEN 'email_sha256'
# MAGIC       WHEN profile.emailMd5 IS NOT NULL THEN 'email_md5'
# MAGIC       -- phone_sha256 check in Python layer
# MAGIC       ELSE 'session_id'
# MAGIC     END as customer_key_source_preliminary,
# MAGIC     
# MAGIC     -- Identity presence flags
# MAGIC     profile.id IS NOT NULL as has_profile_id,
# MAGIC     (profile.emailSha256 IS NOT NULL OR profile.emailMd5 IS NOT NULL) as has_email_identifier,
# MAGIC     profile.telephone IS NOT NULL as has_phone_raw,
# MAGIC     
# MAGIC     -- Determine if user is identified
# MAGIC     (profile.id IS NOT NULL 
# MAGIC      OR fluentId IS NOT NULL 
# MAGIC      OR profile.emailSha256 IS NOT NULL 
# MAGIC      OR profile.emailMd5 IS NOT NULL
# MAGIC      OR profile.telephone IS NOT NULL) as is_identified_user_preliminary
# MAGIC       
# MAGIC   FROM source_filtered
# MAGIC ),
# MAGIC
# MAGIC conversion_logic AS (
# MAGIC   -- Step 2: Apply conversion detection logic (aligned with existing fact tables)
# MAGIC   -- Conversion is defined as: sourceReference = 'offer-convert' AND conversion_type != 'Click'
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     
# MAGIC     -- Extract conversion type name from campaignData
# MAGIC     campaignData.conversionTypeName as conversion_type_name,
# MAGIC     
# MAGIC     -- Conversion detection: Based on sourceReference = 'offer-convert' and conversion type
# MAGIC     -- Matches logic from fact_adflow_session and fact_hourly_performance_metrics
# MAGIC     CASE
# MAGIC       WHEN sourceReference = 'offer-convert' 
# MAGIC            AND COALESCE(campaignData.conversionTypeName, '') != 'Click'
# MAGIC       THEN TRUE
# MAGIC       ELSE FALSE
# MAGIC     END as is_conversion_event,
# MAGIC     
# MAGIC     -- Transaction detection: Based on orderId presence
# MAGIC     data.orderId IS NOT NULL as is_transaction_event,
# MAGIC     
# MAGIC     -- Extract conversion type
# MAGIC     CASE 
# MAGIC       WHEN sourceReference = 'offer-convert' 
# MAGIC            AND COALESCE(campaignData.conversionTypeName, '') != 'Click'
# MAGIC       THEN campaignData.conversionTypeName
# MAGIC       ELSE NULL
# MAGIC     END as conversion_type,
# MAGIC     
# MAGIC     -- Extract conversion goal ID
# MAGIC     CASE 
# MAGIC       WHEN sourceReference = 'offer-convert' 
# MAGIC            AND COALESCE(campaignData.conversionTypeName, '') != 'Click'
# MAGIC       THEN campaignData.conversionGoalId
# MAGIC       ELSE NULL
# MAGIC     END as conversion_goal_id,
# MAGIC     
# MAGIC     -- Extract order ID
# MAGIC     data.orderId as order_id,
# MAGIC     
# MAGIC
# MAGIC   
# MAGIC     -- Event type classification
# MAGIC     CASE 
# MAGIC         WHEN sourceReference = 'offer-view'    THEN 'view'
# MAGIC         WHEN sourceReference = 'offer-click'   THEN 'click'
# MAGIC         WHEN sourceReference = 'offer-convert' THEN 'conversion'
# MAGIC     END as event_type,
# MAGIC     
# MAGIC     -- P1 (primary position) view detection
# MAGIC       CASE 
# MAGIC       WHEN sourceReference = 'offer-view' 
# MAGIC           AND campaignData.flowImpressionPosition = 1 
# MAGIC       THEN TRUE
# MAGIC       ELSE FALSE
# MAGIC   END as is_p1_view
# MAGIC     
# MAGIC   FROM identity_resolved
# MAGIC ),
# MAGIC
# MAGIC flattened_data AS (
# MAGIC   -- Step 3: Flatten all nested structures
# MAGIC   SELECT
# MAGIC     -- Generate unique event primary key
# MAGIC     SHA2(CONCAT(offerTraceId, '_', CAST(timestamp AS STRING)), 256) as event_pk,
# MAGIC     
# MAGIC     -- Primary identifiers
# MAGIC     offerTraceId as offer_trace_id,
# MAGIC     sessionId as session_id,
# MAGIC     sourceReferenceId as source_reference_id,
# MAGIC     sourceReference as source_reference,
# MAGIC     
# MAGIC     -- Customer identity (preliminary - will be updated with phone in next step)
# MAGIC     customer_key_preliminary as customer_key,
# MAGIC     customer_key_source_preliminary as customer_key_source,
# MAGIC     profile_id_raw as profile_id,
# MAGIC     fluent_id_raw as fluent_id,
# MAGIC     email_sha256_raw as email_sha256,
# MAGIC     email_md5_raw as email_md5,
# MAGIC     phone_raw,  -- Will be normalized and hashed in Python
# MAGIC     is_identified_user_preliminary as is_identified_user,
# MAGIC     NOT is_identified_user_preliminary as is_anonymous_user,
# MAGIC     has_profile_id,
# MAGIC     has_email_identifier,
# MAGIC     has_phone_raw,
# MAGIC     
# MAGIC     -- Timestamps and temporal attributes
# MAGIC     CAST(timestamp AS TIMESTAMP) as event_timestamp,
# MAGIC     from_utc_timestamp(CAST(timestamp AS TIMESTAMP), 'America/New_York') as event_timestamp_est,
# MAGIC     CAST(from_utc_timestamp(CAST(timestamp AS TIMESTAMP), 'America/New_York') AS DATE) as event_date_est,
# MAGIC     CAST(HOUR(from_utc_timestamp(CAST(timestamp AS TIMESTAMP), 'America/New_York')) AS INT) as event_hour_est,
# MAGIC     
# MAGIC     -- Local hour (if timezone offset available)
# MAGIC     CAST(CASE
# MAGIC       WHEN timestamp IS NOT NULL
# MAGIC       THEN HOUR(CAST(timestamp AS TIMESTAMP) + MAKE_INTERVAL(0, 0, 0, 0, CAST(timestamp AS INT), 0, 0))
# MAGIC       ELSE HOUR(from_utc_timestamp(CAST(timestamp AS TIMESTAMP), 'America/New_York'))
# MAGIC     END AS INT) as local_hour_of_day,
# MAGIC     
# MAGIC     -- Day of week
# MAGIC     DATE_FORMAT(from_utc_timestamp(CAST(timestamp AS TIMESTAMP), 'America/New_York'), 'EEEE') as day_of_week,
# MAGIC     
# MAGIC     -- Business hours flag (9am - 5pm EST)
# MAGIC     HOUR(from_utc_timestamp(CAST(timestamp AS TIMESTAMP), 'America/New_York')) BETWEEN 9 AND 16 as is_business_hours,
# MAGIC     
# MAGIC     -- Weekend flag (Saturday=7, Sunday=1 in DAYOFWEEK)
# MAGIC     DAYOFWEEK(from_utc_timestamp(CAST(timestamp AS TIMESTAMP), 'America/New_York')) IN (1, 7) as is_weekend,
# MAGIC     
# MAGIC     -- Event classification (from previous CTE)
# MAGIC     event_type,
# MAGIC     COALESCE(productScope, 'unknown') as product_scope,
# MAGIC     is_p1_view,
# MAGIC     is_conversion_event,
# MAGIC     is_transaction_event,
# MAGIC     
# MAGIC     -- Campaign dimensions (flattened)
# MAGIC     campaignData.advertiserId as advertiser_id,
# MAGIC     campaignData.advertiserName as advertiser_name,
# MAGIC     campaignData.campaignId as campaign_id,
# MAGIC     campaignData.campaignName as campaign_name,
# MAGIC     campaignData.campaignType as campaign_type,
# MAGIC     campaignData.vertical as campaign_vertical,
# MAGIC     campaignData.subVertical as campaign_sub_vertical,
# MAGIC     campaignData.adgroupId as adgroup_id,
# MAGIC     campaignData.adgroupName as adgroup_name,
# MAGIC     campaignData.creativeId as creative_id,
# MAGIC     campaignData.creativeName as creative_name,
# MAGIC     campaignData.trackingId as tracking_id,
# MAGIC     
# MAGIC     -- Conversion data (from previous CTE)
# MAGIC     conversion_type,
# MAGIC     conversion_type_name,
# MAGIC     conversion_goal_id,
# MAGIC     order_id,
# MAGIC     CAST(campaignData.revenue AS DECIMAL(19,4)) as revenue,
# MAGIC     CAST(campaignData.grossRevenue AS DECIMAL(19,4)) as gross_revenue,
# MAGIC     CAST(campaignData.adjustedRevenue AS DECIMAL(19,4)) as adjusted_revenue,
# MAGIC     CAST(campaignData.saleAmount AS DECIMAL(19,4)) as sale_amount,
# MAGIC     data.currency as currency,
# MAGIC     
# MAGIC     -- Device and context (flattened)
# MAGIC     device.inferredDeviceType as device_type,
# MAGIC     device.osName as device_os,
# MAGIC     device.osVersion as device_os_version,
# MAGIC     device.brandName as device_brand,
# MAGIC     device.deviceModel as device_model,
# MAGIC     device.clientName as browser_name,
# MAGIC     device.clientVersion as browser_version,
# MAGIC     
# MAGIC     -- Device type flags
# MAGIC     LOWER(device.inferredDeviceType) IN ('smartphone', 'mobile') as is_mobile,
# MAGIC     LOWER(device.inferredDeviceType) = 'tablet' as is_tablet,
# MAGIC     LOWER(device.inferredDeviceType) IN ('desktop', 'pc') as is_desktop,
# MAGIC     COALESCE(device.isBot, FALSE) as is_bot,
# MAGIC     
# MAGIC     -- Geographic data (coalesced from multiple sources)
# MAGIC     COALESCE(data.country, "US") as country,
# MAGIC     COALESCE(profile.state, data.state) as state,
# MAGIC     COALESCE(profile.city, data.city) as city,
# MAGIC     COALESCE(profile.zip, data.zip) as zip,
# MAGIC     
# MAGIC     -- Traffic source (flattened)
# MAGIC     trafficSource.trafficPartnerId as partner_id,
# MAGIC     trafficSource.trafficPartnerName as partner_name,
# MAGIC     trafficSource.sourceId as source_id,
# MAGIC     trafficSource.trafficPartnerType as traffic_partner_type,
# MAGIC     trafficSource.subAff1 as subaff1,
# MAGIC     trafficSource.subAff2 as subaff2,
# MAGIC     trafficSource.subAff3 as subaff3,
# MAGIC     trafficSource.subAff4 as subaff4,
# MAGIC     
# MAGIC     -- Session context
# MAGIC     referer,
# MAGIC     device.userAgent as user_agent,
# MAGIC     
# MAGIC     -- Profile attributes
# MAGIC     profile.gender as profile_gender,
# MAGIC     profile.attrs.HomeOwne as profile_home_owner,
# MAGIC     CASE 
# MAGIC       WHEN profile.attrs.ChilLiviInHous = 'true' THEN TRUE
# MAGIC       WHEN profile.attrs.ChilLiviInHous = 'false' THEN FALSE
# MAGIC       ELSE NULL
# MAGIC     END as profile_has_children,
# MAGIC     profile.attrs.ownCar as profile_owns_car,
# MAGIC     CAST(profile.firstVisit AS TIMESTAMP) as profile_first_visit,
# MAGIC     CAST(profile.lastVisit AS TIMESTAMP) as profile_last_visit,
# MAGIC     
# MAGIC     -- Data quality
# MAGIC     _corrupt_record IS NOT NULL as has_corrupt_record,
# MAGIC     
# MAGIC     -- Metadata
# MAGIC     INPUT_FILE_NAME() as source_file,
# MAGIC     createDate as ingestion_timestamp,
# MAGIC     CURRENT_TIMESTAMP() as processing_timestamp,
# MAGIC     CURRENT_DATE() as silver_load_date
# MAGIC     
# MAGIC   FROM conversion_logic
# MAGIC ),
# MAGIC
# MAGIC data_quality_scored AS (
# MAGIC   -- Step 4: Calculate data quality score
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     -- Comprehensive data quality score (0-1)
# MAGIC     CAST((
# MAGIC       -- Critical fields (0.65 total)
# MAGIC       CASE WHEN customer_key IS NOT NULL THEN 0.20 ELSE 0.0 END +
# MAGIC       CASE WHEN event_timestamp IS NOT NULL THEN 0.20 ELSE 0.0 END +
# MAGIC       CASE WHEN campaign_id IS NOT NULL THEN 0.15 ELSE 0.0 END +
# MAGIC       CASE WHEN device_type IS NOT NULL THEN 0.10 ELSE 0.0 END +
# MAGIC
# MAGIC       -- Identity quality bonus (0.15 total)
# MAGIC       CASE
# MAGIC         WHEN customer_key_source = 'profile_id' THEN 0.15
# MAGIC         WHEN customer_key_source = 'fluent_id' THEN 0.12
# MAGIC         WHEN customer_key_source = 'email_sha256' THEN 0.10
# MAGIC         WHEN customer_key_source = 'email_md5' THEN 0.08
# MAGIC         WHEN customer_key_source = 'phone_sha256' THEN 0.08
# MAGIC         ELSE 0.0
# MAGIC       END +
# MAGIC
# MAGIC       -- Important supporting fields (0.20 total)
# MAGIC       CASE WHEN advertiser_id IS NOT NULL THEN 0.05 ELSE 0.0 END +
# MAGIC       CASE WHEN creative_id IS NOT NULL THEN 0.05 ELSE 0.0 END +
# MAGIC       CASE WHEN country IS NOT NULL THEN 0.05 ELSE 0.0 END +
# MAGIC       CASE WHEN browser_name IS NOT NULL THEN 0.05 ELSE 0.0 END
# MAGIC     ) AS DOUBLE) as data_quality_score
# MAGIC     
# MAGIC   FROM flattened_data
# MAGIC ),
# MAGIC
# MAGIC deduped AS (
# MAGIC   -- Step 5: Deduplicate by event_pk (keeping most recent by createDate)
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY event_pk 
# MAGIC       ORDER BY ingestion_timestamp DESC
# MAGIC     ) as row_num
# MAGIC   FROM data_quality_scored
# MAGIC ),
# MAGIC
# MAGIC final_filtered AS (
# MAGIC   -- Step 6: Apply final filters
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     row_num > 1 as is_duplicate
# MAGIC   FROM deduped
# MAGIC   WHERE row_num = 1  -- Keep only first occurrence
# MAGIC     AND is_bot = FALSE  -- Remove bot traffic
# MAGIC     AND data_quality_score >= 0.5  -- Minimum quality threshold
# MAGIC     AND event_timestamp >= '2020-01-01'  -- Valid date range
# MAGIC     AND event_timestamp <= CURRENT_TIMESTAMP()  -- No future dates
# MAGIC     AND NOT (
# MAGIC       LOWER(COALESCE(campaign_name, '')) LIKE '%test%'  -- Remove test campaigns
# MAGIC       OR LOWER(COALESCE(advertiser_name, '')) LIKE '%test%'
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC -- Final SELECT with partitioning columns
# MAGIC SELECT 
# MAGIC   -- Primary keys and identifiers
# MAGIC   event_pk,
# MAGIC   offer_trace_id,
# MAGIC   session_id,
# MAGIC   source_reference_id,
# MAGIC   source_reference,
# MAGIC   
# MAGIC   -- User identity (will be updated with phone hash in next cell)
# MAGIC   customer_key,
# MAGIC   customer_key_source,
# MAGIC   profile_id,
# MAGIC   fluent_id,
# MAGIC   email_sha256,
# MAGIC   email_md5,
# MAGIC   phone_raw,  -- Temporary, will be replaced with normalized/hashed versions
# MAGIC   is_identified_user,
# MAGIC   is_anonymous_user,
# MAGIC   has_profile_id,
# MAGIC   has_email_identifier,
# MAGIC   has_phone_raw,
# MAGIC   
# MAGIC   -- Timestamps
# MAGIC   event_timestamp,
# MAGIC   event_date_est,
# MAGIC   event_hour_est,
# MAGIC   local_hour_of_day,
# MAGIC   day_of_week,
# MAGIC   is_business_hours,
# MAGIC   is_weekend,
# MAGIC   
# MAGIC   -- Event classification
# MAGIC   event_type,
# MAGIC   product_scope,
# MAGIC   is_p1_view,
# MAGIC   is_conversion_event,
# MAGIC   is_transaction_event,
# MAGIC   
# MAGIC   -- Campaign dimensions
# MAGIC   advertiser_id,
# MAGIC   advertiser_name,
# MAGIC   campaign_id,
# MAGIC   campaign_name,
# MAGIC   campaign_type,
# MAGIC   campaign_vertical,
# MAGIC   campaign_sub_vertical,
# MAGIC   adgroup_id,
# MAGIC   adgroup_name,
# MAGIC   creative_id,
# MAGIC   creative_name,
# MAGIC   tracking_id,
# MAGIC   
# MAGIC   -- Conversion data
# MAGIC   conversion_type,
# MAGIC   conversion_type_name,
# MAGIC   conversion_goal_id,
# MAGIC   order_id,
# MAGIC   revenue,
# MAGIC   gross_revenue,
# MAGIC   adjusted_revenue,
# MAGIC   sale_amount,
# MAGIC   currency,
# MAGIC   
# MAGIC   -- Device and context
# MAGIC   device_type,
# MAGIC   device_os,
# MAGIC   device_os_version,
# MAGIC   device_brand,
# MAGIC   device_model,
# MAGIC   browser_name,
# MAGIC   browser_version,
# MAGIC   is_mobile,
# MAGIC   is_tablet,
# MAGIC   is_desktop,
# MAGIC   is_bot,
# MAGIC   
# MAGIC   -- Geographic data
# MAGIC   country,
# MAGIC   state,
# MAGIC   city,
# MAGIC   zip,
# MAGIC   
# MAGIC   -- Traffic source
# MAGIC   partner_id,
# MAGIC   partner_name,
# MAGIC   source_id,
# MAGIC   traffic_partner_type,
# MAGIC   subaff1,
# MAGIC   subaff2,
# MAGIC   subaff3,
# MAGIC   subaff4,
# MAGIC   
# MAGIC   -- Session context
# MAGIC   referer,
# MAGIC   user_agent,
# MAGIC   
# MAGIC   -- Profile attributes
# MAGIC   profile_gender,
# MAGIC   profile_home_owner,
# MAGIC   profile_has_children,
# MAGIC   profile_owns_car,
# MAGIC   profile_first_visit,
# MAGIC   profile_last_visit,
# MAGIC   
# MAGIC   -- Data quality
# MAGIC   has_corrupt_record,
# MAGIC   is_duplicate,
# MAGIC   data_quality_score,
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   source_file,
# MAGIC   ingestion_timestamp,
# MAGIC   processing_timestamp,
# MAGIC   silver_load_date,
# MAGIC   
# MAGIC   -- Partitioning columns
# MAGIC   event_date_est as date_est,
# MAGIC   event_hour_est as hour_est
# MAGIC   
# MAGIC FROM final_filtered;

# COMMAND ----------

display(spark.table("silver_customer_events_transformed").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply Phone Normalization and Hash (Python UDFs)

# COMMAND ----------

from pyspark.sql import functions as F

# Read the SQL transformation
df_transformed = spark.table("silver_customer_events_transformed")

# Apply phone normalization and hashing
df_with_phone = df_transformed \
    .withColumn("phone_normalized_value", normalize_phone(F.col("phone_raw"))) \
    .withColumn("phone_sha256_hash", hash_phone(F.col("phone_raw"))) \
    .withColumn("has_phone_identifier", F.col("phone_normalized_value").isNotNull())

# Update customer_key and customer_key_source if phone is only available identifier
df_final = df_with_phone \
    .withColumn(
        "customer_key",
        F.when(
            (F.col("customer_key_source") == "session_id") & F.col("phone_sha256_hash").isNotNull(),
            F.col("phone_sha256_hash")
        ).otherwise(F.col("customer_key"))
    ) \
    .withColumn(
        "customer_key_source",
        F.when(
            (F.col("customer_key_source") == "session_id") & F.col("phone_sha256_hash").isNotNull(),
            "phone_sha256"
        ).otherwise(F.col("customer_key_source"))
    ) \
    .withColumn(
        "is_identified_user",
        F.when(
            F.col("phone_sha256_hash").isNotNull(),
            True
        ).otherwise(F.col("is_identified_user"))
    ) \
    .withColumn(
        "is_anonymous_user",
        F.when(
            F.col("phone_sha256_hash").isNotNull(),
            False
        ).otherwise(F.col("is_anonymous_user"))
    ) \
    .drop("phone_raw")  # Drop raw phone for PII compliance

# Register as temp view for next steps
df_final.createOrReplaceTempView("silver_customer_events_final")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Delta Table with MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create target table if not exists
# MAGIC CREATE TABLE IF NOT EXISTS silver_customer_events_enriched (
# MAGIC   event_pk STRING,
# MAGIC   offer_trace_id STRING,
# MAGIC   session_id STRING,
# MAGIC   source_reference_id STRING,
# MAGIC   source_reference STRING,
# MAGIC   customer_key STRING,
# MAGIC   customer_key_source STRING,
# MAGIC   profile_id STRING,
# MAGIC   fluent_id STRING,
# MAGIC   email_sha256 STRING,
# MAGIC   email_md5 STRING,
# MAGIC   phone_sha256_hash STRING,
# MAGIC   phone_normalized_value STRING,
# MAGIC   is_identified_user BOOLEAN,
# MAGIC   is_anonymous_user BOOLEAN,
# MAGIC   has_profile_id BOOLEAN,
# MAGIC   has_email_identifier BOOLEAN,
# MAGIC   has_phone_identifier BOOLEAN,
# MAGIC   event_timestamp TIMESTAMP,
# MAGIC   event_date_est DATE,
# MAGIC   event_hour_est INT,
# MAGIC   local_hour_of_day INT,
# MAGIC   day_of_week STRING,
# MAGIC   is_business_hours BOOLEAN,
# MAGIC   is_weekend BOOLEAN,
# MAGIC   event_type STRING,
# MAGIC   product_scope STRING,
# MAGIC   is_p1_view BOOLEAN,
# MAGIC   is_conversion_event BOOLEAN,
# MAGIC   is_transaction_event BOOLEAN,
# MAGIC   advertiser_id STRING,
# MAGIC   advertiser_name STRING,
# MAGIC   campaign_id STRING,
# MAGIC   campaign_name STRING,
# MAGIC   campaign_type STRING,
# MAGIC   campaign_vertical STRING,
# MAGIC   campaign_sub_vertical STRING,
# MAGIC   adgroup_id STRING,
# MAGIC   adgroup_name STRING,
# MAGIC   creative_id STRING,
# MAGIC   creative_name STRING,
# MAGIC   tracking_id STRING,
# MAGIC   conversion_type STRING,
# MAGIC   conversion_type_name STRING,
# MAGIC   conversion_goal_id STRING,
# MAGIC   order_id STRING,
# MAGIC   revenue DECIMAL(19,4),
# MAGIC   gross_revenue DECIMAL(19,4),
# MAGIC   adjusted_revenue DECIMAL(19,4),
# MAGIC   sale_amount DECIMAL(19,4),
# MAGIC   currency STRING,
# MAGIC   device_type STRING,
# MAGIC   device_os STRING,
# MAGIC   device_os_version STRING,
# MAGIC   device_brand STRING,
# MAGIC   device_model STRING,
# MAGIC   browser_name STRING,
# MAGIC   browser_version STRING,
# MAGIC   is_mobile BOOLEAN,
# MAGIC   is_tablet BOOLEAN,
# MAGIC   is_desktop BOOLEAN,
# MAGIC   is_bot BOOLEAN,
# MAGIC   country STRING,
# MAGIC   state STRING,
# MAGIC   city STRING,
# MAGIC   zip STRING,
# MAGIC   partner_id STRING,
# MAGIC   partner_name STRING,
# MAGIC   source_id STRING,
# MAGIC   traffic_partner_type STRING,
# MAGIC   subaff1 STRING,
# MAGIC   subaff2 STRING,
# MAGIC   subaff3 STRING,
# MAGIC   subaff4 STRING,
# MAGIC   referer STRING,
# MAGIC   user_agent STRING,
# MAGIC   profile_gender STRING,
# MAGIC   profile_home_owner STRING,
# MAGIC   profile_has_children BOOLEAN,
# MAGIC   profile_owns_car STRING,
# MAGIC   profile_first_visit TIMESTAMP,
# MAGIC   profile_last_visit TIMESTAMP,
# MAGIC   has_corrupt_record BOOLEAN,
# MAGIC   is_duplicate BOOLEAN,
# MAGIC   data_quality_score DOUBLE,
# MAGIC   source_file STRING,
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   processing_timestamp TIMESTAMP,
# MAGIC   silver_load_date DATE,
# MAGIC   date_est DATE,
# MAGIC   hour_est INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_est, hour_est);

# COMMAND ----------

# Write data based on RUN_MODE
if RUN_MODE == "FULL_REFRESH":
    # ==========================================================================
    # FULL REFRESH MODE: Overwrite table or specific date partitions
    # ==========================================================================
    print(f"FULL REFRESH: Overwriting table {TARGET_TABLE}")

    df_to_write = spark.table("silver_customer_events_final")

    if START_DATE:
        # Overwrite only specific partitions based on date range
        # Note: replaceWhere doesn't support overwriteSchema, so schema must match
        # Use mergeSchema to handle minor schema differences
        df_to_write.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"date_est >= '{START_DATE}'") \
            .option("mergeSchema", "true") \
            .saveAsTable(TARGET_TABLE)
        print(f"Partitions replaced for dates >= {START_DATE}")
    else:
        # Full table overwrite - drop and recreate to force schema alignment
        print("Dropping existing table for full schema refresh...")
        spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
        df_to_write.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("date_est", "hour_est") \
            .saveAsTable(TARGET_TABLE)
        print("Full table overwritten with new schema")

    record_count = df_to_write.count()
    print(f"FULL REFRESH completed with {record_count:,} records")

else:
    # ==========================================================================
    # INCREMENTAL MODE: MERGE new/updated records
    # ==========================================================================
    print(f"INCREMENTAL: Performing MERGE into {TARGET_TABLE}")

    spark.sql("""
        MERGE INTO silver_customer_events_enriched target
        USING silver_customer_events_final source
        ON target.event_pk = source.event_pk
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("INCREMENTAL MERGE completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Optimize Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze table for query optimization
# MAGIC ANALYZE TABLE silver_customer_events_enriched COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Update Watermark

# COMMAND ----------

# Update watermark (only for INCREMENTAL mode)
if RUN_MODE == "INCREMENTAL":
    spark.sql("""
        MERGE INTO silver_customer_events_watermark target
        USING (
            SELECT
                'silver_customer_events_enriched' as table_name,
                MAX(event_timestamp) as last_processed_timestamp,
                MAX(event_date_est) as last_processed_date,
                CURRENT_TIMESTAMP() as updated_at
            FROM silver_customer_events_enriched
            WHERE date_est >= CURRENT_DATE - 7
        ) source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Watermark updated successfully")
else:
    print("FULL REFRESH mode: Watermark NOT updated (preserving for future incremental runs)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Data Quality Checks and Reporting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily summary with identity resolution breakdown
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   COUNT(*) as total_events,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT session_id) as unique_sessions,
# MAGIC   
# MAGIC   -- Identity resolution breakdown
# MAGIC   SUM(CASE WHEN customer_key_source = 'profile_id' THEN 1 ELSE 0 END) as profile_id_events,
# MAGIC   SUM(CASE WHEN customer_key_source = 'fluent_id' THEN 1 ELSE 0 END) as fluent_id_events,
# MAGIC   SUM(CASE WHEN customer_key_source = 'email_sha256' THEN 1 ELSE 0 END) as email_sha256_events,
# MAGIC   SUM(CASE WHEN customer_key_source = 'email_md5' THEN 1 ELSE 0 END) as email_md5_events,
# MAGIC   SUM(CASE WHEN customer_key_source = 'phone_sha256' THEN 1 ELSE 0 END) as phone_events,
# MAGIC   SUM(CASE WHEN customer_key_source = 'session_id' THEN 1 ELSE 0 END) as anonymous_events,
# MAGIC   
# MAGIC   -- User identification rates
# MAGIC   ROUND(SUM(CASE WHEN is_identified_user THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_identified,
# MAGIC   ROUND(SUM(CASE WHEN has_profile_id THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_with_profile,
# MAGIC   ROUND(SUM(CASE WHEN has_email_identifier THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_with_email,
# MAGIC   ROUND(SUM(CASE WHEN has_phone_identifier THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_with_phone,
# MAGIC   
# MAGIC   -- Event type breakdown
# MAGIC   SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) as conversion_events,
# MAGIC   SUM(CASE WHEN event_type = 'transaction' THEN 1 ELSE 0 END) as transaction_events,
# MAGIC   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as click_events,
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as view_events,
# MAGIC   
# MAGIC   -- Conversion metrics (matching fact table logic: sourceReference = 'offer-convert')
# MAGIC   COALESCE(
# MAGIC     COUNT(DISTINCT CASE 
# MAGIC       WHEN source_reference = 'offer-convert' 
# MAGIC       AND conversion_type_name != 'Click' 
# MAGIC       THEN source_reference_id 
# MAGIC     END),
# MAGIC     0
# MAGIC   ) as conversions,
# MAGIC   
# MAGIC   ROUND(
# MAGIC     COALESCE(
# MAGIC       COUNT(DISTINCT CASE 
# MAGIC         WHEN source_reference = 'offer-convert' 
# MAGIC         AND conversion_type_name != 'Click' 
# MAGIC         THEN source_reference_id 
# MAGIC       END),
# MAGIC       0
# MAGIC     ) * 100.0 / NULLIF(COUNT(*), 0), 
# MAGIC     2
# MAGIC   ) as conversion_rate,
# MAGIC   
# MAGIC   ROUND(SUM(CASE WHEN is_p1_view THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as p1_view_rate,
# MAGIC   
# MAGIC   -- Revenue
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)), 2) as total_revenue,
# MAGIC   ROUND(AVG(CASE WHEN revenue > 0 THEN revenue ELSE NULL END), 2) as avg_revenue_per_conversion,
# MAGIC   
# MAGIC   -- Data quality
# MAGIC   ROUND(AVG(data_quality_score), 3) as avg_quality_score,
# MAGIC   SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END) as duplicate_events
# MAGIC   
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY date_est
# MAGIC ORDER BY date_est DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identity resolution effectiveness analysis
# MAGIC SELECT
# MAGIC   customer_key_source,
# MAGIC   COUNT(*) as event_count,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_events,
# MAGIC   ROUND(AVG(data_quality_score), 3) as avg_quality_score,
# MAGIC   -- Conversion count using fact table logic
# MAGIC   COALESCE(
# MAGIC     COUNT(DISTINCT CASE 
# MAGIC       WHEN source_reference = 'offer-convert' 
# MAGIC       AND conversion_type_name != 'Click' 
# MAGIC       THEN source_reference_id 
# MAGIC     END),
# MAGIC     0
# MAGIC   ) as conversions,
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)), 2) as total_revenue
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY customer_key_source
# MAGIC ORDER BY event_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Multi-identifier coverage analysis
# MAGIC WITH customer_pii AS (
# MAGIC   SELECT DISTINCT
# MAGIC     customer_key,
# MAGIC     MAX(has_profile_id) as has_profile_id,
# MAGIC     MAX(has_email_identifier) as has_email_identifier,
# MAGIC     MAX(has_phone_identifier) as has_phone_identifier
# MAGIC   FROM silver_customer_events_enriched
# MAGIC   WHERE date_est >= CURRENT_DATE - 7
# MAGIC   GROUP BY customer_key
# MAGIC )
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN has_profile_id THEN 1 ELSE 0 END) as customers_with_profile_id,
# MAGIC   SUM(CASE WHEN has_email_identifier THEN 1 ELSE 0 END) as customers_with_email,
# MAGIC   SUM(CASE WHEN has_phone_identifier THEN 1 ELSE 0 END) as customers_with_phone,
# MAGIC   SUM(CASE WHEN has_profile_id AND has_email_identifier THEN 1 ELSE 0 END) as profile_and_email,
# MAGIC   SUM(CASE WHEN has_profile_id AND has_phone_identifier THEN 1 ELSE 0 END) as profile_and_phone,
# MAGIC   SUM(CASE WHEN has_email_identifier AND has_phone_identifier THEN 1 ELSE 0 END) as email_and_phone,
# MAGIC   SUM(CASE WHEN has_profile_id AND has_email_identifier AND has_phone_identifier THEN 1 ELSE 0 END) as all_three_identifiers,
# MAGIC   COUNT(*) as total_unique_customers
# MAGIC FROM customer_pii;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Conversion funnel analysis (matching fact table metrics)
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN event_type = 'view' THEN customer_key END) as viewers,
# MAGIC   COUNT(DISTINCT CASE WHEN event_type = 'click' THEN customer_key END) as clickers,
# MAGIC   COUNT(DISTINCT CASE WHEN is_conversion_event THEN customer_key END) as converters,
# MAGIC   COUNT(DISTINCT CASE WHEN is_transaction_event THEN customer_key END) as buyers,
# MAGIC   
# MAGIC   -- Conversion count using fact table definition
# MAGIC   COALESCE(
# MAGIC     COUNT(DISTINCT CASE 
# MAGIC       WHEN source_reference = 'offer-convert' 
# MAGIC       AND conversion_type_name != 'Click' 
# MAGIC       THEN source_reference_id 
# MAGIC     END),
# MAGIC     0
# MAGIC   ) as conversions_fact_definition,
# MAGIC   
# MAGIC   -- Conversion rates
# MAGIC   ROUND(
# MAGIC     COUNT(DISTINCT CASE WHEN event_type = 'click' THEN customer_key END) * 100.0 /
# MAGIC     NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'view' THEN customer_key END), 0),
# MAGIC     2
# MAGIC   ) as view_to_click_rate,
# MAGIC   
# MAGIC   ROUND(
# MAGIC     COUNT(DISTINCT CASE WHEN is_conversion_event THEN customer_key END) * 100.0 /
# MAGIC     NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'click' THEN customer_key END), 0),
# MAGIC     2
# MAGIC   ) as click_to_conversion_rate,
# MAGIC   
# MAGIC   -- Revenue metrics
# MAGIC   SUM(COALESCE(revenue, 0)) as total_revenue,
# MAGIC   COUNT(DISTINCT CASE WHEN revenue > 0 THEN order_id END) as orders_with_revenue
# MAGIC   
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY date_est
# MAGIC ORDER BY date_est DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Create Identity Resolution Audit Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create audit table for ongoing monitoring
# MAGIC CREATE OR REPLACE TABLE silver_identity_resolution_audit
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   date_est as audit_date,
# MAGIC   customer_key_source,
# MAGIC   COUNT(*) as event_count,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT session_id) as unique_sessions,
# MAGIC   ROUND(AVG(data_quality_score), 3) as avg_quality_score,
# MAGIC   -- Conversion count using fact table logic
# MAGIC   COALESCE(
# MAGIC     COUNT(DISTINCT CASE 
# MAGIC       WHEN source_reference = 'offer-convert' 
# MAGIC       AND conversion_type_name != 'Click' 
# MAGIC       THEN source_reference_id 
# MAGIC     END),
# MAGIC     0
# MAGIC   ) as conversions,
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)), 2) as total_revenue,
# MAGIC   CURRENT_TIMESTAMP() as audit_timestamp
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 30
# MAGIC GROUP BY date_est, customer_key_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent audit results
# MAGIC SELECT * 
# MAGIC FROM silver_identity_resolution_audit
# MAGIC ORDER BY audit_date DESC, event_count DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Job Completion Summary

# COMMAND ----------

from datetime import datetime

# Get summary statistics
summary_stats = spark.sql("""
  SELECT
    COUNT(*) as total_processed,
    COUNT(DISTINCT customer_key) as unique_customers,
    COUNT(DISTINCT session_id) as unique_sessions,
    SUM(CASE WHEN is_conversion_event THEN 1 ELSE 0 END) as total_conversions,
    COALESCE(
      COUNT(DISTINCT CASE 
        WHEN source_reference = 'offer-convert' 
        AND conversion_type_name != 'Click' 
        THEN source_reference_id 
      END),
      0
    ) as conversions_fact_definition,
    ROUND(SUM(COALESCE(revenue, 0)), 2) as total_revenue,
    ROUND(AVG(data_quality_score), 3) as avg_quality_score
  FROM silver_customer_events_enriched
  WHERE date_est >= CURRENT_DATE - 1
""").collect()[0]

# Get identity breakdown
identity_breakdown = spark.sql("""
  SELECT 
    customer_key_source,
    COUNT(*) as count
  FROM silver_customer_events_enriched
  WHERE date_est >= CURRENT_DATE - 1
  GROUP BY customer_key_source
  ORDER BY count DESC
""").collect()

# Print completion summary
print("=" * 80)
print("SILVER LAYER - CUSTOMER EVENTS ENRICHED - JOB COMPLETED")
print("=" * 80)
print(f"Run Mode: {RUN_MODE}")
if RUN_MODE == "FULL_REFRESH":
    print(f"Date Range: {START_DATE or 'default'} to {END_DATE or 'now'}")
print(f"Total Events Processed: {summary_stats['total_processed']:,}")
print(f"Unique Customers: {summary_stats['unique_customers']:,}")
print(f"Unique Sessions: {summary_stats['unique_sessions']:,}")
print(f"Total Conversions (is_conversion_event): {summary_stats['total_conversions']:,}")
print(f"Conversions (Fact Table Definition): {summary_stats['conversions_fact_definition']:,}")
print(f"Total Revenue: ${summary_stats['total_revenue']:,}")
print(f"Avg Quality Score: {summary_stats['avg_quality_score']}")
print("")
print("IDENTITY RESOLUTION BREAKDOWN:")
for row in identity_breakdown:
    source = row['customer_key_source']
    count = row['count']
    pct = (count / summary_stats['total_processed']) * 100
    print(f"  {source}: {count:,} ({pct:.1f}%)")
print("")
print(f"Processing Completed: {datetime.now()}")
print("=" * 80)

# Return success
identity_summary = {row['customer_key_source']: row['count'] for row in identity_breakdown}
dbutils.notebook.exit(f"Success: {RUN_MODE} - Processed {summary_stats['total_processed']:,} events. Conversions: {summary_stats['conversions_fact_definition']:,}. Identity: {identity_summary}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Perfect! I've updated the notebook with the correct conversion logic:
# MAGIC
# MAGIC ## Key Changes:
# MAGIC
# MAGIC 1. **Conversion Logic**: Now properly identifies conversions as:
# MAGIC    ```sql
# MAGIC    sourceReference = 'offer-convert' 
# MAGIC    AND conversion_type_name != 'Click'
# MAGIC    ```
# MAGIC
# MAGIC 2. **Added `conversion_type_name` field**: Extracted from `campaignData.conversionTypeName`
# MAGIC
# MAGIC 3. **Updated all conversion metrics** to use the fact table definition consistently throughout:
# MAGIC    - Data quality checks
# MAGIC    - Conversion funnel analysis
# MAGIC    - Identity resolution audit
# MAGIC    - Job completion summary
# MAGIC
# MAGIC 4. **Two conversion metrics** are now tracked:
# MAGIC    - `is_conversion_event` (boolean flag using the correct logic)
# MAGIC    - `conversions_fact_definition` (distinct count of `source_reference_id` where conditions match)
# MAGIC
# MAGIC This now aligns perfectly with your existing fact tables' conversion logic! Would you like me to proceed with the next notebooks (Silver Sessions and Gold Customer 360)?