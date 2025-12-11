# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Customer Sessions Enriched
# MAGIC
# MAGIC **Purpose:** Aggregate event-level data into session-level metrics for customer behavior analysis:
# MAGIC - Session-level engagement metrics (duration, depth, events)
# MAGIC - Conversion and revenue aggregation by session
# MAGIC - Device switching and cross-device behavior
# MAGIC - Traffic source attribution
# MAGIC - Temporal patterns by session
# MAGIC
# MAGIC **Source:** silver_customer_events_enriched  
# MAGIC **Target:** silver_customer_sessions_enriched  
# MAGIC **Schedule:** Every 30 minutes (after events enrichment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

# Configuration
SOURCE_TABLE = "centraldata_sandbox.test.silver_customer_events_enriched"
TARGET_TABLE = "centraldata_sandbox.test.silver_customer_sessions_enriched"
CHECKPOINT_TABLE = "centraldata_sandbox.test.silver_customer_sessions_watermark"

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
# MAGIC CREATE TABLE IF NOT EXISTS silver_customer_sessions_watermark (
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
# MAGIC MERGE INTO silver_customer_sessions_watermark target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     'silver_customer_sessions_enriched' as table_name,
# MAGIC     CAST('2025-01-01 00:00:00' AS TIMESTAMP) as last_processed_timestamp,
# MAGIC     CAST('2025-01-01' AS DATE) as last_processed_date,
# MAGIC     CURRENT_TIMESTAMP() as updated_at
# MAGIC ) source
# MAGIC ON target.table_name = source.table_name
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# Determine date range based on RUN_MODE
if RUN_MODE == "FULL_REFRESH":
    # Full refresh mode - use provided date range or defaults
    if START_DATE:
        start_ts = f"CAST('{START_DATE}' AS DATE)"
    else:
        # Default: process last 365 days for full refresh
        start_ts = "CURRENT_DATE - INTERVAL 365 DAYS"

    if END_DATE:
        end_ts = f"CAST('{END_DATE}' AS DATE)"
    else:
        end_ts = "CURRENT_DATE"

    # Create watermark view for FULL_REFRESH with date range
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW last_watermark_sessions AS
        SELECT
            {start_ts} as watermark_ts,
            {end_ts} as end_watermark_ts
    """)
    print(f"FULL REFRESH MODE: Processing sessions from {START_DATE or 'last 365 days'} to {END_DATE or 'now'}")

else:
    # Incremental mode - use watermark table
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW last_watermark_sessions AS
        SELECT
            COALESCE(CAST(last_processed_date AS DATE), CURRENT_DATE - INTERVAL 2 DAYS) as watermark_ts,
            CAST(NULL AS DATE) as end_watermark_ts
        FROM silver_customer_sessions_watermark
        WHERE table_name = 'silver_customer_sessions_enriched'
    """)
    print("INCREMENTAL MODE: Using watermark table for processing window")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview watermark values
# MAGIC SELECT * FROM last_watermark_sessions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Main Session Aggregation - SQL-Based

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aggregate events into sessions with comprehensive metrics
# MAGIC CREATE OR REPLACE TEMP VIEW silver_customer_sessions_transformed AS
# MAGIC
# MAGIC WITH sessions_filtered AS (
# MAGIC   -- Get events for sessions we need to process/update
# MAGIC   -- Filter based on run mode (incremental or full refresh with date range)
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     from_utc_timestamp(event_timestamp, 'America/New_York') as event_timestamp_est
# MAGIC   FROM silver_customer_events_enriched
# MAGIC   WHERE date_est >= (SELECT watermark_ts FROM last_watermark_sessions)
# MAGIC     AND (
# MAGIC       (SELECT end_watermark_ts FROM last_watermark_sessions) IS NULL  -- Incremental: no end date
# MAGIC       OR date_est <= (SELECT end_watermark_ts FROM last_watermark_sessions)  -- Full refresh: apply end date
# MAGIC     )
# MAGIC ),
# MAGIC
# MAGIC session_aggregated AS (
# MAGIC   -- Main session-level aggregation
# MAGIC   SELECT
# MAGIC     -- Primary Keys
# MAGIC     SHA2(CONCAT(session_id, '_', CAST(MIN(event_date_est) AS STRING)), 256) as session_pk,
# MAGIC     session_id,
# MAGIC     customer_key,
# MAGIC     
# MAGIC     -- Session Timing (using EST timestamps)
# MAGIC     MIN(event_date_est) as session_date_est,
# MAGIC     MIN(event_timestamp_est) as session_start_timestamp_est,
# MAGIC     MAX(event_timestamp_est) as session_end_timestamp_est,
# MAGIC     CAST((UNIX_TIMESTAMP(MAX(event_timestamp_est)) - UNIX_TIMESTAMP(MIN(event_timestamp_est))) AS BIGINT) as session_duration_seconds,
# MAGIC     CAST(HOUR(MIN(event_timestamp_est)) AS INT) as session_hour_est,
# MAGIC     DATE_FORMAT(MIN(event_timestamp_est), 'EEEE') as day_of_week,
# MAGIC     HOUR(MIN(event_timestamp_est)) BETWEEN 9 AND 16 as is_business_hours,
# MAGIC     DAYOFWEEK(MIN(event_date_est)) IN (1, 7) as is_weekend,
# MAGIC     
# MAGIC     -- User Context
# MAGIC     MAX(is_identified_user) as is_identified_session,
# MAGIC     MIN(is_anonymous_user) as is_anonymous_session,
# MAGIC     MAX(profile_id) as profile_id,
# MAGIC     MAX(fluent_id) as fluent_id,
# MAGIC     
# MAGIC     -- Session Event Counts
# MAGIC     COUNT(*) as total_events,
# MAGIC     SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as view_events,
# MAGIC     SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as click_events,
# MAGIC     SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) as conversion_events,
# MAGIC     SUM(CASE WHEN is_transaction_event THEN 1 ELSE 0 END) as transaction_events,
# MAGIC     SUM(CASE WHEN is_p1_view THEN 1 ELSE 0 END) as p1_view_events,
# MAGIC     
# MAGIC     -- Engagement Metrics
# MAGIC     COUNT(DISTINCT campaign_id) as session_depth,
# MAGIC     COUNT(DISTINCT advertiser_id) as unique_advertisers_viewed,
# MAGIC     COUNT(DISTINCT creative_id) as unique_creatives_viewed,
# MAGIC     
# MAGIC     -- Time between events (avg seconds)
# MAGIC     CASE 
# MAGIC       WHEN COUNT(*) > 1 THEN 
# MAGIC         CAST((UNIX_TIMESTAMP(MAX(event_timestamp_est)) - UNIX_TIMESTAMP(MIN(event_timestamp_est))) AS DOUBLE) / (COUNT(*) - 1)
# MAGIC       ELSE NULL
# MAGIC     END as avg_time_between_events_sec,
# MAGIC     
# MAGIC     -- Conversion & Revenue
# MAGIC     MAX(is_conversion_event) as has_conversion,
# MAGIC     MAX(is_transaction_event) as has_transaction,
# MAGIC     CAST(SUM(COALESCE(revenue, 0)) AS DECIMAL(19,4)) as total_session_revenue,
# MAGIC     
# MAGIC     -- Distinct conversion count using fact table logic
# MAGIC     COUNT(DISTINCT CASE 
# MAGIC       WHEN source_reference = 'offer-convert' 
# MAGIC       AND conversion_type_name != 'Click' 
# MAGIC       THEN source_reference_id 
# MAGIC     END) as conversion_count,
# MAGIC     
# MAGIC     -- Transaction IDs array (filter out nulls)
# MAGIC     ARRAY_DISTINCT(FILTER(COLLECT_LIST(order_id), x -> x IS NOT NULL)) as transaction_ids,
# MAGIC     
# MAGIC     -- Conversion types array (filter out nulls)
# MAGIC     ARRAY_DISTINCT(FILTER(COLLECT_LIST(conversion_type_name), x -> x IS NOT NULL)) as conversion_types,
# MAGIC     
# MAGIC     -- Device & Geographic
# MAGIC     MODE(device_type) as primary_device_type,
# MAGIC     CAST(COUNT(DISTINCT device_type) - 1 AS INT) as device_switches,
# MAGIC     MAX(country) as country,
# MAGIC     MAX(state) as state,
# MAGIC     MAX(city) as city,
# MAGIC     
# MAGIC     -- Traffic Source
# MAGIC     MAX(partner_id) as partner_id,
# MAGIC     MAX(source_id) as source_id,
# MAGIC     MAX(traffic_partner_type) as traffic_partner_type,
# MAGIC     MAX(product_scope) as product_scope,
# MAGIC     
# MAGIC     -- Campaign Exposure (filter out nulls)
# MAGIC     ARRAY_DISTINCT(FILTER(COLLECT_LIST(campaign_id), x -> x IS NOT NULL)) as campaigns_viewed,
# MAGIC     ARRAY_DISTINCT(FILTER(COLLECT_LIST(advertiser_id), x -> x IS NOT NULL)) as advertisers_interacted,
# MAGIC     ARRAY_DISTINCT(FILTER(COLLECT_LIST(campaign_vertical), x -> x IS NOT NULL)) as verticals_explored,
# MAGIC     
# MAGIC     -- Metadata
# MAGIC     CURRENT_TIMESTAMP() as processing_timestamp,
# MAGIC     CURRENT_DATE() as silver_load_date
# MAGIC     
# MAGIC   FROM sessions_filtered
# MAGIC   WHERE session_id IS NOT NULL
# MAGIC   GROUP BY session_id, customer_key
# MAGIC ),
# MAGIC
# MAGIC sessions_with_engagement AS (
# MAGIC   -- Calculate engagement score
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     
# MAGIC     -- Engagement Score: Weighted combination of metrics (0-100 scale)
# MAGIC     LEAST(100, ROUND(
# MAGIC       -- Session duration component (max 30 points)
# MAGIC       (LEAST(session_duration_seconds, 600) / 600.0) * 30 +
# MAGIC       
# MAGIC       -- Events per session component (max 25 points)
# MAGIC       (LEAST(total_events, 20) / 20.0) * 25 +
# MAGIC       
# MAGIC       -- Click-through rate component (max 20 points)
# MAGIC       CASE WHEN view_events > 0 
# MAGIC         THEN (CAST(click_events AS DOUBLE) / view_events) * 20 
# MAGIC         ELSE 0 
# MAGIC       END +
# MAGIC       
# MAGIC       -- Conversion component (max 15 points)
# MAGIC       CASE WHEN has_conversion THEN 15 ELSE 0 END +
# MAGIC       
# MAGIC       -- Content diversity component (max 10 points)
# MAGIC       (LEAST(session_depth, 5) / 5.0) * 10
# MAGIC     , 2)) as engagement_score
# MAGIC     
# MAGIC   FROM session_aggregated
# MAGIC )
# MAGIC
# MAGIC -- Final SELECT with partitioning column
# MAGIC SELECT
# MAGIC   -- Primary Keys
# MAGIC   session_pk,
# MAGIC   session_id,
# MAGIC   customer_key,
# MAGIC   
# MAGIC   -- Session Timing (EST)
# MAGIC   session_date_est,
# MAGIC   session_start_timestamp_est as session_start_timestamp,
# MAGIC   session_end_timestamp_est as session_end_timestamp,
# MAGIC   session_duration_seconds,
# MAGIC   session_hour_est,
# MAGIC   day_of_week,
# MAGIC   is_business_hours,
# MAGIC   is_weekend,
# MAGIC   
# MAGIC   -- User Context
# MAGIC   is_identified_session,
# MAGIC   is_anonymous_session,
# MAGIC   profile_id,
# MAGIC   fluent_id,
# MAGIC   
# MAGIC   -- Session Metrics
# MAGIC   total_events,
# MAGIC   view_events,
# MAGIC   click_events,
# MAGIC   conversion_events,
# MAGIC   transaction_events,
# MAGIC   p1_view_events,
# MAGIC   
# MAGIC   -- Engagement Metrics
# MAGIC   session_depth,
# MAGIC   unique_advertisers_viewed,
# MAGIC   unique_creatives_viewed,
# MAGIC   avg_time_between_events_sec,
# MAGIC   engagement_score,
# MAGIC   
# MAGIC   -- Conversion & Revenue
# MAGIC   has_conversion,
# MAGIC   has_transaction,
# MAGIC   total_session_revenue,
# MAGIC   conversion_count,
# MAGIC   transaction_ids,
# MAGIC   conversion_types,
# MAGIC   
# MAGIC   -- Device & Geographic
# MAGIC   primary_device_type,
# MAGIC   device_switches,
# MAGIC   country,
# MAGIC   state,
# MAGIC   city,
# MAGIC   
# MAGIC   -- Traffic Source
# MAGIC   partner_id,
# MAGIC   source_id,
# MAGIC   traffic_partner_type,
# MAGIC   product_scope,
# MAGIC   
# MAGIC   -- Campaign Exposure
# MAGIC   campaigns_viewed,
# MAGIC   advertisers_interacted,
# MAGIC   verticals_explored,
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   processing_timestamp,
# MAGIC   silver_load_date,
# MAGIC   
# MAGIC   -- Partitioning column
# MAGIC   session_date_est as date_est
# MAGIC   
# MAGIC FROM sessions_with_engagement;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview transformed sessions
# MAGIC SELECT * FROM silver_customer_sessions_transformed LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Delta Table with MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create target table if not exists
# MAGIC CREATE TABLE IF NOT EXISTS silver_customer_sessions_enriched (
# MAGIC   session_pk STRING,
# MAGIC   session_id STRING,
# MAGIC   customer_key STRING,
# MAGIC   session_date_est DATE,
# MAGIC   session_start_timestamp TIMESTAMP,
# MAGIC   session_end_timestamp TIMESTAMP,
# MAGIC   session_duration_seconds BIGINT,
# MAGIC   session_hour_est INT,
# MAGIC   day_of_week STRING,
# MAGIC   is_business_hours BOOLEAN,
# MAGIC   is_weekend BOOLEAN,
# MAGIC   is_identified_session BOOLEAN,
# MAGIC   is_anonymous_session BOOLEAN,
# MAGIC   profile_id STRING,
# MAGIC   fluent_id STRING,
# MAGIC   total_events BIGINT,
# MAGIC   view_events BIGINT,
# MAGIC   click_events BIGINT,
# MAGIC   conversion_events BIGINT,
# MAGIC   transaction_events BIGINT,
# MAGIC   p1_view_events BIGINT,
# MAGIC   session_depth BIGINT,
# MAGIC   unique_advertisers_viewed BIGINT,
# MAGIC   unique_creatives_viewed BIGINT,
# MAGIC   avg_time_between_events_sec DOUBLE,
# MAGIC   engagement_score DOUBLE,
# MAGIC   has_conversion BOOLEAN,
# MAGIC   has_transaction BOOLEAN,
# MAGIC   total_session_revenue DECIMAL(19,4),
# MAGIC   conversion_count BIGINT,
# MAGIC   transaction_ids ARRAY<STRING>,
# MAGIC   conversion_types ARRAY<STRING>,
# MAGIC   primary_device_type STRING,
# MAGIC   device_switches INT,
# MAGIC   country STRING,
# MAGIC   state STRING,
# MAGIC   city STRING,
# MAGIC   partner_id STRING,
# MAGIC   source_id STRING,
# MAGIC   traffic_partner_type STRING,
# MAGIC   product_scope STRING,
# MAGIC   campaigns_viewed ARRAY<STRING>,
# MAGIC   advertisers_interacted ARRAY<STRING>,
# MAGIC   verticals_explored ARRAY<STRING>,
# MAGIC   processing_timestamp TIMESTAMP,
# MAGIC   silver_load_date DATE,
# MAGIC   date_est DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_est);

# COMMAND ----------

# Write data based on RUN_MODE
if RUN_MODE == "FULL_REFRESH":
    # ==========================================================================
    # FULL REFRESH MODE: Overwrite table or specific date partitions
    # ==========================================================================
    print(f"FULL REFRESH: Overwriting table {TARGET_TABLE}")

    df_to_write = spark.table("silver_customer_sessions_transformed")

    if START_DATE:
        # Overwrite only specific partitions based on date range
        df_to_write.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"date_est >= '{START_DATE}'") \
            .option("overwriteSchema", "true") \
            .saveAsTable(TARGET_TABLE)
        print(f"Partitions replaced for dates >= {START_DATE}")
    else:
        # Full table overwrite
        df_to_write.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(TARGET_TABLE)
        print("Full table overwritten")

    record_count = df_to_write.count()
    print(f"FULL REFRESH completed with {record_count:,} sessions")

else:
    # ==========================================================================
    # INCREMENTAL MODE: MERGE new/updated records
    # ==========================================================================
    print(f"INCREMENTAL: Performing MERGE into {TARGET_TABLE}")

    spark.sql("""
        MERGE INTO silver_customer_sessions_enriched target
        USING silver_customer_sessions_transformed source
        ON target.session_pk = source.session_pk
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("INCREMENTAL MERGE completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimize Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize with Z-ORDER on key query columns
# MAGIC OPTIMIZE silver_customer_sessions_enriched
# MAGIC ZORDER BY (customer_key, session_id, session_start_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze table for query optimization
# MAGIC ANALYZE TABLE silver_customer_sessions_enriched COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Update Watermark

# COMMAND ----------

# Update watermark (only for INCREMENTAL mode)
if RUN_MODE == "INCREMENTAL":
    spark.sql("""
        MERGE INTO silver_customer_sessions_watermark target
        USING (
            SELECT
                'silver_customer_sessions_enriched' as table_name,
                MAX(session_start_timestamp) as last_processed_timestamp,
                MAX(session_date_est) as last_processed_date,
                CURRENT_TIMESTAMP() as updated_at
            FROM silver_customer_sessions_enriched
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
# MAGIC ## 8. Data Quality Checks and Reporting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily session summary
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   COUNT(*) as total_sessions,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   
# MAGIC   -- Session characteristics
# MAGIC   ROUND(AVG(session_duration_seconds), 2) as avg_session_duration_sec,
# MAGIC   ROUND(AVG(total_events), 2) as avg_events_per_session,
# MAGIC   ROUND(AVG(session_depth), 2) as avg_session_depth,
# MAGIC   ROUND(AVG(engagement_score), 2) as avg_engagement_score,
# MAGIC   
# MAGIC   -- Conversion metrics
# MAGIC   SUM(CASE WHEN has_conversion THEN 1 ELSE 0 END) as sessions_with_conversion,
# MAGIC   SUM(conversion_count) as total_conversions,
# MAGIC   ROUND(SUM(CASE WHEN has_conversion THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as conversion_rate_pct,
# MAGIC   
# MAGIC   -- Revenue
# MAGIC   ROUND(SUM(total_session_revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(CASE WHEN total_session_revenue > 0 THEN total_session_revenue END), 2) as avg_revenue_per_converting_session,
# MAGIC   
# MAGIC   -- Device behavior
# MAGIC   SUM(CASE WHEN device_switches > 0 THEN 1 ELSE 0 END) as sessions_with_device_switch,
# MAGIC   ROUND(AVG(device_switches), 2) as avg_device_switches
# MAGIC   
# MAGIC FROM silver_customer_sessions_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY date_est
# MAGIC ORDER BY date_est DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Engagement score distribution
# MAGIC SELECT
# MAGIC   CASE 
# MAGIC     WHEN engagement_score >= 75 THEN 'High (75-100)'
# MAGIC     WHEN engagement_score >= 50 THEN 'Medium (50-74)'
# MAGIC     WHEN engagement_score >= 25 THEN 'Low (25-49)'
# MAGIC     ELSE 'Very Low (0-24)'
# MAGIC   END as engagement_tier,
# MAGIC   COUNT(*) as session_count,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   ROUND(AVG(session_duration_seconds), 2) as avg_duration_sec,
# MAGIC   ROUND(AVG(total_events), 2) as avg_events,
# MAGIC   SUM(conversion_count) as total_conversions,
# MAGIC   ROUND(SUM(total_session_revenue), 2) as total_revenue
# MAGIC FROM silver_customer_sessions_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY 
# MAGIC   CASE 
# MAGIC     WHEN engagement_score >= 75 THEN 'High (75-100)'
# MAGIC     WHEN engagement_score >= 50 THEN 'Medium (50-74)'
# MAGIC     WHEN engagement_score >= 25 THEN 'Low (25-49)'
# MAGIC     ELSE 'Very Low (0-24)'
# MAGIC   END
# MAGIC ORDER BY engagement_tier DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cross-device behavior analysis
# MAGIC SELECT
# MAGIC   primary_device_type,
# MAGIC   COUNT(*) as session_count,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   SUM(CASE WHEN device_switches > 0 THEN 1 ELSE 0 END) as sessions_with_switch,
# MAGIC   ROUND(AVG(engagement_score), 2) as avg_engagement,
# MAGIC   SUM(conversion_count) as conversions,
# MAGIC   ROUND(SUM(total_session_revenue), 2) as revenue
# MAGIC FROM silver_customer_sessions_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY primary_device_type
# MAGIC ORDER BY session_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Temporal patterns - Hourly distribution
# MAGIC SELECT
# MAGIC   session_hour_est,
# MAGIC   COUNT(*) as session_count,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   ROUND(AVG(engagement_score), 2) as avg_engagement,
# MAGIC   SUM(conversion_count) as conversions,
# MAGIC   ROUND(SUM(total_session_revenue), 2) as revenue
# MAGIC FROM silver_customer_sessions_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY session_hour_est
# MAGIC ORDER BY session_hour_est;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Day of week patterns
# MAGIC SELECT
# MAGIC   day_of_week,
# MAGIC   is_weekend,
# MAGIC   COUNT(*) as session_count,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   ROUND(AVG(engagement_score), 2) as avg_engagement,
# MAGIC   SUM(conversion_count) as conversions,
# MAGIC   ROUND(SUM(total_session_revenue), 2) as revenue
# MAGIC FROM silver_customer_sessions_enriched
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY day_of_week, is_weekend
# MAGIC ORDER BY 
# MAGIC   CASE day_of_week
# MAGIC     WHEN 'Monday' THEN 1
# MAGIC     WHEN 'Tuesday' THEN 2
# MAGIC     WHEN 'Wednesday' THEN 3
# MAGIC     WHEN 'Thursday' THEN 4
# MAGIC     WHEN 'Friday' THEN 5
# MAGIC     WHEN 'Saturday' THEN 6
# MAGIC     WHEN 'Sunday' THEN 7
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Job Completion Summary

# COMMAND ----------

from datetime import datetime

# Get summary statistics
summary_stats = spark.sql("""
  SELECT
    COUNT(*) as total_sessions,
    COUNT(DISTINCT customer_key) as unique_customers,
    ROUND(AVG(session_duration_seconds), 2) as avg_duration_sec,
    ROUND(AVG(total_events), 2) as avg_events_per_session,
    ROUND(AVG(engagement_score), 2) as avg_engagement_score,
    SUM(conversion_count) as total_conversions,
    ROUND(SUM(total_session_revenue), 2) as total_revenue
  FROM silver_customer_sessions_enriched
  WHERE date_est >= CURRENT_DATE - 1
""").collect()[0]

# Print completion summary
print("=" * 80)
print("SILVER LAYER - CUSTOMER SESSIONS ENRICHED - JOB COMPLETED")
print("=" * 80)
print(f"Run Mode: {RUN_MODE}")
if RUN_MODE == "FULL_REFRESH":
    print(f"Date Range: {START_DATE or 'default'} to {END_DATE or 'now'}")
print(f"Total Sessions Processed: {summary_stats['total_sessions']:,}")
print(f"Unique Customers: {summary_stats['unique_customers']:,}")
print(f"Avg Session Duration: {summary_stats['avg_duration_sec']:.2f} seconds")
print(f"Avg Events Per Session: {summary_stats['avg_events_per_session']:.2f}")
print(f"Avg Engagement Score: {summary_stats['avg_engagement_score']:.2f}")
print(f"Total Conversions: {summary_stats['total_conversions']:,}")
print(f"Total Revenue: ${summary_stats['total_revenue']:,.2f}")
print("")
print(f"Processing Completed: {datetime.now()}")
print("=" * 80)

# Return success
dbutils.notebook.exit(f"Success: {RUN_MODE} - Processed {summary_stats['total_sessions']:,} sessions for {summary_stats['unique_customers']:,} customers")


