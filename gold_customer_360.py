# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Customer 360 Daily
# MAGIC
# MAGIC **Purpose:** Build comprehensive customer-day level metrics for 360-degree customer view:
# MAGIC - Daily activity aggregation from events and sessions
# MAGIC - Lifetime value and behavioral metrics with window functions
# MAGIC - Customer segmentation (value, engagement, lifecycle, churn risk)
# MAGIC - Cross-device behavior and temporal patterns
# MAGIC - Streak calculations (consecutive active days)
# MAGIC - Engagement trend tracking
# MAGIC
# MAGIC **Sources:**
# MAGIC - silver_customer_events_enriched
# MAGIC - silver_customer_sessions_enriched
# MAGIC
# MAGIC **Targets:**
# MAGIC - gold_customer_360_daily (main table)
# MAGIC - gold_customer_segments (Type-1 SCD current state)
# MAGIC
# MAGIC **Schedule:** Daily (after silver layer processing)
# MAGIC
# MAGIC **Downstream Dependencies:**
# MAGIC - gold_customer_360_mv.py (9 Unity Catalog views)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

# Configuration
EVENTS_SOURCE = "centraldata_sandbox.test.silver_customer_events_enriched"
SESSIONS_SOURCE = "centraldata_sandbox.test.silver_customer_sessions_enriched"
TARGET_TABLE = "centraldata_sandbox.test.gold_customer_360_daily"
SEGMENTS_TABLE = "centraldata_sandbox.test.gold_customer_segments"
CHECKPOINT_TABLE = "centraldata_sandbox.test.gold_customer_360_watermark"

# =============================================================================
# RUN MODE CONFIGURATION
# =============================================================================
# Set RUN_MODE to control how the notebook processes data:
#   - "INCREMENTAL": Process only new data since last watermark (default for daily runs)
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
# MAGIC CREATE TABLE IF NOT EXISTS gold_customer_360_watermark (
# MAGIC   table_name STRING,
# MAGIC   last_processed_timestamp TIMESTAMP,
# MAGIC   last_processed_date DATE,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Initialize watermark if not exists
# MAGIC MERGE INTO gold_customer_360_watermark target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     'gold_customer_360_daily' as table_name,
# MAGIC     CAST('2025-01-01 00:00:00' AS TIMESTAMP) as last_processed_timestamp,
# MAGIC     CAST('2025-01-01' AS DATE) as last_processed_date,
# MAGIC     CURRENT_TIMESTAMP() as updated_at
# MAGIC ) source
# MAGIC ON target.table_name = source.table_name
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Get Last Watermark

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
        CREATE OR REPLACE TEMP VIEW last_watermark_360 AS
        SELECT
            {start_ts} as watermark_date,
            {end_ts} as end_watermark_date
    """)
    print(f"FULL REFRESH MODE: Processing data from {START_DATE or 'last 365 days'} to {END_DATE or 'now'}")

else:
    # Incremental mode - use watermark table
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW last_watermark_360 AS
        SELECT
            COALESCE(CAST(last_processed_date AS DATE), CURRENT_DATE - INTERVAL 7 DAYS) as watermark_date,
            CAST(NULL AS DATE) as end_watermark_date
        FROM gold_customer_360_watermark
        WHERE table_name = 'gold_customer_360_daily'
    """)
    print("INCREMENTAL MODE: Using watermark table for processing window")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview watermark values
# MAGIC SELECT * FROM last_watermark_360

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Main Transformation - Customer 360 Daily Aggregation (SQL)
# MAGIC
# MAGIC ### Data Flow:
# MAGIC 1. **Daily Event Metrics**: Aggregate events per customer per day
# MAGIC 2. **Daily Session Metrics**: Aggregate sessions per customer per day
# MAGIC 3. **Combined Daily Activity**: Join events and sessions
# MAGIC 4. **Lifetime Metrics**: Window functions for running totals
# MAGIC 5. **Behavioral Patterns**: Device, temporal, traffic patterns
# MAGIC 6. **Customer Segmentation**: Value, engagement, lifecycle, churn risk

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- STEP 1: Daily Event Metrics per Customer
# MAGIC -- Aggregates event-level data to customer-day level
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW daily_event_metrics AS
# MAGIC
# MAGIC SELECT
# MAGIC   customer_key,
# MAGIC   date_est as behavior_date,
# MAGIC
# MAGIC   -- Customer Profile (take latest per day)
# MAGIC   MAX(fluent_id) as fluent_id,
# MAGIC   MAX(profile_id) as profile_id,
# MAGIC   MAX(is_identified_user) as is_identified_customer,
# MAGIC   MAX(country) as profile_geo_country,
# MAGIC   MAX(state) as profile_geo_state,
# MAGIC   MAX(city) as profile_geo_city,
# MAGIC   MAX(zip) as profile_zip,
# MAGIC   MAX(profile_gender) as profile_gender,
# MAGIC   MAX(profile_home_owner) as profile_home_owner,
# MAGIC   MAX(profile_has_children) as profile_has_children,
# MAGIC   MAX(profile_owns_car) as profile_owns_car,
# MAGIC
# MAGIC   -- Event Counts
# MAGIC   COUNT(*) as total_events,
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as total_views,
# MAGIC   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as total_clicks,
# MAGIC   SUM(CASE WHEN is_p1_view THEN 1 ELSE 0 END) as total_p1_views,
# MAGIC
# MAGIC   -- Conversion Metrics (using fact table logic: sourceReference = 'offer-convert' AND conversion_type_name != 'Click')
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert'
# MAGIC     AND COALESCE(conversion_type_name, '') != 'Click'
# MAGIC     THEN source_reference_id
# MAGIC   END) as total_conversions,
# MAGIC
# MAGIC   -- Transaction Metrics
# MAGIC   COUNT(DISTINCT CASE WHEN is_transaction_event THEN order_id END) as total_transactions,
# MAGIC
# MAGIC   -- Revenue Metrics
# MAGIC   COALESCE(SUM(CASE
# MAGIC     WHEN source_reference = 'offer-convert'
# MAGIC     AND COALESCE(conversion_type_name, '') != 'Click'
# MAGIC     THEN revenue
# MAGIC   END), 0) as total_revenue_generated,
# MAGIC   COALESCE(SUM(CASE WHEN is_transaction_event THEN sale_amount END), 0) as total_transaction_value,
# MAGIC
# MAGIC   -- Campaign Exposure
# MAGIC   COUNT(DISTINCT campaign_id) as unique_campaigns_viewed,
# MAGIC   COUNT(DISTINCT advertiser_id) as unique_advertisers_interacted,
# MAGIC   COUNT(DISTINCT creative_id) as unique_creatives_seen,
# MAGIC   COUNT(DISTINCT campaign_vertical) as unique_verticals_explored,
# MAGIC
# MAGIC   -- Campaign arrays (filter out nulls)
# MAGIC   ARRAY_DISTINCT(FILTER(COLLECT_LIST(campaign_id), x -> x IS NOT NULL)) as campaigns_interacted,
# MAGIC   ARRAY_DISTINCT(FILTER(COLLECT_LIST(advertiser_id), x -> x IS NOT NULL)) as advertisers_interacted,
# MAGIC   ARRAY_DISTINCT(FILTER(COLLECT_LIST(campaign_vertical), x -> x IS NOT NULL)) as verticals_explored,
# MAGIC
# MAGIC   -- Device Metrics
# MAGIC   COUNT(DISTINCT device_type) as unique_device_types_used,
# MAGIC   MODE(device_type) as primary_device_type,
# MAGIC   SUM(CASE WHEN is_mobile THEN 1 ELSE 0 END) as mobile_events,
# MAGIC   SUM(CASE WHEN is_desktop THEN 1 ELSE 0 END) as desktop_events,
# MAGIC   SUM(CASE WHEN is_tablet THEN 1 ELSE 0 END) as tablet_events,
# MAGIC
# MAGIC   -- Temporal Metrics
# MAGIC   SUM(CASE WHEN is_business_hours THEN 1 ELSE 0 END) as business_hours_events,
# MAGIC   SUM(CASE WHEN NOT is_business_hours THEN 1 ELSE 0 END) as after_hours_events,
# MAGIC   SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) as weekend_events,
# MAGIC   SUM(CASE WHEN NOT is_weekend THEN 1 ELSE 0 END) as weekday_events,
# MAGIC   MODE(local_hour_of_day) as most_active_hour_est,
# MAGIC   MODE(day_of_week) as most_active_day_of_week,
# MAGIC
# MAGIC   -- Time-of-day distribution
# MAGIC   SUM(CASE WHEN local_hour_of_day BETWEEN 6 AND 11 THEN 1 ELSE 0 END) as morning_events,
# MAGIC   SUM(CASE WHEN local_hour_of_day BETWEEN 12 AND 17 THEN 1 ELSE 0 END) as afternoon_events,
# MAGIC   SUM(CASE WHEN local_hour_of_day BETWEEN 18 AND 22 THEN 1 ELSE 0 END) as evening_events,
# MAGIC   SUM(CASE WHEN local_hour_of_day < 6 OR local_hour_of_day > 22 THEN 1 ELSE 0 END) as night_events,
# MAGIC
# MAGIC   -- Traffic Sources
# MAGIC   COUNT(DISTINCT partner_id) as unique_partners_used,
# MAGIC   COUNT(DISTINCT source_id) as unique_sources_used,
# MAGIC   MODE(partner_id) as primary_partner_id,
# MAGIC   MODE(source_id) as primary_source_id,
# MAGIC
# MAGIC   -- Data Quality
# MAGIC   ROUND(AVG(data_quality_score), 3) as avg_event_data_quality_score,
# MAGIC
# MAGIC   -- Timestamps
# MAGIC   MIN(event_timestamp) as first_event_timestamp,
# MAGIC   MAX(event_timestamp) as last_event_timestamp,
# MAGIC
# MAGIC   -- First conversion date for this day (if any)
# MAGIC   MIN(CASE
# MAGIC     WHEN source_reference = 'offer-convert'
# MAGIC     AND COALESCE(conversion_type_name, '') != 'Click'
# MAGIC     THEN event_timestamp
# MAGIC   END) as first_conversion_timestamp_today
# MAGIC
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE date_est >= (SELECT watermark_date FROM last_watermark_360)
# MAGIC   AND (
# MAGIC     (SELECT end_watermark_date FROM last_watermark_360) IS NULL  -- Incremental: no end date
# MAGIC     OR date_est <= (SELECT end_watermark_date FROM last_watermark_360)  -- Full refresh: apply end date
# MAGIC   )
# MAGIC   AND customer_key IS NOT NULL
# MAGIC   AND customer_key NOT LIKE 'ANON_%'  -- Exclude anonymous users for Customer 360
# MAGIC GROUP BY customer_key, date_est;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- STEP 2: Daily Session Metrics per Customer
# MAGIC -- Aggregates session-level data to customer-day level
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW daily_session_metrics AS
# MAGIC
# MAGIC SELECT
# MAGIC   customer_key,
# MAGIC   date_est as behavior_date,
# MAGIC
# MAGIC   -- Session Counts
# MAGIC   COUNT(DISTINCT session_id) as total_sessions,
# MAGIC
# MAGIC   -- Session Duration Metrics
# MAGIC   ROUND(AVG(session_duration_seconds), 2) as avg_session_duration_sec,
# MAGIC   ROUND(SUM(session_duration_seconds), 2) as total_session_duration_sec,
# MAGIC   MAX(session_duration_seconds) as max_session_duration_sec,
# MAGIC
# MAGIC   -- Engagement Metrics
# MAGIC   ROUND(AVG(total_events), 2) as avg_events_per_session,
# MAGIC   ROUND(AVG(session_depth), 2) as avg_session_depth,
# MAGIC   ROUND(AVG(engagement_score), 2) as avg_engagement_score,
# MAGIC   MAX(engagement_score) as max_engagement_score,
# MAGIC   SUM(CASE WHEN engagement_score >= 75 THEN 1 ELSE 0 END) as total_high_engagement_sessions,
# MAGIC
# MAGIC   -- Session Revenue
# MAGIC   COALESCE(SUM(total_session_revenue), 0) as total_session_revenue,
# MAGIC   ROUND(AVG(CASE WHEN total_session_revenue > 0 THEN total_session_revenue END), 2) as avg_revenue_per_session,
# MAGIC
# MAGIC   -- Session Conversions
# MAGIC   SUM(conversion_count) as total_session_conversions,
# MAGIC   SUM(CASE WHEN has_conversion THEN 1 ELSE 0 END) as sessions_with_conversion,
# MAGIC   SUM(CASE WHEN has_transaction THEN 1 ELSE 0 END) as sessions_with_transaction,
# MAGIC
# MAGIC   -- Device Behavior in Sessions
# MAGIC   SUM(device_switches) as total_device_switches,
# MAGIC   SUM(CASE WHEN device_switches > 0 THEN 1 ELSE 0 END) as sessions_with_device_switch,
# MAGIC
# MAGIC   -- Session Timing
# MAGIC   SUM(CASE WHEN is_business_hours THEN 1 ELSE 0 END) as business_hours_sessions,
# MAGIC   SUM(CASE WHEN NOT is_business_hours THEN 1 ELSE 0 END) as after_hours_sessions,
# MAGIC   SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) as weekend_sessions,
# MAGIC   SUM(CASE WHEN NOT is_weekend THEN 1 ELSE 0 END) as weekday_sessions
# MAGIC
# MAGIC FROM silver_customer_sessions_enriched
# MAGIC WHERE date_est >= (SELECT watermark_date FROM last_watermark_360)
# MAGIC   AND (
# MAGIC     (SELECT end_watermark_date FROM last_watermark_360) IS NULL
# MAGIC     OR date_est <= (SELECT end_watermark_date FROM last_watermark_360)
# MAGIC   )
# MAGIC   AND customer_key IS NOT NULL
# MAGIC   AND customer_key NOT LIKE 'ANON_%'
# MAGIC GROUP BY customer_key, date_est;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- STEP 3: Combined Daily Activity (Join Events + Sessions)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW daily_activity_combined AS
# MAGIC
# MAGIC SELECT
# MAGIC   -- Use events as base (more granular)
# MAGIC   e.customer_key,
# MAGIC   e.behavior_date,
# MAGIC
# MAGIC   -- Customer Profile
# MAGIC   e.fluent_id,
# MAGIC   e.profile_id,
# MAGIC   e.is_identified_customer,
# MAGIC   e.profile_geo_country,
# MAGIC   e.profile_geo_state,
# MAGIC   e.profile_geo_city,
# MAGIC   e.profile_zip,
# MAGIC   e.profile_gender,
# MAGIC   e.profile_home_owner,
# MAGIC   e.profile_has_children,
# MAGIC   e.profile_owns_car,
# MAGIC
# MAGIC   -- Event Metrics
# MAGIC   e.total_events,
# MAGIC   e.total_views,
# MAGIC   e.total_clicks,
# MAGIC   e.total_p1_views,
# MAGIC   e.total_conversions,
# MAGIC   e.total_transactions,
# MAGIC   e.total_revenue_generated,
# MAGIC   e.total_transaction_value,
# MAGIC
# MAGIC   -- Calculated Rates
# MAGIC   ROUND(e.total_clicks * 100.0 / NULLIF(e.total_views, 0), 2) as click_through_rate,
# MAGIC   ROUND(e.total_p1_views * 100.0 / NULLIF(e.total_views, 0), 2) as p1_view_rate,
# MAGIC
# MAGIC   -- Campaign Exposure
# MAGIC   e.unique_campaigns_viewed,
# MAGIC   e.unique_advertisers_interacted,
# MAGIC   e.unique_creatives_seen,
# MAGIC   e.unique_verticals_explored,
# MAGIC   e.campaigns_interacted,
# MAGIC   e.advertisers_interacted,
# MAGIC   e.verticals_explored,
# MAGIC
# MAGIC   -- Content Diversity Score (0-1)
# MAGIC   ROUND(
# MAGIC     (
# MAGIC       LEAST(e.unique_campaigns_viewed, 10) / 10.0 * 0.4 +
# MAGIC       LEAST(e.unique_advertisers_interacted, 10) / 10.0 * 0.3 +
# MAGIC       LEAST(e.unique_verticals_explored, 5) / 5.0 * 0.3
# MAGIC     ), 3
# MAGIC   ) as content_diversity_score,
# MAGIC
# MAGIC   -- Device Metrics
# MAGIC   e.unique_device_types_used,
# MAGIC   e.primary_device_type,
# MAGIC   e.mobile_events,
# MAGIC   e.desktop_events,
# MAGIC   e.tablet_events,
# MAGIC   ROUND(e.mobile_events * 100.0 / NULLIF(e.total_events, 0), 2) as mobile_session_pct,
# MAGIC   ROUND(e.desktop_events * 100.0 / NULLIF(e.total_events, 0), 2) as desktop_session_pct,
# MAGIC   ROUND(e.tablet_events * 100.0 / NULLIF(e.total_events, 0), 2) as tablet_session_pct,
# MAGIC   e.unique_device_types_used > 1 as cross_device_user_flag,
# MAGIC
# MAGIC   -- Temporal Metrics
# MAGIC   e.most_active_hour_est,
# MAGIC   e.most_active_day_of_week,
# MAGIC   ROUND(e.morning_events * 100.0 / NULLIF(e.total_events, 0), 2) as morning_activity_pct,
# MAGIC   ROUND(e.afternoon_events * 100.0 / NULLIF(e.total_events, 0), 2) as afternoon_activity_pct,
# MAGIC   ROUND(e.evening_events * 100.0 / NULLIF(e.total_events, 0), 2) as evening_activity_pct,
# MAGIC   ROUND(e.night_events * 100.0 / NULLIF(e.total_events, 0), 2) as night_activity_pct,
# MAGIC
# MAGIC   -- Traffic Sources
# MAGIC   e.unique_partners_used,
# MAGIC   e.unique_sources_used,
# MAGIC   e.primary_partner_id,
# MAGIC   e.primary_source_id,
# MAGIC
# MAGIC   -- Session Metrics (from sessions table)
# MAGIC   COALESCE(s.total_sessions, 0) as total_sessions,
# MAGIC   COALESCE(s.avg_session_duration_sec, 0) as avg_session_duration_sec,
# MAGIC   COALESCE(s.total_session_duration_sec, 0) as total_session_duration_sec,
# MAGIC   COALESCE(s.avg_events_per_session, 0) as avg_events_per_session,
# MAGIC   COALESCE(s.avg_session_depth, 0) as avg_session_depth,
# MAGIC   COALESCE(s.avg_engagement_score, 0) as avg_engagement_score,
# MAGIC   COALESCE(s.max_engagement_score, 0) as max_engagement_score,
# MAGIC   COALESCE(s.total_high_engagement_sessions, 0) as total_high_engagement_sessions,
# MAGIC   COALESCE(s.avg_revenue_per_session, 0) as avg_revenue_per_session,
# MAGIC
# MAGIC   -- Session-level conversion/transaction counts
# MAGIC   COALESCE(s.sessions_with_conversion, 0) as sessions_with_conversion,
# MAGIC   COALESCE(s.sessions_with_transaction, 0) as sessions_with_transaction,
# MAGIC
# MAGIC   -- Calculated Rates from Sessions
# MAGIC   ROUND(e.total_conversions * 100.0 / NULLIF(s.total_sessions, 0), 2) as conversion_rate,
# MAGIC   ROUND(e.total_transactions * 100.0 / NULLIF(s.total_sessions, 0), 2) as transaction_rate,
# MAGIC   ROUND(e.total_transaction_value / NULLIF(e.total_transactions, 0), 2) as avg_transaction_value,
# MAGIC
# MAGIC   -- Session Timing
# MAGIC   COALESCE(s.business_hours_sessions, 0) as business_hours_sessions,
# MAGIC   COALESCE(s.after_hours_sessions, 0) as after_hours_sessions,
# MAGIC   COALESCE(s.weekend_sessions, 0) as weekend_sessions,
# MAGIC   COALESCE(s.weekday_sessions, 0) as weekday_sessions,
# MAGIC
# MAGIC   -- Data Quality
# MAGIC   e.avg_event_data_quality_score,
# MAGIC   e.first_event_timestamp,
# MAGIC   e.last_event_timestamp,
# MAGIC   e.first_conversion_timestamp_today
# MAGIC
# MAGIC FROM daily_event_metrics e
# MAGIC LEFT JOIN daily_session_metrics s
# MAGIC   ON e.customer_key = s.customer_key
# MAGIC   AND e.behavior_date = s.behavior_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- STEP 4: Lifetime Metrics with Window Functions
# MAGIC -- Running totals, customer tenure, and conversion timing calculations
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW daily_with_lifetime_metrics AS
# MAGIC
# MAGIC SELECT
# MAGIC   d.*,
# MAGIC
# MAGIC   -- Customer Tenure (days since first activity)
# MAGIC   DATEDIFF(
# MAGIC     d.behavior_date,
# MAGIC     MIN(d.behavior_date) OVER (PARTITION BY d.customer_key)
# MAGIC   ) as customer_tenure_days,
# MAGIC
# MAGIC   -- First and Last Activity Dates
# MAGIC   MIN(d.behavior_date) OVER (PARTITION BY d.customer_key) as first_activity_date,
# MAGIC   MAX(d.behavior_date) OVER (PARTITION BY d.customer_key) as last_activity_date,
# MAGIC
# MAGIC   -- Days Since Last Activity (relative to current behavior_date)
# MAGIC   COALESCE(
# MAGIC     DATEDIFF(
# MAGIC       d.behavior_date,
# MAGIC       LAG(d.behavior_date, 1) OVER (PARTITION BY d.customer_key ORDER BY d.behavior_date)
# MAGIC     ),
# MAGIC     0
# MAGIC   ) as days_since_last_activity,
# MAGIC
# MAGIC   -- Lifetime Revenue (cumulative)
# MAGIC   SUM(d.total_revenue_generated) OVER (
# MAGIC     PARTITION BY d.customer_key
# MAGIC     ORDER BY d.behavior_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ) as lifetime_revenue,
# MAGIC
# MAGIC   -- Lifetime Transactions (cumulative)
# MAGIC   SUM(d.total_transactions) OVER (
# MAGIC     PARTITION BY d.customer_key
# MAGIC     ORDER BY d.behavior_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ) as lifetime_transaction_count,
# MAGIC
# MAGIC   -- Lifetime Conversions (cumulative)
# MAGIC   SUM(d.total_conversions) OVER (
# MAGIC     PARTITION BY d.customer_key
# MAGIC     ORDER BY d.behavior_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ) as lifetime_conversion_count,
# MAGIC
# MAGIC   -- Lifetime Sessions (cumulative)
# MAGIC   SUM(d.total_sessions) OVER (
# MAGIC     PARTITION BY d.customer_key
# MAGIC     ORDER BY d.behavior_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ) as lifetime_session_count,
# MAGIC
# MAGIC   -- Lifetime Events (cumulative)
# MAGIC   SUM(d.total_events) OVER (
# MAGIC     PARTITION BY d.customer_key
# MAGIC     ORDER BY d.behavior_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ) as lifetime_event_count,
# MAGIC
# MAGIC   -- Total Active Days (count of distinct days with activity up to this date)
# MAGIC   COUNT(*) OVER (
# MAGIC     PARTITION BY d.customer_key
# MAGIC     ORDER BY d.behavior_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ) as lifetime_active_days,
# MAGIC
# MAGIC   -- Average Days Between Visits
# MAGIC   CASE
# MAGIC     WHEN COUNT(*) OVER (PARTITION BY d.customer_key ORDER BY d.behavior_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) > 1
# MAGIC     THEN ROUND(
# MAGIC       DATEDIFF(d.behavior_date, MIN(d.behavior_date) OVER (PARTITION BY d.customer_key)) * 1.0 /
# MAGIC       (COUNT(*) OVER (PARTITION BY d.customer_key ORDER BY d.behavior_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - 1),
# MAGIC       2
# MAGIC     )
# MAGIC     ELSE NULL
# MAGIC   END as avg_days_between_visits,
# MAGIC
# MAGIC   -- Rolling 30-day Metrics
# MAGIC   SUM(d.total_sessions) OVER (
# MAGIC     PARTITION BY d.customer_key
# MAGIC     ORDER BY d.behavior_date
# MAGIC     RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
# MAGIC   ) as sessions_last_30_days,
# MAGIC
# MAGIC   SUM(d.total_revenue_generated) OVER (
# MAGIC     PARTITION BY d.customer_key
# MAGIC     ORDER BY d.behavior_date
# MAGIC     RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
# MAGIC   ) as revenue_last_30_days,
# MAGIC
# MAGIC   SUM(d.total_conversions) OVER (
# MAGIC     PARTITION BY d.customer_key
# MAGIC     ORDER BY d.behavior_date
# MAGIC     RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
# MAGIC   ) as conversions_last_30_days,
# MAGIC
# MAGIC   -- Historical Engagement (for trend calculation)
# MAGIC   AVG(d.avg_engagement_score) OVER (
# MAGIC     PARTITION BY d.customer_key
# MAGIC     ORDER BY d.behavior_date
# MAGIC     ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
# MAGIC   ) as avg_engagement_last_7_days
# MAGIC
# MAGIC FROM daily_activity_combined d;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- STEP 5: Customer Segmentation
# MAGIC -- Value, Engagement, Lifecycle, and Churn Risk segments
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW daily_with_segmentation AS
# MAGIC
# MAGIC SELECT
# MAGIC   l.*,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- CUSTOMER VALUE SEGMENT
# MAGIC   -- Based on lifetime revenue
# MAGIC   -- ==========================================================================
# MAGIC   CASE
# MAGIC     WHEN l.lifetime_revenue >= 500 THEN 'high_value'
# MAGIC     WHEN l.lifetime_revenue >= 100 THEN 'medium_value'
# MAGIC     WHEN l.lifetime_revenue > 0 THEN 'low_value'
# MAGIC     ELSE 'no_value'
# MAGIC   END as customer_value_segment,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- ENGAGEMENT SEGMENT
# MAGIC   -- Based on engagement score and session activity
# MAGIC   -- ==========================================================================
# MAGIC   CASE
# MAGIC     WHEN l.avg_engagement_score >= 75 AND l.total_sessions >= 3 THEN 'highly_engaged'
# MAGIC     WHEN l.avg_engagement_score >= 50 OR l.total_sessions >= 2 THEN 'moderately_engaged'
# MAGIC     WHEN l.avg_engagement_score >= 25 OR l.total_sessions >= 1 THEN 'low_engaged'
# MAGIC     ELSE 'minimal'
# MAGIC   END as engagement_segment,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- PURCHASE PROPENSITY SEGMENT
# MAGIC   -- Based on conversion history and engagement
# MAGIC   -- ==========================================================================
# MAGIC   CASE
# MAGIC     WHEN l.lifetime_conversion_count >= 3 AND l.avg_engagement_score >= 60 THEN 'high_propensity'
# MAGIC     WHEN l.lifetime_conversion_count >= 1 OR (l.avg_engagement_score >= 50 AND l.total_clicks > 0) THEN 'medium_propensity'
# MAGIC     WHEN l.total_clicks > 0 OR l.avg_engagement_score >= 30 THEN 'low_propensity'
# MAGIC     ELSE 'unknown'
# MAGIC   END as purchase_propensity_segment,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- LIFECYCLE STAGE
# MAGIC   -- Based on tenure and recency
# MAGIC   -- ==========================================================================
# MAGIC   CASE
# MAGIC     WHEN l.customer_tenure_days <= 30 THEN 'new'
# MAGIC     WHEN COALESCE(l.days_since_last_activity, 0) <= 7 THEN 'active'
# MAGIC     WHEN COALESCE(l.days_since_last_activity, 0) <= 30 THEN 'engaged'
# MAGIC     WHEN COALESCE(l.days_since_last_activity, 0) <= 60 THEN 'at_risk'
# MAGIC     ELSE 'churned'
# MAGIC   END as lifecycle_stage,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- SESSION FREQUENCY TIER
# MAGIC   -- Based on average days between visits
# MAGIC   -- ==========================================================================
# MAGIC   CASE
# MAGIC     WHEN l.avg_days_between_visits IS NULL THEN 'one_time'
# MAGIC     WHEN l.avg_days_between_visits <= 3 THEN 'daily_visitor'
# MAGIC     WHEN l.avg_days_between_visits <= 7 THEN 'weekly_visitor'
# MAGIC     WHEN l.avg_days_between_visits <= 30 THEN 'monthly_visitor'
# MAGIC     ELSE 'occasional_visitor'
# MAGIC   END as session_frequency_tier,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- ENGAGEMENT TREND
# MAGIC   -- Compare current engagement to recent average
# MAGIC   -- ==========================================================================
# MAGIC   CASE
# MAGIC     WHEN l.avg_engagement_last_7_days IS NULL THEN 'new'
# MAGIC     WHEN l.avg_engagement_score > l.avg_engagement_last_7_days * 1.1 THEN 'increasing'
# MAGIC     WHEN l.avg_engagement_score < l.avg_engagement_last_7_days * 0.9 THEN 'decreasing'
# MAGIC     ELSE 'stable'
# MAGIC   END as engagement_trend,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- RECENCY TIER
# MAGIC   -- ==========================================================================
# MAGIC   CASE
# MAGIC     WHEN l.days_since_last_activity <= 1 THEN 'recent'
# MAGIC     WHEN l.days_since_last_activity <= 7 THEN 'moderate'
# MAGIC     ELSE 'dormant'
# MAGIC   END as recency_tier,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- CHURN RISK SCORE (0-1)
# MAGIC   -- Higher score = higher risk of churning
# MAGIC   -- ==========================================================================
# MAGIC   LEAST(1.0, GREATEST(0.0, ROUND(
# MAGIC     -- Recency component (40% weight) - more days since activity = higher risk
# MAGIC     COALESCE(
# MAGIC       LEAST(l.days_since_last_activity, 90) / 90.0 * 0.4,
# MAGIC       0.2  -- Default if no previous activity
# MAGIC     ) +
# MAGIC
# MAGIC     -- Frequency component (30% weight) - fewer visits = higher risk
# MAGIC     (1 - LEAST(l.lifetime_active_days, 30) / 30.0) * 0.3 +
# MAGIC
# MAGIC     -- Engagement component (20% weight) - lower engagement = higher risk
# MAGIC     (1 - LEAST(l.avg_engagement_score, 100) / 100.0) * 0.2 +
# MAGIC
# MAGIC     -- Value component (10% weight) - no revenue = slight risk increase
# MAGIC     CASE WHEN l.lifetime_revenue = 0 THEN 0.1 ELSE 0 END
# MAGIC   , 3))) as churn_risk_score,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- CUSTOMER LIFETIME VALUE ESTIMATE (Simplified)
# MAGIC   -- Based on current revenue and projected future value
# MAGIC   -- ==========================================================================
# MAGIC   ROUND(
# MAGIC     l.lifetime_revenue +
# MAGIC     -- Project future value based on recent activity
# MAGIC     COALESCE(l.revenue_last_30_days, 0) * 12 *
# MAGIC     -- Adjust by retention probability (1 - churn_risk)
# MAGIC     (1 - LEAST(1.0, GREATEST(0.0,
# MAGIC       COALESCE(LEAST(l.days_since_last_activity, 90) / 90.0 * 0.4, 0.2) +
# MAGIC       (1 - LEAST(l.lifetime_active_days, 30) / 30.0) * 0.3 +
# MAGIC       (1 - LEAST(l.avg_engagement_score, 100) / 100.0) * 0.2 +
# MAGIC       CASE WHEN l.lifetime_revenue = 0 THEN 0.1 ELSE 0 END
# MAGIC     ))),
# MAGIC     2
# MAGIC   ) as customer_lifetime_value_est,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- PROFILE COMPLETENESS SCORE (0-1)
# MAGIC   -- Based on available profile attributes
# MAGIC   -- ==========================================================================
# MAGIC   ROUND(
# MAGIC     (
# MAGIC       CASE WHEN l.is_identified_customer THEN 0.25 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_id IS NOT NULL THEN 0.15 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_geo_country IS NOT NULL THEN 0.15 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_geo_state IS NOT NULL THEN 0.10 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_geo_city IS NOT NULL THEN 0.05 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_zip IS NOT NULL THEN 0.05 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_gender IS NOT NULL THEN 0.10 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_home_owner IS NOT NULL THEN 0.05 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_has_children IS NOT NULL THEN 0.05 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_owns_car IS NOT NULL THEN 0.05 ELSE 0 END
# MAGIC     ), 3
# MAGIC   ) as profile_completeness_score,
# MAGIC
# MAGIC   -- ==========================================================================
# MAGIC   -- OVERALL DATA QUALITY SCORE (0-1)
# MAGIC   -- Combines event quality, profile completeness, and activity signals
# MAGIC   -- ==========================================================================
# MAGIC   ROUND(
# MAGIC     l.avg_event_data_quality_score * 0.5 +
# MAGIC     (
# MAGIC       CASE WHEN l.is_identified_customer THEN 0.25 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_id IS NOT NULL THEN 0.15 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_geo_country IS NOT NULL THEN 0.15 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_geo_state IS NOT NULL THEN 0.10 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_gender IS NOT NULL THEN 0.10 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_home_owner IS NOT NULL THEN 0.05 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_has_children IS NOT NULL THEN 0.05 ELSE 0 END +
# MAGIC       CASE WHEN l.profile_owns_car IS NOT NULL THEN 0.05 ELSE 0 END
# MAGIC     ) * 0.3 +
# MAGIC     CASE WHEN l.total_sessions >= 1 THEN 0.2 ELSE 0.1 END,
# MAGIC     3
# MAGIC   ) as data_quality_score
# MAGIC
# MAGIC FROM daily_with_lifetime_metrics l;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- STEP 6: Final Customer 360 Daily Table (SQL portion)
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW gold_customer_360_sql_transformed AS
# MAGIC
# MAGIC SELECT
# MAGIC   -- Generate unique PK for customer-day
# MAGIC   SHA2(CONCAT(s.customer_key, '_', CAST(s.behavior_date AS STRING)), 256) as customer_day_pk,
# MAGIC
# MAGIC   -- Customer Identifiers
# MAGIC   s.customer_key,
# MAGIC   s.fluent_id,
# MAGIC   s.profile_id,
# MAGIC   s.is_identified_customer,
# MAGIC
# MAGIC   -- Behavior Date
# MAGIC   s.behavior_date,
# MAGIC
# MAGIC   -- Customer Tenure
# MAGIC   s.customer_tenure_days,
# MAGIC   s.first_activity_date,
# MAGIC   s.last_activity_date,
# MAGIC   s.days_since_last_activity,
# MAGIC   s.avg_days_between_visits,
# MAGIC
# MAGIC   -- Is Active Today Flag
# MAGIC   TRUE as is_active_today,
# MAGIC
# MAGIC   -- Profile Attributes
# MAGIC   s.profile_geo_country,
# MAGIC   s.profile_geo_state,
# MAGIC   s.profile_geo_city,
# MAGIC   s.profile_zip,
# MAGIC   s.profile_gender,
# MAGIC   s.profile_home_owner,
# MAGIC   s.profile_has_children,
# MAGIC   s.profile_owns_car,
# MAGIC
# MAGIC   -- Daily Activity Metrics
# MAGIC   s.total_sessions,
# MAGIC   s.total_events,
# MAGIC   s.total_views,
# MAGIC   s.total_clicks,
# MAGIC   s.total_p1_views,
# MAGIC   s.click_through_rate,
# MAGIC   s.p1_view_rate,
# MAGIC
# MAGIC   -- Session Metrics
# MAGIC   s.avg_session_duration_sec,
# MAGIC   s.total_session_duration_sec,
# MAGIC   s.avg_events_per_session,
# MAGIC   s.avg_session_depth,
# MAGIC   s.avg_engagement_score,
# MAGIC   s.max_engagement_score,
# MAGIC   s.total_high_engagement_sessions,
# MAGIC
# MAGIC   -- Conversion & Revenue (Daily)
# MAGIC   s.total_conversions,
# MAGIC   s.total_transactions,
# MAGIC   s.conversion_rate,
# MAGIC   s.transaction_rate,
# MAGIC   s.total_revenue_generated,
# MAGIC   s.avg_revenue_per_session,
# MAGIC   s.total_transaction_value,
# MAGIC   s.avg_transaction_value,
# MAGIC
# MAGIC   -- Lifetime Metrics
# MAGIC   s.lifetime_revenue,
# MAGIC   s.lifetime_transaction_count,
# MAGIC   s.lifetime_conversion_count,
# MAGIC   s.lifetime_session_count,
# MAGIC   s.lifetime_event_count,
# MAGIC   s.lifetime_active_days,
# MAGIC
# MAGIC   -- Rolling 30-day Metrics
# MAGIC   s.sessions_last_30_days,
# MAGIC   s.revenue_last_30_days,
# MAGIC   s.conversions_last_30_days,
# MAGIC
# MAGIC   -- Customer Lifetime Value
# MAGIC   s.customer_lifetime_value_est,
# MAGIC
# MAGIC   -- Campaign Exposure (Daily)
# MAGIC   s.unique_campaigns_viewed,
# MAGIC   s.unique_advertisers_interacted,
# MAGIC   s.unique_creatives_seen,
# MAGIC   s.unique_verticals_explored,
# MAGIC   s.content_diversity_score,
# MAGIC
# MAGIC   -- Campaign Exposure Arrays
# MAGIC   s.campaigns_interacted as campaigns_interacted_30d,
# MAGIC   s.advertisers_interacted as advertisers_interacted_30d,
# MAGIC   s.verticals_explored as verticals_preferred_30d,
# MAGIC
# MAGIC   -- Device Behavior
# MAGIC   s.unique_device_types_used,
# MAGIC   s.primary_device_type,
# MAGIC   s.mobile_session_pct,
# MAGIC   s.desktop_session_pct,
# MAGIC   s.tablet_session_pct,
# MAGIC   s.cross_device_user_flag,
# MAGIC
# MAGIC   -- Temporal Patterns
# MAGIC   s.business_hours_sessions,
# MAGIC   s.after_hours_sessions,
# MAGIC   s.weekend_sessions,
# MAGIC   s.weekday_sessions,
# MAGIC   s.most_active_hour_est,
# MAGIC   s.most_active_day_of_week,
# MAGIC   s.morning_activity_pct,
# MAGIC   s.afternoon_activity_pct,
# MAGIC   s.evening_activity_pct,
# MAGIC   s.night_activity_pct,
# MAGIC
# MAGIC   -- Traffic Sources
# MAGIC   s.primary_partner_id,
# MAGIC   s.primary_source_id,
# MAGIC   s.unique_partners_used,
# MAGIC   s.unique_sources_used,
# MAGIC
# MAGIC   -- Segmentation
# MAGIC   s.customer_value_segment,
# MAGIC   s.engagement_segment,
# MAGIC   s.purchase_propensity_segment,
# MAGIC   s.lifecycle_stage,
# MAGIC   s.session_frequency_tier,
# MAGIC   s.engagement_trend,
# MAGIC   s.recency_tier,
# MAGIC   s.churn_risk_score,
# MAGIC
# MAGIC   -- Data Quality
# MAGIC   s.profile_completeness_score,
# MAGIC   s.data_quality_score,
# MAGIC
# MAGIC   -- Timestamps for conversion tracking
# MAGIC   s.first_conversion_timestamp_today,
# MAGIC
# MAGIC   -- Metadata
# MAGIC   s.first_event_timestamp,
# MAGIC   s.last_event_timestamp,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_timestamp,
# MAGIC   CURRENT_TIMESTAMP() as processing_timestamp,
# MAGIC
# MAGIC   -- Partitioning Column
# MAGIC   s.behavior_date as date_est
# MAGIC
# MAGIC FROM daily_with_segmentation s;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Python Post-Processing: Streak & Conversion Timing Calculations
# MAGIC
# MAGIC These metrics require row-by-row logic that's more efficient in PySpark:
# MAGIC - Consecutive active days
# MAGIC - Longest active streak
# MAGIC - Days since first/last conversion

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read the SQL-transformed data
df_360 = spark.table("gold_customer_360_sql_transformed")

# Define window specs
window_customer_date = Window.partitionBy("customer_key").orderBy("behavior_date")
window_customer = Window.partitionBy("customer_key")

# Calculate streak metrics
df_360_enhanced = df_360 \
    .withColumn(
        "prev_behavior_date",
        F.lag("behavior_date").over(window_customer_date)
    ) \
    .withColumn(
        "is_consecutive",
        F.when(
            F.datediff(F.col("behavior_date"), F.col("prev_behavior_date")) == 1,
            True
        ).otherwise(False)
    ) \
    .withColumn(
        "streak_group",
        F.sum(F.when(F.col("is_consecutive"), 0).otherwise(1)).over(window_customer_date)
    ) \
    .withColumn(
        "consecutive_active_days",
        F.row_number().over(
            Window.partitionBy("customer_key", "streak_group").orderBy("behavior_date")
        )
    ) \
    .withColumn(
        "longest_active_streak_days",
        F.max("consecutive_active_days").over(window_customer)
    )

# Calculate conversion timing metrics
df_360_with_conversions = df_360_enhanced \
    .withColumn(
        "has_conversion_today",
        F.col("total_conversions") > 0
    ) \
    .withColumn(
        "first_conversion_date",
        F.when(F.col("has_conversion_today"), F.col("behavior_date")).otherwise(None)
    ) \
    .withColumn(
        "customer_first_conversion_date",
        F.min(F.col("first_conversion_date")).over(window_customer)
    ) \
    .withColumn(
        "customer_last_conversion_date",
        F.max(F.col("first_conversion_date")).over(window_customer)
    ) \
    .withColumn(
        "days_since_first_conversion",
        F.when(
            F.col("customer_first_conversion_date").isNotNull(),
            F.datediff(F.col("behavior_date"), F.col("customer_first_conversion_date"))
        ).otherwise(None)
    ) \
    .withColumn(
        "days_since_last_conversion",
        F.when(
            F.col("customer_last_conversion_date").isNotNull() &
            (F.col("behavior_date") >= F.col("customer_last_conversion_date")),
            F.datediff(F.col("behavior_date"), F.col("customer_last_conversion_date"))
        ).otherwise(None)
    ) \
    .withColumn(
        "conversion_frequency_days",
        F.when(
            (F.col("lifetime_conversion_count") > 1) & F.col("days_since_first_conversion").isNotNull(),
            F.round(F.col("days_since_first_conversion") / F.col("lifetime_conversion_count"), 2)
        ).otherwise(None)
    )

# Select final columns and drop temp columns
df_final = df_360_with_conversions.select(
    "customer_day_pk",
    "customer_key",
    "fluent_id",
    "profile_id",
    "is_identified_customer",
    "behavior_date",
    "customer_tenure_days",
    "first_activity_date",
    "last_activity_date",
    "days_since_last_activity",
    "avg_days_between_visits",
    "is_active_today",
    "profile_geo_country",
    "profile_geo_state",
    "profile_geo_city",
    "profile_zip",
    "profile_gender",
    "profile_home_owner",
    "profile_has_children",
    "profile_owns_car",
    "total_sessions",
    "total_events",
    "total_views",
    "total_clicks",
    "total_p1_views",
    "click_through_rate",
    "p1_view_rate",
    "avg_session_duration_sec",
    "total_session_duration_sec",
    "avg_events_per_session",
    "avg_session_depth",
    "avg_engagement_score",
    "max_engagement_score",
    "total_high_engagement_sessions",
    "total_conversions",
    "total_transactions",
    "conversion_rate",
    "transaction_rate",
    "total_revenue_generated",
    "avg_revenue_per_session",
    "total_transaction_value",
    "avg_transaction_value",
    "lifetime_revenue",
    "lifetime_transaction_count",
    "lifetime_conversion_count",
    "lifetime_session_count",
    "lifetime_event_count",
    "lifetime_active_days",
    "sessions_last_30_days",
    "revenue_last_30_days",
    "conversions_last_30_days",
    "customer_lifetime_value_est",
    "unique_campaigns_viewed",
    "unique_advertisers_interacted",
    "unique_creatives_seen",
    "unique_verticals_explored",
    "content_diversity_score",
    "campaigns_interacted_30d",
    "advertisers_interacted_30d",
    "verticals_preferred_30d",
    "unique_device_types_used",
    "primary_device_type",
    "mobile_session_pct",
    "desktop_session_pct",
    "tablet_session_pct",
    "cross_device_user_flag",
    "business_hours_sessions",
    "after_hours_sessions",
    "weekend_sessions",
    "weekday_sessions",
    "most_active_hour_est",
    "most_active_day_of_week",
    "morning_activity_pct",
    "afternoon_activity_pct",
    "evening_activity_pct",
    "night_activity_pct",
    "primary_partner_id",
    "primary_source_id",
    "unique_partners_used",
    "unique_sources_used",
    "customer_value_segment",
    "engagement_segment",
    "purchase_propensity_segment",
    "lifecycle_stage",
    "session_frequency_tier",
    "engagement_trend",
    "recency_tier",
    "consecutive_active_days",
    "longest_active_streak_days",
    "days_since_first_conversion",
    "days_since_last_conversion",
    "conversion_frequency_days",
    "churn_risk_score",
    "profile_completeness_score",
    "data_quality_score",
    "first_event_timestamp",
    "last_event_timestamp",
    "last_updated_timestamp",
    "processing_timestamp",
    "date_est"
)

# Register as temp view for final write
df_final.createOrReplaceTempView("gold_customer_360_transformed")

print(f"Enhanced Customer 360 with {df_final.count():,} customer-day records")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview transformed data
# MAGIC SELECT * FROM gold_customer_360_transformed LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Target Table and Write Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create target table if not exists
# MAGIC CREATE TABLE IF NOT EXISTS gold_customer_360_daily (
# MAGIC   customer_day_pk STRING,
# MAGIC   customer_key STRING,
# MAGIC   fluent_id STRING,
# MAGIC   profile_id STRING,
# MAGIC   is_identified_customer BOOLEAN,
# MAGIC   behavior_date DATE,
# MAGIC   customer_tenure_days INT,
# MAGIC   first_activity_date DATE,
# MAGIC   last_activity_date DATE,
# MAGIC   days_since_last_activity INT,
# MAGIC   avg_days_between_visits DOUBLE,
# MAGIC   is_active_today BOOLEAN,
# MAGIC   profile_geo_country STRING,
# MAGIC   profile_geo_state STRING,
# MAGIC   profile_geo_city STRING,
# MAGIC   profile_zip STRING,
# MAGIC   profile_gender STRING,
# MAGIC   profile_home_owner STRING,
# MAGIC   profile_has_children BOOLEAN,
# MAGIC   profile_owns_car STRING,
# MAGIC   total_sessions BIGINT,
# MAGIC   total_events BIGINT,
# MAGIC   total_views BIGINT,
# MAGIC   total_clicks BIGINT,
# MAGIC   total_p1_views BIGINT,
# MAGIC   click_through_rate DOUBLE,
# MAGIC   p1_view_rate DOUBLE,
# MAGIC   avg_session_duration_sec DOUBLE,
# MAGIC   total_session_duration_sec DOUBLE,
# MAGIC   avg_events_per_session DOUBLE,
# MAGIC   avg_session_depth DOUBLE,
# MAGIC   avg_engagement_score DOUBLE,
# MAGIC   max_engagement_score DOUBLE,
# MAGIC   total_high_engagement_sessions BIGINT,
# MAGIC   total_conversions BIGINT,
# MAGIC   total_transactions BIGINT,
# MAGIC   conversion_rate DOUBLE,
# MAGIC   transaction_rate DOUBLE,
# MAGIC   total_revenue_generated DECIMAL(19,4),
# MAGIC   avg_revenue_per_session DOUBLE,
# MAGIC   total_transaction_value DECIMAL(19,4),
# MAGIC   avg_transaction_value DOUBLE,
# MAGIC   lifetime_revenue DECIMAL(19,4),
# MAGIC   lifetime_transaction_count BIGINT,
# MAGIC   lifetime_conversion_count BIGINT,
# MAGIC   lifetime_session_count BIGINT,
# MAGIC   lifetime_event_count BIGINT,
# MAGIC   lifetime_active_days BIGINT,
# MAGIC   sessions_last_30_days BIGINT,
# MAGIC   revenue_last_30_days DECIMAL(19,4),
# MAGIC   conversions_last_30_days BIGINT,
# MAGIC   customer_lifetime_value_est DOUBLE,
# MAGIC   unique_campaigns_viewed BIGINT,
# MAGIC   unique_advertisers_interacted BIGINT,
# MAGIC   unique_creatives_seen BIGINT,
# MAGIC   unique_verticals_explored BIGINT,
# MAGIC   content_diversity_score DOUBLE,
# MAGIC   campaigns_interacted_30d ARRAY<STRING>,
# MAGIC   advertisers_interacted_30d ARRAY<STRING>,
# MAGIC   verticals_preferred_30d ARRAY<STRING>,
# MAGIC   unique_device_types_used BIGINT,
# MAGIC   primary_device_type STRING,
# MAGIC   mobile_session_pct DOUBLE,
# MAGIC   desktop_session_pct DOUBLE,
# MAGIC   tablet_session_pct DOUBLE,
# MAGIC   cross_device_user_flag BOOLEAN,
# MAGIC   business_hours_sessions BIGINT,
# MAGIC   after_hours_sessions BIGINT,
# MAGIC   weekend_sessions BIGINT,
# MAGIC   weekday_sessions BIGINT,
# MAGIC   most_active_hour_est INT,
# MAGIC   most_active_day_of_week STRING,
# MAGIC   morning_activity_pct DOUBLE,
# MAGIC   afternoon_activity_pct DOUBLE,
# MAGIC   evening_activity_pct DOUBLE,
# MAGIC   night_activity_pct DOUBLE,
# MAGIC   primary_partner_id STRING,
# MAGIC   primary_source_id STRING,
# MAGIC   unique_partners_used BIGINT,
# MAGIC   unique_sources_used BIGINT,
# MAGIC   customer_value_segment STRING,
# MAGIC   engagement_segment STRING,
# MAGIC   purchase_propensity_segment STRING,
# MAGIC   lifecycle_stage STRING,
# MAGIC   session_frequency_tier STRING,
# MAGIC   engagement_trend STRING,
# MAGIC   recency_tier STRING,
# MAGIC   consecutive_active_days INT,
# MAGIC   longest_active_streak_days INT,
# MAGIC   days_since_first_conversion INT,
# MAGIC   days_since_last_conversion INT,
# MAGIC   conversion_frequency_days DOUBLE,
# MAGIC   churn_risk_score DOUBLE,
# MAGIC   profile_completeness_score DOUBLE,
# MAGIC   data_quality_score DOUBLE,
# MAGIC   first_event_timestamp TIMESTAMP,
# MAGIC   last_event_timestamp TIMESTAMP,
# MAGIC   last_updated_timestamp TIMESTAMP,
# MAGIC   processing_timestamp TIMESTAMP,
# MAGIC   date_est DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_est)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# Write data based on RUN_MODE
if RUN_MODE == "FULL_REFRESH":
    # ==========================================================================
    # FULL REFRESH MODE: Overwrite table or specific date partitions
    # ==========================================================================
    print(f"FULL REFRESH: Overwriting table {TARGET_TABLE}")

    df_to_write = spark.table("gold_customer_360_transformed")

    if START_DATE:
        # Overwrite only specific partitions based on date range
        from pyspark.sql.functions import col, lit

        # Filter data by date_est
        df_filtered = df_to_write.filter(col("date_est") >= lit(START_DATE).cast("date"))

        if END_DATE:
            df_filtered = df_filtered.filter(col("date_est") <= lit(END_DATE).cast("date"))
            replace_condition = f"date_est >= CAST('{START_DATE}' AS DATE) AND date_est <= CAST('{END_DATE}' AS DATE)"
            print(f"Filtering data for date range: {START_DATE} to {END_DATE}")
        else:
            replace_condition = f"date_est >= CAST('{START_DATE}' AS DATE)"
            print(f"Filtering data for dates >= {START_DATE}")

        df_filtered.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", replace_condition) \
            .option("mergeSchema", "true") \
            .saveAsTable(TARGET_TABLE)
        print(f"Partitions replaced for: {replace_condition}")
        record_count = df_filtered.count()
    else:
        # Full table overwrite
        print("Dropping existing table for full schema refresh...")
        spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
        df_to_write.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("date_est") \
            .saveAsTable(TARGET_TABLE)
        print("Full table overwritten with new schema")
        record_count = df_to_write.count()

    print(f"FULL REFRESH completed with {record_count:,} customer-day records")

else:
    # ==========================================================================
    # INCREMENTAL MODE: MERGE new/updated records
    # ==========================================================================
    print(f"INCREMENTAL: Performing MERGE into {TARGET_TABLE}")

    spark.sql("""
        MERGE INTO gold_customer_360_daily target
        USING gold_customer_360_transformed source
        ON target.customer_day_pk = source.customer_day_pk
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("INCREMENTAL MERGE completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Current State Segments Table (Type-1 SCD)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create/replace current state customer segments table
# MAGIC -- This provides a single-row-per-customer view for operational use
# MAGIC CREATE OR REPLACE TABLE gold_customer_segments
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )
# MAGIC AS
# MAGIC WITH latest_customer_data AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY behavior_date DESC) as rn
# MAGIC   FROM gold_customer_360_daily
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_key,
# MAGIC   fluent_id,
# MAGIC   profile_id,
# MAGIC   is_identified_customer,
# MAGIC
# MAGIC   -- Current Segments
# MAGIC   customer_value_segment as current_value_segment,
# MAGIC   engagement_segment as current_engagement_segment,
# MAGIC   purchase_propensity_segment as current_propensity_segment,
# MAGIC   lifecycle_stage as current_lifecycle_stage,
# MAGIC   session_frequency_tier as current_frequency_tier,
# MAGIC   engagement_trend as current_engagement_trend,
# MAGIC   recency_tier as current_recency_tier,
# MAGIC
# MAGIC   -- Churn Risk
# MAGIC   churn_risk_score as current_churn_risk_score,
# MAGIC   CASE
# MAGIC     WHEN churn_risk_score >= 0.7 THEN 'high'
# MAGIC     WHEN churn_risk_score >= 0.4 THEN 'medium'
# MAGIC     ELSE 'low'
# MAGIC   END as current_churn_risk_tier,
# MAGIC
# MAGIC   -- Lifetime Metrics
# MAGIC   lifetime_revenue as total_lifetime_revenue,
# MAGIC   lifetime_transaction_count as total_lifetime_transactions,
# MAGIC   lifetime_conversion_count as total_lifetime_conversions,
# MAGIC   lifetime_session_count as total_lifetime_sessions,
# MAGIC   lifetime_active_days as total_active_days,
# MAGIC   customer_lifetime_value_est,
# MAGIC
# MAGIC   -- Streak Metrics
# MAGIC   consecutive_active_days as current_streak_days,
# MAGIC   longest_active_streak_days,
# MAGIC
# MAGIC   -- Activity Metrics
# MAGIC   behavior_date as last_behavior_date,
# MAGIC   first_activity_date,
# MAGIC   customer_tenure_days,
# MAGIC   days_since_last_activity,
# MAGIC   avg_days_between_visits,
# MAGIC
# MAGIC   -- Engagement Metrics
# MAGIC   avg_engagement_score as latest_engagement_score,
# MAGIC
# MAGIC   -- Profile
# MAGIC   profile_geo_country,
# MAGIC   profile_geo_state,
# MAGIC   profile_completeness_score,
# MAGIC   data_quality_score,
# MAGIC
# MAGIC   -- Device Preference
# MAGIC   primary_device_type,
# MAGIC   cross_device_user_flag,
# MAGIC
# MAGIC   -- Metadata
# MAGIC   CURRENT_TIMESTAMP() as segment_updated_timestamp
# MAGIC
# MAGIC FROM latest_customer_data
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Optimize Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize main 360 table
# MAGIC OPTIMIZE gold_customer_360_daily
# MAGIC ZORDER BY (customer_key, behavior_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize segments table
# MAGIC OPTIMIZE gold_customer_segments
# MAGIC ZORDER BY (customer_key, current_value_segment, current_lifecycle_stage);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze tables for query optimization
# MAGIC ANALYZE TABLE gold_customer_360_daily COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE gold_customer_segments COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Update Watermark

# COMMAND ----------

# Update watermark (only for INCREMENTAL mode)
if RUN_MODE == "INCREMENTAL":
    spark.sql("""
        MERGE INTO gold_customer_360_watermark target
        USING (
            SELECT
                'gold_customer_360_daily' as table_name,
                MAX(last_event_timestamp) as last_processed_timestamp,
                MAX(behavior_date) as last_processed_date,
                CURRENT_TIMESTAMP() as updated_at
            FROM gold_customer_360_daily
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
# MAGIC -- Daily summary
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   COUNT(*) as total_customer_days,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC
# MAGIC   -- Segment distribution
# MAGIC   SUM(CASE WHEN customer_value_segment = 'high_value' THEN 1 ELSE 0 END) as high_value_customers,
# MAGIC   SUM(CASE WHEN customer_value_segment = 'medium_value' THEN 1 ELSE 0 END) as medium_value_customers,
# MAGIC   SUM(CASE WHEN customer_value_segment = 'low_value' THEN 1 ELSE 0 END) as low_value_customers,
# MAGIC
# MAGIC   -- Lifecycle distribution
# MAGIC   SUM(CASE WHEN lifecycle_stage = 'new' THEN 1 ELSE 0 END) as new_customers,
# MAGIC   SUM(CASE WHEN lifecycle_stage = 'active' THEN 1 ELSE 0 END) as active_customers,
# MAGIC   SUM(CASE WHEN lifecycle_stage = 'at_risk' THEN 1 ELSE 0 END) as at_risk_customers,
# MAGIC
# MAGIC   -- Engagement Trend
# MAGIC   SUM(CASE WHEN engagement_trend = 'increasing' THEN 1 ELSE 0 END) as engagement_increasing,
# MAGIC   SUM(CASE WHEN engagement_trend = 'decreasing' THEN 1 ELSE 0 END) as engagement_decreasing,
# MAGIC
# MAGIC   -- Engagement
# MAGIC   ROUND(AVG(avg_engagement_score), 2) as avg_engagement,
# MAGIC   ROUND(AVG(churn_risk_score), 3) as avg_churn_risk,
# MAGIC
# MAGIC   -- Revenue
# MAGIC   ROUND(SUM(total_revenue_generated), 2) as total_daily_revenue,
# MAGIC   ROUND(SUM(lifetime_revenue), 2) as total_lifetime_revenue,
# MAGIC
# MAGIC   -- Streaks
# MAGIC   ROUND(AVG(consecutive_active_days), 1) as avg_current_streak,
# MAGIC   MAX(longest_active_streak_days) as max_streak_days,
# MAGIC
# MAGIC   -- Data Quality
# MAGIC   ROUND(AVG(data_quality_score), 3) as avg_data_quality
# MAGIC
# MAGIC FROM gold_customer_360_daily
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY date_est
# MAGIC ORDER BY date_est DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current state segment summary
# MAGIC SELECT
# MAGIC   current_value_segment,
# MAGIC   current_lifecycle_stage,
# MAGIC   current_churn_risk_tier,
# MAGIC   COUNT(*) as customer_count,
# MAGIC   ROUND(AVG(total_lifetime_revenue), 2) as avg_lifetime_revenue,
# MAGIC   ROUND(AVG(customer_lifetime_value_est), 2) as avg_clv,
# MAGIC   ROUND(AVG(latest_engagement_score), 2) as avg_engagement,
# MAGIC   ROUND(AVG(days_since_last_activity), 1) as avg_days_since_activity
# MAGIC FROM gold_customer_segments
# MAGIC GROUP BY current_value_segment, current_lifecycle_stage, current_churn_risk_tier
# MAGIC ORDER BY customer_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Job Completion Summary

# COMMAND ----------

from datetime import datetime

# Get summary statistics
summary_stats = spark.sql("""
  SELECT
    COUNT(*) as total_customer_days,
    COUNT(DISTINCT customer_key) as unique_customers,
    SUM(CASE WHEN customer_value_segment = 'high_value' THEN 1 ELSE 0 END) as high_value_customers,
    ROUND(SUM(total_revenue_generated), 2) as total_daily_revenue,
    ROUND(AVG(lifetime_revenue), 2) as avg_lifetime_revenue,
    ROUND(AVG(avg_engagement_score), 2) as avg_engagement_score,
    ROUND(AVG(churn_risk_score), 3) as avg_churn_risk_score,
    ROUND(AVG(data_quality_score), 3) as avg_data_quality_score,
    ROUND(AVG(consecutive_active_days), 1) as avg_streak_days,
    MAX(longest_active_streak_days) as max_streak_days
  FROM gold_customer_360_daily
  WHERE date_est >= CURRENT_DATE - 1
""").collect()[0]

# Get segment breakdown
segment_breakdown = spark.sql("""
  SELECT
    current_value_segment,
    COUNT(*) as count
  FROM gold_customer_segments
  GROUP BY current_value_segment
  ORDER BY count DESC
""").collect()

# Get engagement trend breakdown
trend_breakdown = spark.sql("""
  SELECT
    engagement_trend,
    COUNT(DISTINCT customer_key) as count
  FROM gold_customer_360_daily
  WHERE date_est >= CURRENT_DATE - 1
  GROUP BY engagement_trend
  ORDER BY count DESC
""").collect()

# Print completion summary
print("=" * 80)
print("GOLD LAYER - CUSTOMER 360 DAILY - JOB COMPLETED")
print("=" * 80)
print(f"Run Mode: {RUN_MODE}")
if RUN_MODE == "FULL_REFRESH":
    print(f"Date Range: {START_DATE or 'default'} to {END_DATE or 'now'}")
print(f"Total Customer-Days Processed: {summary_stats['total_customer_days']:,}")
print(f"Unique Customers: {summary_stats['unique_customers']:,}")
print(f"High Value Customers: {summary_stats['high_value_customers']:,}")
print(f"Total Daily Revenue: ${summary_stats['total_daily_revenue']:,.2f}")
print(f"Avg Lifetime Revenue: ${summary_stats['avg_lifetime_revenue']:,.2f}")
print(f"Avg Engagement Score: {summary_stats['avg_engagement_score']:.2f}")
print(f"Avg Churn Risk Score: {summary_stats['avg_churn_risk_score']:.3f}")
print(f"Avg Data Quality Score: {summary_stats['avg_data_quality_score']:.3f}")
print(f"Avg Streak Days: {summary_stats['avg_streak_days']:.1f}")
print(f"Max Streak Days: {summary_stats['max_streak_days']}")
print("")
print("CUSTOMER VALUE SEGMENTS (Current State):")
for row in segment_breakdown:
    segment = row['current_value_segment']
    count = row['count']
    print(f"  {segment}: {count:,}")
print("")
print("ENGAGEMENT TRENDS (Today):")
for row in trend_breakdown:
    trend = row['engagement_trend']
    count = row['count']
    print(f"  {trend}: {count:,}")
print("")
print(f"Processing Completed: {datetime.now()}")
print("=" * 80)

# Return success
segment_summary = {row['current_value_segment']: row['count'] for row in segment_breakdown}
dbutils.notebook.exit(f"Success: {RUN_MODE} - Processed {summary_stats['total_customer_days']:,} customer-days. Revenue: ${summary_stats['total_daily_revenue']:,.2f}. Segments: {segment_summary}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Tables & Downstream Dependencies
# MAGIC
# MAGIC ### Tables Created:
# MAGIC | Table | Type | Purpose |
# MAGIC |-------|------|---------|
# MAGIC | `gold_customer_360_daily` | Delta (partitioned) | Customer-day level metrics with full history |
# MAGIC | `gold_customer_segments` | Delta | Type-1 SCD current state (1 row per customer) |
# MAGIC
# MAGIC ### Downstream Views (gold_customer_360_mv.py):
# MAGIC | View | Source |
# MAGIC |------|--------|
# MAGIC | `customer_360_metrics` | gold_customer_360_daily |
# MAGIC | `customer_segments_summary` | gold_customer_360_daily |
# MAGIC | `customer_daily_kpis` | gold_customer_360_daily |
# MAGIC | `customer_current_state` | gold_customer_360_daily |
# MAGIC | `high_value_customers` | gold_customer_360_daily |
# MAGIC | `at_risk_customers` | gold_customer_360_daily |
# MAGIC | `new_customer_cohort` | gold_customer_360_daily |
# MAGIC | `repeat_customer_analysis` | gold_customer_360_daily |
# MAGIC | `repeat_customer_summary` | gold_customer_360_daily |
# MAGIC
# MAGIC ### New Columns Added (Merged from Original):
# MAGIC - `profile_zip` - Customer zip code
# MAGIC - `unique_creatives_seen` - Creative diversity metric
# MAGIC - `engagement_trend` - increasing/decreasing/stable/new
# MAGIC - `recency_tier` - recent/moderate/dormant
# MAGIC - `consecutive_active_days` - Current active streak
# MAGIC - `longest_active_streak_days` - Max historical streak
# MAGIC - `days_since_first_conversion` - Conversion timing
# MAGIC - `days_since_last_conversion` - Conversion recency
# MAGIC - `conversion_frequency_days` - Average days between conversions
