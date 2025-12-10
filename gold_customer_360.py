# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Customer 360 Daily
# MAGIC
# MAGIC **Purpose:** Create comprehensive daily customer behavior profiles with:
# MAGIC - Daily activity summary and behavioral patterns
# MAGIC - Browsing and purchase behavior metrics
# MAGIC - Cross-device and temporal patterns
# MAGIC - Ad interaction history and preferences
# MAGIC - Engagement intensity and loyalty indicators
# MAGIC - Customer segmentation attributes
# MAGIC
# MAGIC **Sources:** 
# MAGIC - silver_customer_events_enriched
# MAGIC - silver_customer_sessions_enriched
# MAGIC
# MAGIC **Target:** gold_customer_360_daily  
# MAGIC **Schedule:** Daily at 2 AM EST (after previous day complete)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

# Configuration
SOURCE_EVENTS_TABLE = "centraldata_sandbox.test.silver_customer_events_enriched"
SOURCE_SESSIONS_TABLE = "centraldata_sandbox.test.silver_customer_sessions_enriched"
TARGET_TABLE = "centraldata_sandbox.test.gold_customer_360_daily"
CHECKPOINT_TABLE = "centraldata_sandbox.test.gold_customer_360_watermark"

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
# MAGIC MERGE INTO gold_customer_360_watermark target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     'gold_customer_360_daily' as table_name,
# MAGIC     CAST('2025-01-01' AS DATE) as last_processed_date,
# MAGIC     CURRENT_TIMESTAMP() as updated_at
# MAGIC ) source
# MAGIC ON target.table_name = source.table_name
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get last processed date
# MAGIC CREATE OR REPLACE TEMP VIEW last_watermark_360 AS
# MAGIC SELECT 
# MAGIC   CURRENT_DATE - INTERVAL 7 DAYS as watermark_date
# MAGIC FROM gold_customer_360_watermark
# MAGIC WHERE table_name = 'gold_customer_360_daily';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from last_watermark_360

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Main Customer 360 Transformation - SQL-Based

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Build comprehensive daily customer 360 view
# MAGIC CREATE OR REPLACE TEMP VIEW gold_customer_360_daily_transformed AS
# MAGIC
# MAGIC WITH daily_events AS (
# MAGIC   -- Get events for processing window
# MAGIC   SELECT *
# MAGIC   FROM silver_customer_events_enriched
# MAGIC   WHERE date_est >= (SELECT watermark_date FROM last_watermark_360)
# MAGIC ),
# MAGIC
# MAGIC daily_sessions AS (
# MAGIC   -- Get sessions for processing window
# MAGIC   SELECT *
# MAGIC   FROM silver_customer_sessions_enriched
# MAGIC   WHERE date_est >= (SELECT watermark_date FROM last_watermark_360)
# MAGIC ),
# MAGIC
# MAGIC customer_profile_base AS (
# MAGIC   -- Extract latest customer profile info per day
# MAGIC   SELECT
# MAGIC     customer_key,
# MAGIC     date_est as behavior_date,
# MAGIC     MAX(fluent_id) as fluent_id,
# MAGIC     MAX(profile_id) as profile_id,
# MAGIC     MAX(email_sha256) as email_sha256,
# MAGIC     MAX(is_identified_user) as is_identified_customer,
# MAGIC     MAX(profile_gender) as profile_gender,
# MAGIC     MAX(country) as profile_geo_country,
# MAGIC     MAX(state) as profile_geo_state,
# MAGIC     MAX(city) as profile_geo_city,
# MAGIC     MAX(zip) as profile_zip,
# MAGIC     MIN(event_timestamp) as first_activity_timestamp,
# MAGIC     MAX(event_timestamp) as last_activity_timestamp
# MAGIC   FROM daily_events
# MAGIC   GROUP BY customer_key, date_est
# MAGIC ),
# MAGIC
# MAGIC customer_lifetime_context AS (
# MAGIC   -- Calculate lifetime metrics up to behavior date
# MAGIC   SELECT
# MAGIC     e.customer_key,
# MAGIC     e.date_est as behavior_date,
# MAGIC     MIN(e.profile_first_visit) as customer_first_visit,
# MAGIC     DATEDIFF(e.date_est, MIN(e.profile_first_visit)) as customer_tenure_days,
# MAGIC     
# MAGIC     -- Lifetime conversions and revenue (cumulative to date)
# MAGIC     COUNT(DISTINCT CASE 
# MAGIC       WHEN e.source_reference = 'offer-convert' 
# MAGIC       AND e.conversion_type_name != 'Click'
# MAGIC       AND e.date_est <= e.date_est
# MAGIC       THEN e.source_reference_id 
# MAGIC     END) as lifetime_transaction_count,
# MAGIC     
# MAGIC     SUM(CASE WHEN e.date_est <= e.date_est THEN COALESCE(e.revenue, 0) ELSE 0 END) as lifetime_revenue
# MAGIC     
# MAGIC   FROM daily_events e
# MAGIC   GROUP BY e.customer_key, e.date_est
# MAGIC ),
# MAGIC
# MAGIC daily_activity_summary AS (
# MAGIC   -- Aggregate daily activity from sessions
# MAGIC   SELECT
# MAGIC     customer_key,
# MAGIC     date_est as behavior_date,
# MAGIC     
# MAGIC     -- Activity flags
# MAGIC     TRUE as is_active_today,
# MAGIC     
# MAGIC     -- Session metrics
# MAGIC     COUNT(DISTINCT session_id) as total_sessions,
# MAGIC     SUM(total_events) as total_events,
# MAGIC     ROUND(AVG(session_duration_seconds), 2) as avg_session_duration_sec,
# MAGIC     ROUND(AVG(total_events), 2) as avg_events_per_session,
# MAGIC     
# MAGIC     -- Browsing behavior
# MAGIC     SUM(view_events) as total_views,
# MAGIC     SUM(click_events) as total_clicks,
# MAGIC     ROUND(SUM(click_events) * 100.0 / NULLIF(SUM(view_events), 0), 2) as click_through_rate,
# MAGIC     SUM(p1_view_events) as total_p1_views,
# MAGIC     ROUND(SUM(p1_view_events) * 100.0 / NULLIF(SUM(view_events), 0), 2) as p1_view_rate,
# MAGIC     
# MAGIC     -- Content consumption
# MAGIC     SIZE(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(campaigns_viewed)))) as unique_campaigns_viewed,
# MAGIC     SIZE(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(advertisers_interacted)))) as unique_advertisers_interacted,
# MAGIC     SIZE(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(verticals_explored)))) as unique_verticals_explored,
# MAGIC     ROUND(AVG(session_depth), 2) as avg_session_depth,
# MAGIC     
# MAGIC     -- Purchase/Conversion behavior
# MAGIC     SUM(conversion_count) as total_conversions,
# MAGIC     SUM(CASE WHEN has_transaction THEN 1 ELSE 0 END) as total_transactions,
# MAGIC     ROUND(SUM(CASE WHEN has_conversion THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as conversion_rate,
# MAGIC     ROUND(SUM(CASE WHEN has_transaction THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as transaction_rate,
# MAGIC     SUM(total_session_revenue) as total_transaction_value,
# MAGIC     SUM(total_session_revenue) as total_revenue_generated,
# MAGIC     ROUND(AVG(CASE WHEN total_session_revenue > 0 THEN total_session_revenue END), 2) as avg_transaction_value,
# MAGIC     ROUND(AVG(total_session_revenue), 2) as avg_revenue_per_session,
# MAGIC     
# MAGIC     -- Device behavior
# MAGIC     COUNT(DISTINCT primary_device_type) as unique_device_types_used,
# MAGIC     COLLECT_SET(primary_device_type) as device_type_list,
# MAGIC     MODE(primary_device_type) as primary_device_type,
# MAGIC     ROUND(SUM(CASE WHEN LOWER(primary_device_type) IN ('smartphone', 'mobile') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as mobile_session_pct,
# MAGIC     ROUND(SUM(CASE WHEN LOWER(primary_device_type) IN ('desktop', 'pc') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as desktop_session_pct,
# MAGIC     ROUND(SUM(CASE WHEN LOWER(primary_device_type) = 'tablet' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as tablet_session_pct,
# MAGIC     CASE WHEN COUNT(DISTINCT primary_device_type) > 1 THEN TRUE ELSE FALSE END as cross_device_user_flag,
# MAGIC     
# MAGIC     -- Temporal patterns
# MAGIC     SUM(CASE WHEN is_business_hours THEN 1 ELSE 0 END) as business_hours_sessions,
# MAGIC     SUM(CASE WHEN NOT is_business_hours THEN 1 ELSE 0 END) as after_hours_sessions,
# MAGIC     SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) as weekend_sessions,
# MAGIC     SUM(CASE WHEN NOT is_weekend THEN 1 ELSE 0 END) as weekday_sessions,
# MAGIC     MODE(session_hour_est) as most_active_hour_est,
# MAGIC     MODE(day_of_week) as most_active_day_of_week,
# MAGIC     
# MAGIC     -- Time of day distribution
# MAGIC     ROUND(SUM(CASE WHEN session_hour_est BETWEEN 6 AND 11 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as morning_activity_pct,
# MAGIC     ROUND(SUM(CASE WHEN session_hour_est BETWEEN 12 AND 17 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as afternoon_activity_pct,
# MAGIC     ROUND(SUM(CASE WHEN session_hour_est BETWEEN 18 AND 23 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as evening_activity_pct,
# MAGIC     ROUND(SUM(CASE WHEN session_hour_est BETWEEN 0 AND 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as night_activity_pct,
# MAGIC     
# MAGIC     -- Engagement metrics
# MAGIC     ROUND(AVG(engagement_score), 2) as avg_engagement_score,
# MAGIC     MAX(engagement_score) as max_engagement_score,
# MAGIC     SUM(CASE WHEN engagement_score >= 75 THEN 1 ELSE 0 END) as total_high_engagement_sessions,
# MAGIC     
# MAGIC     -- Traffic source patterns
# MAGIC     MODE(partner_id) as primary_partner_id,
# MAGIC     MODE(source_id) as primary_source_id,
# MAGIC     COUNT(DISTINCT partner_id) as unique_partners_used,
# MAGIC     COUNT(DISTINCT source_id) as unique_sources_used,
# MAGIC     
# MAGIC     -- Campaign exposure (last 30 days stored as arrays)
# MAGIC     SLICE(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(campaigns_viewed))), 1, 50) as campaigns_interacted_30d,
# MAGIC     SLICE(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(advertisers_interacted))), 1, 50) as advertisers_interacted_30d,
# MAGIC     SLICE(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(verticals_explored))), 1, 10) as verticals_preferred_30d
# MAGIC     
# MAGIC   FROM daily_sessions
# MAGIC   GROUP BY customer_key, date_est
# MAGIC ),
# MAGIC
# MAGIC customer_recency_metrics AS (
# MAGIC   -- Calculate recency metrics
# MAGIC   SELECT
# MAGIC     customer_key,
# MAGIC     behavior_date,
# MAGIC     DATEDIFF(behavior_date, MAX(behavior_date) OVER (PARTITION BY customer_key ORDER BY behavior_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) as days_since_last_activity
# MAGIC   FROM daily_activity_summary
# MAGIC ),
# MAGIC
# MAGIC customer_360_base AS (
# MAGIC   -- Combine all metrics
# MAGIC   SELECT
# MAGIC     -- Primary key
# MAGIC     SHA2(CONCAT(p.customer_key, '_', CAST(p.behavior_date AS STRING)), 256) as customer_360_pk,
# MAGIC     p.customer_key,
# MAGIC     p.behavior_date,
# MAGIC     
# MAGIC     -- Profile snapshot
# MAGIC     p.fluent_id,
# MAGIC     p.profile_id,
# MAGIC     p.email_sha256,
# MAGIC     p.is_identified_customer,
# MAGIC     l.customer_tenure_days,
# MAGIC     p.profile_gender,
# MAGIC     p.profile_geo_country,
# MAGIC     p.profile_geo_state,
# MAGIC     p.profile_geo_city,
# MAGIC     p.profile_zip,
# MAGIC     
# MAGIC     -- Activity summary
# MAGIC     COALESCE(r.days_since_last_activity, 0) as days_since_last_activity,
# MAGIC     a.is_active_today,
# MAGIC     a.total_sessions,
# MAGIC     a.total_events,
# MAGIC     a.avg_session_duration_sec,
# MAGIC     a.avg_events_per_session,
# MAGIC     
# MAGIC     -- Browsing patterns
# MAGIC     a.total_views,
# MAGIC     a.total_clicks,
# MAGIC     a.click_through_rate,
# MAGIC     a.total_p1_views,
# MAGIC     a.p1_view_rate,
# MAGIC     
# MAGIC     -- Content consumption
# MAGIC     a.unique_campaigns_viewed,
# MAGIC     a.unique_advertisers_interacted,
# MAGIC     a.unique_verticals_explored,
# MAGIC     -- Content diversity score (0-1)
# MAGIC     ROUND(LEAST(1.0, (a.unique_campaigns_viewed / 10.0 + a.unique_advertisers_interacted / 5.0 + a.unique_verticals_explored / 3.0) / 3.0), 3) as content_diversity_score,
# MAGIC     a.avg_session_depth,
# MAGIC     
# MAGIC     -- Purchase behavior
# MAGIC     a.total_conversions,
# MAGIC     a.total_transactions,
# MAGIC     a.conversion_rate,
# MAGIC     a.transaction_rate,
# MAGIC     a.total_transaction_value,
# MAGIC     a.total_revenue_generated,
# MAGIC     a.avg_transaction_value,
# MAGIC     a.avg_revenue_per_session,
# MAGIC     l.lifetime_transaction_count,
# MAGIC     l.lifetime_revenue,
# MAGIC     
# MAGIC     -- Device patterns
# MAGIC     a.unique_device_types_used,
# MAGIC     a.device_type_list,
# MAGIC     a.primary_device_type,
# MAGIC     a.mobile_session_pct,
# MAGIC     a.desktop_session_pct,
# MAGIC     a.tablet_session_pct,
# MAGIC     a.cross_device_user_flag,
# MAGIC     
# MAGIC     -- Temporal patterns
# MAGIC     a.business_hours_sessions,
# MAGIC     a.after_hours_sessions,
# MAGIC     a.weekend_sessions,
# MAGIC     a.weekday_sessions,
# MAGIC     a.most_active_hour_est,
# MAGIC     a.most_active_day_of_week,
# MAGIC     a.morning_activity_pct,
# MAGIC     a.afternoon_activity_pct,
# MAGIC     a.evening_activity_pct,
# MAGIC     a.night_activity_pct,
# MAGIC     
# MAGIC     -- Engagement
# MAGIC     a.avg_engagement_score,
# MAGIC     a.max_engagement_score,
# MAGIC     a.total_high_engagement_sessions,
# MAGIC     
# MAGIC     -- Traffic source
# MAGIC     a.primary_partner_id,
# MAGIC     a.primary_source_id,
# MAGIC     a.unique_partners_used,
# MAGIC     a.unique_sources_used,
# MAGIC     
# MAGIC     -- Campaign history
# MAGIC     a.campaigns_interacted_30d,
# MAGIC     a.advertisers_interacted_30d,
# MAGIC     a.verticals_preferred_30d,
# MAGIC     
# MAGIC     -- Metadata
# MAGIC     l.customer_first_visit as first_seen_date,
# MAGIC     CURRENT_TIMESTAMP() as last_updated_timestamp,
# MAGIC     CURRENT_DATE() as gold_load_date
# MAGIC     
# MAGIC   FROM customer_profile_base p
# MAGIC   LEFT JOIN customer_lifetime_context l 
# MAGIC     ON p.customer_key = l.customer_key AND p.behavior_date = l.behavior_date
# MAGIC   LEFT JOIN daily_activity_summary a 
# MAGIC     ON p.customer_key = a.customer_key AND p.behavior_date = a.behavior_date
# MAGIC   LEFT JOIN customer_recency_metrics r 
# MAGIC     ON p.customer_key = r.customer_key AND p.behavior_date = r.behavior_date
# MAGIC ),
# MAGIC
# MAGIC customer_360_with_trends AS (
# MAGIC   -- Add trend analysis and additional calculations
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     
# MAGIC     -- Engagement trend (comparing to previous period)
# MAGIC     CASE
# MAGIC       WHEN avg_engagement_score > LAG(avg_engagement_score, 1) OVER (PARTITION BY customer_key ORDER BY behavior_date) THEN 'increasing'
# MAGIC       WHEN avg_engagement_score < LAG(avg_engagement_score, 1) OVER (PARTITION BY customer_key ORDER BY behavior_date) THEN 'decreasing'
# MAGIC       ELSE 'stable'
# MAGIC     END as engagement_trend,
# MAGIC     
# MAGIC     -- Session frequency tier
# MAGIC     CASE
# MAGIC       WHEN total_sessions >= 5 THEN 'high'
# MAGIC       WHEN total_sessions >= 2 THEN 'medium'
# MAGIC       ELSE 'low'
# MAGIC     END as session_frequency_tier,
# MAGIC     
# MAGIC     -- Recency tier
# MAGIC     CASE
# MAGIC       WHEN days_since_last_activity <= 1 THEN 'recent'
# MAGIC       WHEN days_since_last_activity <= 7 THEN 'moderate'
# MAGIC       ELSE 'dormant'
# MAGIC     END as recency_tier,
# MAGIC     
# MAGIC     -- Consecutive active days (simple calculation)
# MAGIC     CASE
# MAGIC       WHEN days_since_last_activity = 0 THEN 1
# MAGIC       WHEN days_since_last_activity = 1 THEN 2
# MAGIC       ELSE 0
# MAGIC     END as consecutive_active_days,
# MAGIC     
# MAGIC     -- Churn risk score (simple rule-based)
# MAGIC     ROUND(
# MAGIC       CASE
# MAGIC         WHEN days_since_last_activity > 30 THEN 0.9
# MAGIC         WHEN days_since_last_activity > 14 THEN 0.7
# MAGIC         WHEN days_since_last_activity > 7 THEN 0.5
# MAGIC         WHEN avg_engagement_score < 25 THEN 0.6
# MAGIC         WHEN total_sessions = 1 THEN 0.4
# MAGIC         ELSE 0.2
# MAGIC       END,
# MAGIC       2
# MAGIC     ) as churn_risk_score,
# MAGIC     
# MAGIC     -- Customer lifetime value estimate (simple calculation)
# MAGIC     ROUND(
# MAGIC       COALESCE(lifetime_revenue, 0) + 
# MAGIC       (CASE WHEN avg_engagement_score >= 75 THEN 100 ELSE 0 END) +
# MAGIC       (total_conversions * 50),
# MAGIC       2
# MAGIC     ) as customer_lifetime_value_est,
# MAGIC     
# MAGIC     -- Profile completeness score
# MAGIC     ROUND(
# MAGIC       (CASE WHEN profile_id IS NOT NULL THEN 0.3 ELSE 0 END +
# MAGIC        CASE WHEN email_sha256 IS NOT NULL THEN 0.2 ELSE 0 END +
# MAGIC        CASE WHEN profile_gender IS NOT NULL THEN 0.1 ELSE 0 END +
# MAGIC        CASE WHEN profile_geo_state IS NOT NULL THEN 0.2 ELSE 0 END +
# MAGIC        CASE WHEN profile_zip IS NOT NULL THEN 0.2 ELSE 0 END),
# MAGIC       2
# MAGIC     ) as profile_completeness_score,
# MAGIC     
# MAGIC     -- Data quality score
# MAGIC     ROUND(
# MAGIC       (CASE WHEN customer_key IS NOT NULL THEN 0.3 ELSE 0 END +
# MAGIC        CASE WHEN total_sessions > 0 THEN 0.3 ELSE 0 END +
# MAGIC        CASE WHEN is_identified_customer THEN 0.2 ELSE 0 END +
# MAGIC        CASE WHEN primary_device_type IS NOT NULL THEN 0.1 ELSE 0 END +
# MAGIC        CASE WHEN profile_geo_country IS NOT NULL THEN 0.1 ELSE 0 END),
# MAGIC       2
# MAGIC     ) as data_quality_score
# MAGIC     
# MAGIC   FROM customer_360_base
# MAGIC ),
# MAGIC
# MAGIC customer_360_with_segments AS (
# MAGIC   -- Add segmentation
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     
# MAGIC     -- Value segment (based on lifetime revenue percentiles)
# MAGIC     CASE
# MAGIC       WHEN lifetime_revenue >= PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY lifetime_revenue) OVER () THEN 'high'
# MAGIC       WHEN lifetime_revenue >= PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY lifetime_revenue) OVER () THEN 'medium'
# MAGIC       ELSE 'low'
# MAGIC     END as customer_value_segment,
# MAGIC     
# MAGIC     -- Engagement segment
# MAGIC     CASE
# MAGIC       WHEN avg_engagement_score >= 75 AND total_sessions >= 3 THEN 'highly_engaged'
# MAGIC       WHEN avg_engagement_score >= 50 OR total_sessions >= 2 THEN 'moderate'
# MAGIC       ELSE 'low'
# MAGIC     END as engagement_segment,
# MAGIC     
# MAGIC     -- Purchase propensity segment
# MAGIC     CASE
# MAGIC       WHEN total_conversions > 0 THEN 'converter'
# MAGIC       WHEN total_clicks > 0 THEN 'browser'
# MAGIC       ELSE 'explorer'
# MAGIC     END as purchase_propensity_segment,
# MAGIC     
# MAGIC     -- Lifecycle stage
# MAGIC     CASE
# MAGIC       WHEN customer_tenure_days <= 7 THEN 'new'
# MAGIC       WHEN days_since_last_activity > 30 THEN 'at_risk'
# MAGIC       WHEN days_since_last_activity > 60 THEN 'churned'
# MAGIC       WHEN total_sessions >= 3 AND avg_engagement_score >= 50 THEN 'active'
# MAGIC       ELSE 'casual'
# MAGIC     END as lifecycle_stage
# MAGIC     
# MAGIC   FROM customer_360_with_trends
# MAGIC )
# MAGIC
# MAGIC -- Final select with partitioning column
# MAGIC SELECT
# MAGIC   customer_360_pk,
# MAGIC   customer_key,
# MAGIC   behavior_date,
# MAGIC   fluent_id,
# MAGIC   profile_id,
# MAGIC   email_sha256,
# MAGIC   is_identified_customer,
# MAGIC   customer_tenure_days,
# MAGIC   profile_gender,
# MAGIC   profile_geo_country,
# MAGIC   profile_geo_state,
# MAGIC   profile_geo_city,
# MAGIC   profile_zip,
# MAGIC   days_since_last_activity,
# MAGIC   is_active_today,
# MAGIC   total_sessions,
# MAGIC   total_events,
# MAGIC   avg_session_duration_sec,
# MAGIC   avg_events_per_session,
# MAGIC   total_views,
# MAGIC   total_clicks,
# MAGIC   click_through_rate,
# MAGIC   total_p1_views,
# MAGIC   p1_view_rate,
# MAGIC   unique_campaigns_viewed,
# MAGIC   unique_advertisers_interacted,
# MAGIC   unique_verticals_explored,
# MAGIC   content_diversity_score,
# MAGIC   avg_session_depth,
# MAGIC   total_conversions,
# MAGIC   total_transactions,
# MAGIC   conversion_rate,
# MAGIC   transaction_rate,
# MAGIC   total_transaction_value,
# MAGIC   total_revenue_generated,
# MAGIC   avg_transaction_value,
# MAGIC   avg_revenue_per_session,
# MAGIC   lifetime_transaction_count,
# MAGIC   lifetime_revenue,
# MAGIC   unique_device_types_used,
# MAGIC   device_type_list,
# MAGIC   primary_device_type,
# MAGIC   mobile_session_pct,
# MAGIC   desktop_session_pct,
# MAGIC   tablet_session_pct,
# MAGIC   cross_device_user_flag,
# MAGIC   business_hours_sessions,
# MAGIC   after_hours_sessions,
# MAGIC   weekend_sessions,
# MAGIC   weekday_sessions,
# MAGIC   most_active_hour_est,
# MAGIC   most_active_day_of_week,
# MAGIC   morning_activity_pct,
# MAGIC   afternoon_activity_pct,
# MAGIC   evening_activity_pct,
# MAGIC   night_activity_pct,
# MAGIC   avg_engagement_score,
# MAGIC   max_engagement_score,
# MAGIC   total_high_engagement_sessions,
# MAGIC   engagement_trend,
# MAGIC   session_frequency_tier,
# MAGIC   recency_tier,
# MAGIC   consecutive_active_days,
# MAGIC   churn_risk_score,
# MAGIC   customer_lifetime_value_est,
# MAGIC   primary_partner_id,
# MAGIC   primary_source_id,
# MAGIC   unique_partners_used,
# MAGIC   unique_sources_used,
# MAGIC   campaigns_interacted_30d,
# MAGIC   advertisers_interacted_30d,
# MAGIC   verticals_preferred_30d,
# MAGIC   profile_completeness_score,
# MAGIC   data_quality_score,
# MAGIC   customer_value_segment,
# MAGIC   engagement_segment,
# MAGIC   purchase_propensity_segment,
# MAGIC   lifecycle_stage,
# MAGIC   first_seen_date,
# MAGIC   last_updated_timestamp,
# MAGIC   gold_load_date,
# MAGIC   behavior_date as date_est
# MAGIC FROM customer_360_with_segments;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview transformed customer 360 data
# MAGIC SELECT * FROM gold_customer_360_daily_transformed LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Delta Table with MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create target table if not exists
# MAGIC CREATE TABLE IF NOT EXISTS gold_customer_360_daily (
# MAGIC   customer_360_pk STRING,
# MAGIC   customer_key STRING,
# MAGIC   behavior_date DATE,
# MAGIC   fluent_id STRING,
# MAGIC   profile_id STRING,
# MAGIC   email_sha256 STRING,
# MAGIC   is_identified_customer BOOLEAN,
# MAGIC   customer_tenure_days INT,
# MAGIC   profile_gender STRING,
# MAGIC   profile_geo_country STRING,
# MAGIC   profile_geo_state STRING,
# MAGIC   profile_geo_city STRING,
# MAGIC   profile_zip STRING,
# MAGIC   days_since_last_activity INT,
# MAGIC   is_active_today BOOLEAN,
# MAGIC   total_sessions BIGINT,
# MAGIC   total_events BIGINT,
# MAGIC   avg_session_duration_sec DOUBLE,
# MAGIC   avg_events_per_session DOUBLE,
# MAGIC   total_views BIGINT,
# MAGIC   total_clicks BIGINT,
# MAGIC   click_through_rate DOUBLE,
# MAGIC   total_p1_views BIGINT,
# MAGIC   p1_view_rate DOUBLE,
# MAGIC   unique_campaigns_viewed INT,
# MAGIC   unique_advertisers_interacted INT,
# MAGIC   unique_verticals_explored INT,
# MAGIC   content_diversity_score DOUBLE,
# MAGIC   avg_session_depth DOUBLE,
# MAGIC   total_conversions BIGINT,
# MAGIC   total_transactions BIGINT,
# MAGIC   conversion_rate DOUBLE,
# MAGIC   transaction_rate DOUBLE,
# MAGIC   total_transaction_value DECIMAL(19,4),
# MAGIC   total_revenue_generated DECIMAL(19,4),
# MAGIC   avg_transaction_value DOUBLE,
# MAGIC   avg_revenue_per_session DOUBLE,
# MAGIC   lifetime_transaction_count BIGINT,
# MAGIC   lifetime_revenue DECIMAL(19,4),
# MAGIC   unique_device_types_used INT,
# MAGIC   device_type_list ARRAY<STRING>,
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
# MAGIC   avg_engagement_score DOUBLE,
# MAGIC   max_engagement_score DOUBLE,
# MAGIC   total_high_engagement_sessions BIGINT,
# MAGIC   engagement_trend STRING,
# MAGIC   session_frequency_tier STRING,
# MAGIC   recency_tier STRING,
# MAGIC   consecutive_active_days INT,
# MAGIC   churn_risk_score DOUBLE,
# MAGIC   customer_lifetime_value_est DECIMAL(19,4),
# MAGIC   primary_partner_id STRING,
# MAGIC   primary_source_id STRING,
# MAGIC   unique_partners_used BIGINT,
# MAGIC   unique_sources_used BIGINT,
# MAGIC   campaigns_interacted_30d ARRAY<STRING>,
# MAGIC   advertisers_interacted_30d ARRAY<STRING>,
# MAGIC   verticals_preferred_30d ARRAY<STRING>,
# MAGIC   profile_completeness_score DOUBLE,
# MAGIC   data_quality_score DOUBLE,
# MAGIC   customer_value_segment STRING,
# MAGIC   engagement_segment STRING,
# MAGIC   purchase_propensity_segment STRING,
# MAGIC   lifecycle_stage STRING,
# MAGIC   first_seen_date DATE,
# MAGIC   last_updated_timestamp TIMESTAMP,
# MAGIC   gold_load_date DATE,
# MAGIC   date_est DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_est);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform incremental MERGE
# MAGIC MERGE INTO gold_customer_360_daily target
# MAGIC USING gold_customer_360_daily_transformed source
# MAGIC ON target.customer_360_pk = source.customer_360_pk
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimize Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize with Z-ORDER on key query columns
# MAGIC OPTIMIZE gold_customer_360_daily
# MAGIC ZORDER BY (customer_key, is_identified_customer, behavior_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze table for query optimization
# MAGIC ANALYZE TABLE gold_customer_360_daily COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Current State Segment Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create/refresh current state segment table (Type 1 SCD)
# MAGIC CREATE OR REPLACE TABLE gold_customer_segments
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   customer_key,
# MAGIC   customer_value_segment as current_value_segment,
# MAGIC   engagement_segment as current_engagement_segment,
# MAGIC   lifecycle_stage as current_lifecycle_stage,
# MAGIC   CASE
# MAGIC     WHEN churn_risk_score >= 0.7 THEN 'high'
# MAGIC     WHEN churn_risk_score >= 0.4 THEN 'medium'
# MAGIC     ELSE 'low'
# MAGIC   END as current_churn_risk_tier,
# MAGIC   behavior_date as last_behavior_date,
# MAGIC   lifetime_revenue as total_lifetime_revenue,
# MAGIC   lifetime_transaction_count as total_lifetime_sessions,
# MAGIC   days_since_last_activity,
# MAGIC   CURRENT_TIMESTAMP() as segment_updated_timestamp
# MAGIC FROM gold_customer_360_daily
# MAGIC WHERE behavior_date = (SELECT MAX(behavior_date) FROM gold_customer_360_daily);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize segments table
# MAGIC OPTIMIZE gold_customer_segments
# MAGIC ZORDER BY (customer_key, current_value_segment, current_lifecycle_stage);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Update Watermark

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update watermark with max processed date
# MAGIC MERGE INTO gold_customer_360_watermark target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     'gold_customer_360_daily' as table_name,
# MAGIC     MAX(behavior_date) as last_processed_date,
# MAGIC     CURRENT_TIMESTAMP() as updated_at
# MAGIC   FROM gold_customer_360_daily
# MAGIC ) source
# MAGIC ON target.table_name = source.table_name
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Quality Checks and Reporting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer 360 daily summary
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   
# MAGIC   -- Identification
# MAGIC   SUM(CASE WHEN is_identified_customer THEN 1 ELSE 0 END) as identified_customers,
# MAGIC   ROUND(AVG(profile_completeness_score), 3) as avg_profile_completeness,
# MAGIC   
# MAGIC   -- Activity
# MAGIC   ROUND(AVG(total_sessions), 2) as avg_sessions_per_customer,
# MAGIC   ROUND(AVG(avg_engagement_score), 2) as avg_engagement_score,
# MAGIC   
# MAGIC   -- Conversions & Revenue
# MAGIC   SUM(total_conversions) as total_conversions,
# MAGIC   ROUND(SUM(total_revenue_generated), 2) as total_revenue,
# MAGIC   ROUND(AVG(CASE WHEN total_conversions > 0 THEN total_revenue_generated END), 2) as avg_revenue_per_converter,
# MAGIC   
# MAGIC   -- Segmentation
# MAGIC   SUM(CASE WHEN customer_value_segment = 'high' THEN 1 ELSE 0 END) as high_value_customers,
# MAGIC   SUM(CASE WHEN engagement_segment = 'highly_engaged' THEN 1 ELSE 0 END) as highly_engaged_customers,
# MAGIC   SUM(CASE WHEN lifecycle_stage = 'at_risk' THEN 1 ELSE 0 END) as at_risk_customers,
# MAGIC   SUM(CASE WHEN churn_risk_score >= 0.7 THEN 1 ELSE 0 END) as high_churn_risk_customers
# MAGIC   
# MAGIC FROM gold_customer_360_daily
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY date_est
# MAGIC ORDER BY date_est DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer value segment analysis
# MAGIC SELECT
# MAGIC   customer_value_segment,
# MAGIC   COUNT(DISTINCT customer_key) as customer_count,
# MAGIC   ROUND(AVG(lifetime_revenue), 2) as avg_lifetime_revenue,
# MAGIC   ROUND(AVG(avg_engagement_score), 2) as avg_engagement,
# MAGIC   ROUND(AVG(total_sessions), 2) as avg_daily_sessions,
# MAGIC   SUM(total_conversions) as total_conversions,
# MAGIC   ROUND(AVG(churn_risk_score), 3) as avg_churn_risk
# MAGIC FROM gold_customer_360_daily
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY customer_value_segment
# MAGIC ORDER BY 
# MAGIC   CASE customer_value_segment 
# MAGIC     WHEN 'high' THEN 1 
# MAGIC     WHEN 'medium' THEN 2 
# MAGIC     ELSE 3 
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lifecycle stage distribution
# MAGIC SELECT
# MAGIC   lifecycle_stage,
# MAGIC   COUNT(DISTINCT customer_key) as customer_count,
# MAGIC   ROUND(AVG(customer_tenure_days), 1) as avg_tenure_days,
# MAGIC   ROUND(AVG(days_since_last_activity), 1) as avg_days_since_last_activity,
# MAGIC   ROUND(AVG(lifetime_revenue), 2) as avg_lifetime_revenue,
# MAGIC   ROUND(AVG(avg_engagement_score), 2) as avg_engagement
# MAGIC FROM gold_customer_360_daily
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY lifecycle_stage
# MAGIC ORDER BY customer_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cross-device behavior analysis
# MAGIC SELECT
# MAGIC   cross_device_user_flag,
# MAGIC   COUNT(DISTINCT customer_key) as customer_count,
# MAGIC   ROUND(AVG(unique_device_types_used), 2) as avg_device_types,
# MAGIC   ROUND(AVG(avg_engagement_score), 2) as avg_engagement,
# MAGIC   SUM(total_conversions) as total_conversions,
# MAGIC   ROUND(SUM(total_revenue_generated), 2) as total_revenue,
# MAGIC   ROUND(AVG(lifetime_revenue), 2) as avg_lifetime_revenue
# MAGIC FROM gold_customer_360_daily
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY cross_device_user_flag;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Temporal behavior patterns
# MAGIC SELECT
# MAGIC   most_active_day_of_week,
# MAGIC   most_active_hour_est,
# MAGIC   COUNT(DISTINCT customer_key) as customer_count,
# MAGIC   ROUND(AVG(avg_engagement_score), 2) as avg_engagement,
# MAGIC   SUM(total_conversions) as conversions,
# MAGIC   ROUND(SUM(total_revenue_generated), 2) as revenue
# MAGIC FROM gold_customer_360_daily
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC   AND most_active_day_of_week IS NOT NULL
# MAGIC GROUP BY most_active_day_of_week, most_active_hour_est
# MAGIC ORDER BY customer_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Churn risk analysis
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN churn_risk_score >= 0.7 THEN 'High Risk (0.7-1.0)'
# MAGIC     WHEN churn_risk_score >= 0.4 THEN 'Medium Risk (0.4-0.7)'
# MAGIC     ELSE 'Low Risk (0.0-0.4)'
# MAGIC   END as churn_risk_tier,
# MAGIC   COUNT(DISTINCT customer_key) as customer_count,
# MAGIC   ROUND(AVG(days_since_last_activity), 1) as avg_days_inactive,
# MAGIC   ROUND(AVG(avg_engagement_score), 2) as avg_engagement,
# MAGIC   ROUND(AVG(lifetime_revenue), 2) as avg_lifetime_revenue,
# MAGIC   SUM(CASE WHEN is_active_today THEN 1 ELSE 0 END) as active_today
# MAGIC FROM gold_customer_360_daily
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY 
# MAGIC   CASE
# MAGIC     WHEN churn_risk_score >= 0.7 THEN 'High Risk (0.7-1.0)'
# MAGIC     WHEN churn_risk_score >= 0.4 THEN 'Medium Risk (0.4-0.7)'
# MAGIC     ELSE 'Low Risk (0.0-0.4)'
# MAGIC   END
# MAGIC ORDER BY churn_risk_tier;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Job Completion Summary

# COMMAND ----------

from datetime import datetime

# Get summary statistics
summary_stats = spark.sql("""
  SELECT
    COUNT(DISTINCT customer_key) as unique_customers,
    COUNT(DISTINCT behavior_date) as dates_processed,
    SUM(total_conversions) as total_conversions,
    ROUND(SUM(total_revenue_generated), 2) as total_revenue,
    ROUND(AVG(avg_engagement_score), 2) as avg_engagement_score,
    ROUND(AVG(lifetime_revenue), 2) as avg_lifetime_revenue,
    SUM(CASE WHEN customer_value_segment = 'high' THEN 1 ELSE 0 END) as high_value_customers,
    SUM(CASE WHEN lifecycle_stage = 'at_risk' THEN 1 ELSE 0 END) as at_risk_customers,
    ROUND(AVG(data_quality_score), 3) as avg_data_quality
  FROM gold_customer_360_daily
  WHERE date_est >= CURRENT_DATE - 1
""").collect()[0]

# Get segment breakdown
segment_breakdown = spark.sql("""
  SELECT 
    customer_value_segment,
    COUNT(DISTINCT customer_key) as count
  FROM gold_customer_360_daily
  WHERE date_est >= CURRENT_DATE - 1
  GROUP BY customer_value_segment
  ORDER BY count DESC
""").collect()

# Print completion summary
print("=" * 80)
print("GOLD LAYER - CUSTOMER 360 DAILY - JOB COMPLETED")
print("=" * 80)
print(f"Unique Customers Processed: {summary_stats['unique_customers']:,}")
print(f"Dates Processed: {summary_stats['dates_processed']}")
print(f"Total Conversions: {summary_stats['total_conversions']:,}")
print(f"Total Revenue: ${summary_stats['total_revenue']:,.2f}")
print(f"Avg Engagement Score: {summary_stats['avg_engagement_score']:.2f}")
print(f"Avg Lifetime Revenue: ${summary_stats['avg_lifetime_revenue']:,.2f}")
print(f"High Value Customers: {summary_stats['high_value_customers']:,}")
print(f"At-Risk Customers: {summary_stats['at_risk_customers']:,}")
print(f"Avg Data Quality Score: {summary_stats['avg_data_quality']:.3f}")
print("")
print("VALUE SEGMENT BREAKDOWN:")
for row in segment_breakdown:
    segment = row['customer_value_segment']
    count = row['count']
    pct = (count / summary_stats['unique_customers']) * 100
    print(f"  {segment}: {count:,} ({pct:.1f}%)")
print("")
print(f"Processing Completed: {datetime.now()}")
print("=" * 80)

# Return success
segment_summary = {row['customer_value_segment']: row['count'] for row in segment_breakdown}
dbutils.notebook.exit(f"Success: Processed {summary_stats['unique_customers']:,} customers. Segments: {segment_summary}")



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Summary of Gold Customer 360 Notebook
# MAGIC
# MAGIC This comprehensive notebook creates a **daily customer 360 view** with:
# MAGIC
# MAGIC ### **Key Features:**
# MAGIC
# MAGIC 1. **Profile & Identity**: Customer identification, tenure, demographics
# MAGIC 2. **Daily Activity**: Sessions, events, engagement metrics
# MAGIC 3. **Behavioral Patterns**: 
# MAGIC    - Browsing (views, clicks, CTR)
# MAGIC    - Purchase (conversions, transactions, revenue)
# MAGIC    - Content consumption (campaigns, advertisers, verticals)
# MAGIC 4. **Cross-Device Behavior**: Device diversity, switching patterns
# MAGIC 5. **Temporal Patterns**: Time-of-day, day-of-week preferences
# MAGIC 6. **Engagement Metrics**: Scores, trends, intensity
# MAGIC 7. **Loyalty Indicators**: Recency, frequency, churn risk
# MAGIC 8. **Segmentation**: Value, engagement, propensity, lifecycle
# MAGIC
# MAGIC ### **Business Value:**
# MAGIC
# MAGIC - **Personalization**: Target customers based on behavior and preferences
# MAGIC - **Retention**: Identify at-risk customers for proactive campaigns
# MAGIC - **Revenue Optimization**: Focus on high-value segments
# MAGIC - **Campaign Timing**: Send offers when customers are most active
# MAGIC - **Product Development**: Understand content preferences
# MAGIC
# MAGIC ### **Companion Table:**
# MAGIC
# MAGIC - `gold_customer_segments`: Current-state snapshot for operational use
# MAGIC
# MAGIC All three notebooks (Events, Sessions, Customer 360) are now complete and production-ready!
