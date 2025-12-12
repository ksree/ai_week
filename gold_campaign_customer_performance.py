# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC # Gold Layer: Campaign Customer Performance Table
# MAGIC
# MAGIC **Purpose:** Pre-aggregated table showing customer performance metrics per campaign
# MAGIC - Enables fast queries like "Top customers by revenue for Monopoly campaign"
# MAGIC - Campaign-level attribution for customer value
# MAGIC - Daily incremental refresh from silver_customer_events_enriched
# MAGIC
# MAGIC **Source:** silver_customer_events_enriched
# MAGIC **Target:** gold_campaign_customer_performance (Delta Table)
# MAGIC **Grain:** One row per customer_key + campaign_id + date_est

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

spark.sql("USE CATALOG centraldata_sandbox")
spark.sql("USE SCHEMA test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Gold Campaign Customer Performance Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold Campaign Customer Performance Table
# MAGIC -- Pre-aggregated customer metrics at the campaign level
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold_campaign_customer_performance (
# MAGIC   -- Keys
# MAGIC   customer_key STRING NOT NULL COMMENT 'Unique customer identifier',
# MAGIC   campaign_id STRING NOT NULL COMMENT 'Campaign identifier',
# MAGIC   date_est DATE NOT NULL COMMENT 'Date of activity (EST timezone)',
# MAGIC
# MAGIC   -- Campaign Info (denormalized for query convenience)
# MAGIC   campaign_name STRING COMMENT 'Campaign name',
# MAGIC   advertiser_id STRING COMMENT 'Advertiser ID',
# MAGIC   advertiser_name STRING COMMENT 'Advertiser name',
# MAGIC   vertical STRING COMMENT 'Campaign vertical/category',
# MAGIC
# MAGIC   -- Customer Info (denormalized)
# MAGIC   is_identified_customer BOOLEAN COMMENT 'True if customer has profile ID',
# MAGIC   fluent_id STRING COMMENT 'Fluent ID for customer',
# MAGIC
# MAGIC   -- Engagement Metrics
# MAGIC   total_impressions BIGINT COMMENT 'Total impressions for this campaign',
# MAGIC   total_views BIGINT COMMENT 'Total views for this campaign',
# MAGIC   total_clicks BIGINT COMMENT 'Total clicks on this campaign',
# MAGIC   click_through_rate DOUBLE COMMENT 'CTR = clicks / impressions * 100',
# MAGIC   total_p1_views BIGINT COMMENT 'Primary position views',
# MAGIC
# MAGIC   -- Conversion Metrics
# MAGIC   total_conversions BIGINT COMMENT 'Conversions attributed to this campaign',
# MAGIC   conversion_rate DOUBLE COMMENT 'Conversion rate = conversions / clicks * 100',
# MAGIC   total_transactions BIGINT COMMENT 'Transactions from this campaign',
# MAGIC
# MAGIC   -- Revenue Metrics
# MAGIC   total_revenue DOUBLE COMMENT 'Revenue attributed to this campaign',
# MAGIC   total_transaction_value DOUBLE COMMENT 'Transaction value from this campaign',
# MAGIC   avg_order_value DOUBLE COMMENT 'Average order value',
# MAGIC
# MAGIC   -- Session Metrics
# MAGIC   total_sessions BIGINT COMMENT 'Sessions with this campaign exposure',
# MAGIC   unique_creative_count BIGINT COMMENT 'Unique creatives seen',
# MAGIC
# MAGIC   -- Time Metrics
# MAGIC   first_interaction_time TIMESTAMP COMMENT 'First interaction with campaign',
# MAGIC   last_interaction_time TIMESTAMP COMMENT 'Last interaction with campaign',
# MAGIC
# MAGIC   -- Metadata
# MAGIC   last_updated_timestamp TIMESTAMP COMMENT 'When this record was last updated'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_est)
# MAGIC COMMENT 'Gold layer table with customer performance metrics per campaign, enabling campaign-level customer analysis'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Incremental Load via MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Incremental MERGE for daily refresh
# MAGIC -- Run this daily after silver_customer_events_enriched is updated
# MAGIC
# MAGIC MERGE INTO gold_campaign_customer_performance AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     -- Keys
# MAGIC     customer_key,
# MAGIC     campaign_id,
# MAGIC     date_est,
# MAGIC
# MAGIC     -- Campaign Info (take first non-null value)
# MAGIC     FIRST_VALUE(campaign_name, TRUE) as campaign_name,
# MAGIC     FIRST_VALUE(advertiser_id, TRUE) as advertiser_id,
# MAGIC     FIRST_VALUE(advertiser_name, TRUE) as advertiser_name,
# MAGIC     FIRST_VALUE(campaign_vertical, TRUE) as vertical,
# MAGIC
# MAGIC     -- Customer Info
# MAGIC     MAX(is_identified_user) as is_identified_customer,
# MAGIC     FIRST_VALUE(fluent_id, TRUE) as fluent_id,
# MAGIC
# MAGIC     -- Engagement Metrics
# MAGIC     COUNT(*) as total_impressions,
# MAGIC     SUM(CASE WHEN event_type IN ('view', 'offer-view') THEN 1 ELSE 0 END) as total_views,
# MAGIC     SUM(CASE WHEN event_type IN ('click', 'offer-click') THEN 1 ELSE 0 END) as total_clicks,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN event_type IN ('click', 'offer-click') THEN 1 ELSE 0 END) * 100.0 /
# MAGIC       NULLIF(COUNT(*), 0), 2
# MAGIC     ) as click_through_rate,
# MAGIC     SUM(CASE WHEN is_p1_view THEN 1 ELSE 0 END) as total_p1_views,
# MAGIC
# MAGIC     -- Conversion Metrics
# MAGIC     SUM(CASE
# MAGIC       WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC       THEN 1 ELSE 0
# MAGIC     END) as total_conversions,
# MAGIC     ROUND(
# MAGIC       SUM(CASE
# MAGIC         WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC         THEN 1 ELSE 0
# MAGIC       END) * 100.0 /
# MAGIC       NULLIF(SUM(CASE WHEN event_type IN ('click', 'offer-click') THEN 1 ELSE 0 END), 0), 2
# MAGIC     ) as conversion_rate,
# MAGIC     SUM(CASE WHEN conversion_type_name = 'Transaction' THEN 1 ELSE 0 END) as total_transactions,
# MAGIC
# MAGIC     -- Revenue Metrics
# MAGIC     COALESCE(SUM(revenue), 0) as total_revenue,
# MAGIC     COALESCE(SUM(revenue), 0) as total_transaction_value,
# MAGIC     ROUND(
# MAGIC       COALESCE(SUM(revenue), 0) /
# MAGIC       NULLIF(SUM(CASE WHEN conversion_type_name = 'Transaction' THEN 1 ELSE 0 END), 0), 2
# MAGIC     ) as avg_order_value,
# MAGIC
# MAGIC     -- Session Metrics
# MAGIC     COUNT(DISTINCT session_id) as total_sessions,
# MAGIC     COUNT(DISTINCT creative_id) as unique_creative_count,
# MAGIC
# MAGIC     -- Time Metrics
# MAGIC     MIN(event_timestamp) as first_interaction_time,
# MAGIC     MAX(event_timestamp) as last_interaction_time,
# MAGIC
# MAGIC     -- Metadata
# MAGIC     CURRENT_TIMESTAMP() as last_updated_timestamp
# MAGIC
# MAGIC   FROM silver_customer_events_enriched
# MAGIC   WHERE campaign_id IS NOT NULL
# MAGIC     AND customer_key IS NOT NULL
# MAGIC     AND date_est >= CURRENT_DATE - 7  -- Process last 7 days for incremental
# MAGIC   GROUP BY customer_key, campaign_id, date_est
# MAGIC ) AS source
# MAGIC ON target.customer_key = source.customer_key
# MAGIC   AND target.campaign_id = source.campaign_id
# MAGIC   AND target.date_est = source.date_est
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.campaign_name = source.campaign_name,
# MAGIC   target.advertiser_id = source.advertiser_id,
# MAGIC   target.advertiser_name = source.advertiser_name,
# MAGIC   target.vertical = source.vertical,
# MAGIC   target.is_identified_customer = source.is_identified_customer,
# MAGIC   target.fluent_id = source.fluent_id,
# MAGIC   target.total_impressions = source.total_impressions,
# MAGIC   target.total_views = source.total_views,
# MAGIC   target.total_clicks = source.total_clicks,
# MAGIC   target.click_through_rate = source.click_through_rate,
# MAGIC   target.total_p1_views = source.total_p1_views,
# MAGIC   target.total_conversions = source.total_conversions,
# MAGIC   target.conversion_rate = source.conversion_rate,
# MAGIC   target.total_transactions = source.total_transactions,
# MAGIC   target.total_revenue = source.total_revenue,
# MAGIC   target.total_transaction_value = source.total_transaction_value,
# MAGIC   target.avg_order_value = source.avg_order_value,
# MAGIC   target.total_sessions = source.total_sessions,
# MAGIC   target.unique_creative_count = source.unique_creative_count,
# MAGIC   target.first_interaction_time = source.first_interaction_time,
# MAGIC   target.last_interaction_time = source.last_interaction_time,
# MAGIC   target.last_updated_timestamp = source.last_updated_timestamp
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Initial Full Load (Run Once)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Initial full load - uncomment and run once for historical data
# MAGIC -- WARNING: This will take time for large datasets
# MAGIC
# MAGIC -- INSERT OVERWRITE TABLE gold_campaign_customer_performance
# MAGIC -- SELECT
# MAGIC --   customer_key,
# MAGIC --   campaign_id,
# MAGIC --   date_est,
# MAGIC --   FIRST_VALUE(campaign_name, TRUE) as campaign_name,
# MAGIC --   FIRST_VALUE(advertiser_id, TRUE) as advertiser_id,
# MAGIC --   FIRST_VALUE(advertiser_name, TRUE) as advertiser_name,
# MAGIC --   FIRST_VALUE(campaign_vertical, TRUE) as vertical,
# MAGIC --   MAX(is_identified_user) as is_identified_customer,
# MAGIC --   FIRST_VALUE(fluent_id, TRUE) as fluent_id,
# MAGIC --   COUNT(*) as total_impressions,
# MAGIC --   SUM(CASE WHEN event_type IN ('view', 'offer-view') THEN 1 ELSE 0 END) as total_views,
# MAGIC --   SUM(CASE WHEN event_type IN ('click', 'offer-click') THEN 1 ELSE 0 END) as total_clicks,
# MAGIC --   ROUND(SUM(CASE WHEN event_type IN ('click', 'offer-click') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as click_through_rate,
# MAGIC --   SUM(CASE WHEN is_p1_view THEN 1 ELSE 0 END) as total_p1_views,
# MAGIC --   SUM(CASE WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click' THEN 1 ELSE 0 END) as total_conversions,
# MAGIC --   ROUND(SUM(CASE WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click' THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN event_type IN ('click', 'offer-click') THEN 1 ELSE 0 END), 0), 2) as conversion_rate,
# MAGIC --   SUM(CASE WHEN conversion_type_name = 'Transaction' THEN 1 ELSE 0 END) as total_transactions,
# MAGIC --   COALESCE(SUM(revenue), 0) as total_revenue,
# MAGIC --   COALESCE(SUM(revenue), 0) as total_transaction_value,
# MAGIC --   ROUND(COALESCE(SUM(revenue), 0) / NULLIF(SUM(CASE WHEN conversion_type_name = 'Transaction' THEN 1 ELSE 0 END), 0), 2) as avg_order_value,
# MAGIC --   COUNT(DISTINCT session_id) as total_sessions,
# MAGIC --   COUNT(DISTINCT creative_id) as unique_creative_count,
# MAGIC --   MIN(event_timestamp) as first_interaction_time,
# MAGIC --   MAX(event_timestamp) as last_interaction_time,
# MAGIC --   CURRENT_TIMESTAMP() as last_updated_timestamp
# MAGIC -- FROM silver_customer_events_enriched
# MAGIC -- WHERE campaign_id IS NOT NULL AND customer_key IS NOT NULL
# MAGIC -- GROUP BY customer_key, campaign_id, date_est;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Optimize Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize for common query patterns
# MAGIC OPTIMIZE gold_campaign_customer_performance
# MAGIC ZORDER BY (campaign_id, customer_key);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create UC Metrics Views for Campaign Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top Customers by Campaign View
# MAGIC -- Answers: "Who are the top customers for campaign X?"
# MAGIC
# MAGIC CREATE OR REPLACE VIEW campaign_top_customers
# MAGIC COMMENT 'Top customers ranked by revenue for each campaign - use to answer "Who are top customers for Monopoly campaign?"'
# MAGIC AS
# MAGIC WITH campaign_customer_totals AS (
# MAGIC   SELECT
# MAGIC     campaign_id,
# MAGIC     campaign_name,
# MAGIC     advertiser_id,
# MAGIC     advertiser_name,
# MAGIC     vertical,
# MAGIC     customer_key,
# MAGIC     MAX(is_identified_customer) as is_identified_customer,
# MAGIC     MAX(fluent_id) as fluent_id,
# MAGIC
# MAGIC     -- Aggregate across all dates
# MAGIC     SUM(total_impressions) as total_impressions,
# MAGIC     SUM(total_views) as total_views,
# MAGIC     SUM(total_clicks) as total_clicks,
# MAGIC     SUM(total_conversions) as total_conversions,
# MAGIC     SUM(total_transactions) as total_transactions,
# MAGIC     SUM(total_revenue) as total_revenue,
# MAGIC     SUM(total_transaction_value) as total_transaction_value,
# MAGIC     SUM(total_sessions) as total_sessions,
# MAGIC     COUNT(DISTINCT date_est) as active_days,
# MAGIC     MIN(first_interaction_time) as first_interaction,
# MAGIC     MAX(last_interaction_time) as last_interaction
# MAGIC
# MAGIC   FROM gold_campaign_customer_performance
# MAGIC   GROUP BY campaign_id, campaign_name, advertiser_id, advertiser_name, vertical, customer_key
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY total_revenue DESC) as revenue_rank,
# MAGIC     ROUND(total_clicks * 100.0 / NULLIF(total_impressions, 0), 2) as overall_ctr,
# MAGIC     ROUND(total_conversions * 100.0 / NULLIF(total_clicks, 0), 2) as overall_conversion_rate,
# MAGIC     ROUND(total_revenue / NULLIF(total_transactions, 0), 2) as avg_order_value
# MAGIC   FROM campaign_customer_totals
# MAGIC )
# MAGIC SELECT * FROM ranked
# MAGIC ORDER BY campaign_id, revenue_rank;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Campaign Performance Summary View
# MAGIC -- Aggregated campaign metrics across all customers
# MAGIC
# MAGIC CREATE OR REPLACE VIEW campaign_performance_summary
# MAGIC COMMENT 'Campaign-level performance summary with customer and revenue metrics'
# MAGIC AS
# MAGIC SELECT
# MAGIC   campaign_id,
# MAGIC   campaign_name,
# MAGIC   advertiser_id,
# MAGIC   advertiser_name,
# MAGIC   vertical,
# MAGIC
# MAGIC   -- Customer Metrics
# MAGIC   COUNT(DISTINCT customer_key) as total_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN is_identified_customer THEN customer_key END) as identified_customers,
# MAGIC
# MAGIC   -- Engagement Metrics
# MAGIC   SUM(total_impressions) as total_impressions,
# MAGIC   SUM(total_views) as total_views,
# MAGIC   SUM(total_clicks) as total_clicks,
# MAGIC   ROUND(SUM(total_clicks) * 100.0 / NULLIF(SUM(total_impressions), 0), 2) as overall_ctr,
# MAGIC
# MAGIC   -- Conversion Metrics
# MAGIC   SUM(total_conversions) as total_conversions,
# MAGIC   ROUND(SUM(total_conversions) * 100.0 / NULLIF(SUM(total_clicks), 0), 2) as overall_conversion_rate,
# MAGIC   SUM(total_transactions) as total_transactions,
# MAGIC
# MAGIC   -- Revenue Metrics
# MAGIC   ROUND(SUM(total_revenue), 2) as total_revenue,
# MAGIC   ROUND(SUM(total_transaction_value), 2) as total_transaction_value,
# MAGIC   ROUND(SUM(total_revenue) / NULLIF(COUNT(DISTINCT customer_key), 0), 2) as revenue_per_customer,
# MAGIC   ROUND(SUM(total_transaction_value) / NULLIF(SUM(total_transactions), 0), 2) as avg_order_value,
# MAGIC
# MAGIC   -- Session Metrics
# MAGIC   SUM(total_sessions) as total_sessions,
# MAGIC   ROUND(SUM(total_sessions) * 1.0 / NULLIF(COUNT(DISTINCT customer_key), 0), 2) as sessions_per_customer,
# MAGIC
# MAGIC   -- Date Range
# MAGIC   MIN(date_est) as first_date,
# MAGIC   MAX(date_est) as last_date,
# MAGIC   COUNT(DISTINCT date_est) as active_days
# MAGIC
# MAGIC FROM gold_campaign_customer_performance
# MAGIC GROUP BY campaign_id, campaign_name, advertiser_id, advertiser_name, vertical
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily Campaign Performance View
# MAGIC -- For time-series analysis
# MAGIC
# MAGIC CREATE OR REPLACE VIEW campaign_daily_performance
# MAGIC COMMENT 'Daily campaign metrics for time-series analysis'
# MAGIC AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   campaign_id,
# MAGIC   campaign_name,
# MAGIC   advertiser_name,
# MAGIC   vertical,
# MAGIC
# MAGIC   COUNT(DISTINCT customer_key) as daily_customers,
# MAGIC   SUM(total_impressions) as impressions,
# MAGIC   SUM(total_clicks) as clicks,
# MAGIC   ROUND(SUM(total_clicks) * 100.0 / NULLIF(SUM(total_impressions), 0), 2) as ctr,
# MAGIC   SUM(total_conversions) as conversions,
# MAGIC   SUM(total_transactions) as transactions,
# MAGIC   ROUND(SUM(total_revenue), 2) as revenue,
# MAGIC   ROUND(SUM(total_revenue) / NULLIF(COUNT(DISTINCT customer_key), 0), 2) as revenue_per_customer
# MAGIC
# MAGIC FROM gold_campaign_customer_performance
# MAGIC GROUP BY date_est, campaign_id, campaign_name, advertiser_name, vertical
# MAGIC ORDER BY date_est DESC, revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Example Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Top 10 customers by revenue for a specific campaign
# MAGIC -- Replace 'monopoly' with actual campaign name or use campaign_id
# MAGIC
# MAGIC SELECT
# MAGIC   customer_key,
# MAGIC   fluent_id,
# MAGIC   is_identified_customer,
# MAGIC   campaign_name,
# MAGIC   total_revenue,
# MAGIC   total_transactions,
# MAGIC   avg_order_value,
# MAGIC   total_conversions,
# MAGIC   overall_ctr,
# MAGIC   overall_conversion_rate,
# MAGIC   active_days,
# MAGIC   first_interaction,
# MAGIC   last_interaction
# MAGIC FROM campaign_top_customers
# MAGIC WHERE LOWER(campaign_name) LIKE '%monopoly%'
# MAGIC   AND revenue_rank <= 10
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Campaign performance comparison
# MAGIC SELECT
# MAGIC   campaign_name,
# MAGIC   advertiser_name,
# MAGIC   total_customers,
# MAGIC   total_revenue,
# MAGIC   revenue_per_customer,
# MAGIC   overall_ctr,
# MAGIC   overall_conversion_rate,
# MAGIC   avg_order_value
# MAGIC FROM campaign_performance_summary
# MAGIC WHERE total_revenue > 0
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check row counts and date range
# MAGIC SELECT
# MAGIC   COUNT(*) as total_rows,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT campaign_id) as unique_campaigns,
# MAGIC   MIN(date_est) as min_date,
# MAGIC   MAX(date_est) as max_date,
# MAGIC   ROUND(SUM(total_revenue), 2) as total_revenue
# MAGIC FROM gold_campaign_customer_performance;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Completion Summary

# COMMAND ----------

print("=" * 80)
print("GOLD CAMPAIGN CUSTOMER PERFORMANCE - CREATION COMPLETED")
print("=" * 80)
print("\nTable Created:")
print("  gold_campaign_customer_performance")
print("    - Grain: customer_key + campaign_id + date_est")
print("    - Partitioned by: date_est")
print("    - Z-Ordered by: campaign_id, customer_key")
print("\nViews Created:")
print("  1. campaign_top_customers - Top customers by revenue per campaign")
print("  2. campaign_performance_summary - Campaign-level aggregates")
print("  3. campaign_daily_performance - Daily time-series")
print("\nExample Queries Included:")
print("  - Top 10 customers for Monopoly campaign")
print("  - Campaign performance comparison")
print("\nUsage:")
print("  SELECT * FROM campaign_top_customers")
print("  WHERE LOWER(campaign_name) LIKE '%monopoly%'")
print("  AND revenue_rank <= 10;")
print("=" * 80)
