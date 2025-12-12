# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Event-Level Metrics Views (Unity Catalog)
# MAGIC
# MAGIC **Purpose:** Create pre-computed metrics views for granular event/session analytics using Databricks Unity Catalog.
# MAGIC
# MAGIC **Views in this notebook (13 total):**
# MAGIC 1. Hourly Performance Metrics
# MAGIC 2. Campaign Performance Metrics
# MAGIC 3. Session Engagement Metrics
# MAGIC 4. Customer Identity Metrics
# MAGIC 5. Conversion Funnel Metrics
# MAGIC 6. Device Performance Metrics
# MAGIC 7. Geographic Performance Metrics
# MAGIC 8. Traffic Source Metrics
# MAGIC 9. Temporal Patterns Metrics
# MAGIC 10. Session Funnel Metrics
# MAGIC 11. Repeat Users Daily
# MAGIC 12. New vs Returning Performance
# MAGIC 13. Cohort Retention
# MAGIC
# MAGIC **For customer-level metrics, use gold_customer_360_mv.py instead:**
# MAGIC - `customer_daily_kpis` - Daily executive KPIs (more performant)
# MAGIC - `customer_360_metrics` - Customer lifetime + RFM scores
# MAGIC - `repeat_customer_summary` - Visit frequency distribution
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - silver_customer_events_enriched
# MAGIC - silver_customer_sessions_enriched
# MAGIC
# MAGIC **Target:** Views in Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

spark.sql("USE CATALOG centraldata_sandbox")
spark.sql("USE SCHEMA test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Hourly Performance Metrics View
# MAGIC
# MAGIC For time-of-day optimization and real-time dashboards.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_metrics_hourly_performance AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   event_hour_est as hour_est,
# MAGIC   is_business_hours,
# MAGIC
# MAGIC   -- Volume
# MAGIC   COUNT(*) as events,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT session_id) as unique_sessions,
# MAGIC
# MAGIC   -- Event Types
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
# MAGIC   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
# MAGIC
# MAGIC   -- Conversions & Revenue
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id
# MAGIC   END) as conversions,
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)), 2) as revenue,
# MAGIC
# MAGIC   -- Rates
# MAGIC   ROUND(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0), 2) as ctr_pct,
# MAGIC   ROUND(COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END), 0), 2) as conversion_rate_pct
# MAGIC
# MAGIC FROM silver_customer_events_enriched
# MAGIC GROUP BY date_est, event_hour_est, is_business_hours;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview hourly metrics for today
# MAGIC SELECT * FROM gold_metrics_hourly_performance
# MAGIC WHERE date_est = CURRENT_DATE
# MAGIC ORDER BY hour_est;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Campaign Performance Metrics View
# MAGIC
# MAGIC For campaign optimization and advertiser reporting.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_metrics_campaign_performance AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   campaign_id,
# MAGIC   campaign_name,
# MAGIC   advertiser_id,
# MAGIC   advertiser_name,
# MAGIC   campaign_type,
# MAGIC   campaign_vertical,
# MAGIC
# MAGIC   -- Volume
# MAGIC   COUNT(*) as events,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT session_id) as unique_sessions,
# MAGIC
# MAGIC   -- Event Breakdown
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
# MAGIC   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
# MAGIC   SUM(CASE WHEN is_p1_view THEN 1 ELSE 0 END) as p1_views,
# MAGIC
# MAGIC   -- Conversions
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id
# MAGIC   END) as conversions,
# MAGIC
# MAGIC   -- Revenue
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)), 2) as revenue,
# MAGIC   ROUND(AVG(CASE WHEN revenue > 0 THEN revenue END), 2) as avg_order_value,
# MAGIC
# MAGIC   -- Rates
# MAGIC   ROUND(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0), 2) as ctr_pct,
# MAGIC   ROUND(COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END), 0), 2) as conversion_rate_pct,
# MAGIC
# MAGIC   -- P1 Rate
# MAGIC   ROUND(SUM(CASE WHEN is_p1_view THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0), 2) as p1_rate_pct,
# MAGIC
# MAGIC   -- Revenue per customer
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)) / NULLIF(COUNT(DISTINCT customer_key), 0), 2) as revenue_per_customer
# MAGIC
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE campaign_id IS NOT NULL
# MAGIC GROUP BY date_est, campaign_id, campaign_name, advertiser_id, advertiser_name, campaign_type, campaign_vertical;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top campaigns by revenue (last 7 days)
# MAGIC SELECT
# MAGIC   campaign_name,
# MAGIC   advertiser_name,
# MAGIC   SUM(views) as total_views,
# MAGIC   SUM(clicks) as total_clicks,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate
# MAGIC FROM gold_metrics_campaign_performance
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY campaign_name, advertiser_name
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Session Engagement Metrics View
# MAGIC
# MAGIC For session-level behavior analysis.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_metrics_session_engagement AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC
# MAGIC   -- Session Volume
# MAGIC   COUNT(*) as total_sessions,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   SUM(CASE WHEN is_identified_session THEN 1 ELSE 0 END) as identified_sessions,
# MAGIC
# MAGIC   -- Session Quality
# MAGIC   ROUND(AVG(session_duration_seconds), 2) as avg_session_duration_sec,
# MAGIC   ROUND(AVG(total_events), 2) as avg_events_per_session,
# MAGIC   ROUND(AVG(session_depth), 2) as avg_session_depth,
# MAGIC   ROUND(AVG(engagement_score), 2) as avg_engagement_score,
# MAGIC
# MAGIC   -- Bounce Rate (single-event sessions)
# MAGIC   SUM(CASE WHEN total_events = 1 THEN 1 ELSE 0 END) as bounce_sessions,
# MAGIC   ROUND(SUM(CASE WHEN total_events = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as bounce_rate_pct,
# MAGIC
# MAGIC   -- Conversion Metrics
# MAGIC   SUM(CASE WHEN has_conversion THEN 1 ELSE 0 END) as sessions_with_conversion,
# MAGIC   SUM(conversion_count) as total_conversions,
# MAGIC   ROUND(SUM(CASE WHEN has_conversion THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as session_conversion_rate_pct,
# MAGIC
# MAGIC   -- Revenue
# MAGIC   ROUND(SUM(total_session_revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(CASE WHEN total_session_revenue > 0 THEN total_session_revenue END), 2) as avg_revenue_per_converting_session,
# MAGIC
# MAGIC   -- Device Behavior
# MAGIC   SUM(CASE WHEN device_switches > 0 THEN 1 ELSE 0 END) as sessions_with_device_switch,
# MAGIC   ROUND(SUM(CASE WHEN device_switches > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as device_switch_rate_pct,
# MAGIC
# MAGIC   -- Engagement Tiers
# MAGIC   SUM(CASE WHEN engagement_score >= 75 THEN 1 ELSE 0 END) as high_engagement_sessions,
# MAGIC   SUM(CASE WHEN engagement_score >= 50 AND engagement_score < 75 THEN 1 ELSE 0 END) as medium_engagement_sessions,
# MAGIC   SUM(CASE WHEN engagement_score >= 25 AND engagement_score < 50 THEN 1 ELSE 0 END) as low_engagement_sessions,
# MAGIC   SUM(CASE WHEN engagement_score < 25 THEN 1 ELSE 0 END) as very_low_engagement_sessions
# MAGIC
# MAGIC FROM silver_customer_sessions_enriched
# MAGIC GROUP BY date_est;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview session engagement metrics
# MAGIC SELECT * FROM gold_metrics_session_engagement
# MAGIC ORDER BY date_est DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Customer Identity Metrics View
# MAGIC
# MAGIC For tracking identity resolution effectiveness.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_metrics_customer_identity AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC
# MAGIC   -- Total Counts
# MAGIC   COUNT(*) as total_events,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC
# MAGIC   -- By Identity Source
# MAGIC   COUNT(DISTINCT CASE WHEN customer_key_source = 'email_sha256' THEN customer_key END) as email_sha256_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN customer_key_source = 'email_md5' THEN customer_key END) as email_md5_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN customer_key_source = 'email' THEN customer_key END) as email_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN customer_key_source = 'phone' THEN customer_key END) as phone_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN customer_key_source = 'telephone' THEN customer_key END) as telephone_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN customer_key_source = 'session_id' THEN customer_key END) as anonymous_customers,
# MAGIC
# MAGIC   -- Event counts by source
# MAGIC   SUM(CASE WHEN customer_key_source = 'email_sha256' THEN 1 ELSE 0 END) as email_sha256_events,
# MAGIC   SUM(CASE WHEN customer_key_source = 'email_md5' THEN 1 ELSE 0 END) as email_md5_events,
# MAGIC   SUM(CASE WHEN customer_key_source = 'email' THEN 1 ELSE 0 END) as email_events,
# MAGIC   SUM(CASE WHEN customer_key_source = 'phone' THEN 1 ELSE 0 END) as phone_events,
# MAGIC   SUM(CASE WHEN customer_key_source = 'telephone' THEN 1 ELSE 0 END) as telephone_events,
# MAGIC   SUM(CASE WHEN customer_key_source = 'session_id' THEN 1 ELSE 0 END) as anonymous_events,
# MAGIC
# MAGIC   -- Identification Rates
# MAGIC   ROUND(COUNT(DISTINCT CASE WHEN is_identified_user THEN customer_key END) * 100.0 /
# MAGIC         NULLIF(COUNT(DISTINCT customer_key), 0), 2) as customer_identification_rate_pct,
# MAGIC   ROUND(SUM(CASE WHEN is_identified_user THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as event_identification_rate_pct,
# MAGIC
# MAGIC   -- Conversion by Identity
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click' AND is_identified_user
# MAGIC     THEN source_reference_id
# MAGIC   END) as identified_conversions,
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click' AND NOT is_identified_user
# MAGIC     THEN source_reference_id
# MAGIC   END) as anonymous_conversions,
# MAGIC
# MAGIC   -- Revenue by Identity
# MAGIC   ROUND(SUM(CASE WHEN is_identified_user THEN COALESCE(revenue, 0) ELSE 0 END), 2) as identified_revenue,
# MAGIC   ROUND(SUM(CASE WHEN NOT is_identified_user THEN COALESCE(revenue, 0) ELSE 0 END), 2) as anonymous_revenue
# MAGIC
# MAGIC FROM silver_customer_events_enriched
# MAGIC GROUP BY date_est;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview identity metrics
# MAGIC SELECT * FROM gold_metrics_customer_identity
# MAGIC ORDER BY date_est DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Conversion Funnel Metrics View
# MAGIC
# MAGIC For funnel analysis and optimization.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_metrics_conversion_funnel AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC
# MAGIC   -- Funnel Stages (unique customers at each stage)
# MAGIC   COUNT(DISTINCT customer_key) as total_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN event_type = 'view' THEN customer_key END) as viewers,
# MAGIC   COUNT(DISTINCT CASE WHEN event_type = 'click' THEN customer_key END) as clickers,
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN customer_key
# MAGIC   END) as converters,
# MAGIC   COUNT(DISTINCT CASE WHEN revenue > 0 THEN customer_key END) as buyers,
# MAGIC
# MAGIC   -- Event Counts
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as view_events,
# MAGIC   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as click_events,
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id
# MAGIC   END) as conversion_events,
# MAGIC
# MAGIC   -- Funnel Conversion Rates
# MAGIC   ROUND(COUNT(DISTINCT CASE WHEN event_type = 'click' THEN customer_key END) * 100.0 /
# MAGIC         NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'view' THEN customer_key END), 0), 2) as view_to_click_rate,
# MAGIC   ROUND(COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN customer_key END) * 100.0 /
# MAGIC         NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'click' THEN customer_key END), 0), 2) as click_to_convert_rate,
# MAGIC   ROUND(COUNT(DISTINCT CASE WHEN revenue > 0 THEN customer_key END) * 100.0 /
# MAGIC         NULLIF(COUNT(DISTINCT CASE
# MAGIC           WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC           THEN customer_key END), 0), 2) as convert_to_buy_rate,
# MAGIC
# MAGIC   -- Overall Funnel Rate
# MAGIC   ROUND(COUNT(DISTINCT CASE WHEN revenue > 0 THEN customer_key END) * 100.0 /
# MAGIC         NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'view' THEN customer_key END), 0), 2) as overall_conversion_rate,
# MAGIC
# MAGIC   -- Revenue
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)), 2) as total_revenue,
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)) / NULLIF(COUNT(DISTINCT CASE WHEN revenue > 0 THEN customer_key END), 0), 2) as avg_revenue_per_buyer
# MAGIC
# MAGIC FROM silver_customer_events_enriched
# MAGIC GROUP BY date_est;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview funnel metrics
# MAGIC SELECT * FROM gold_metrics_conversion_funnel
# MAGIC ORDER BY date_est DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Device Performance Metrics View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_metrics_device_performance AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   device_type,
# MAGIC   is_mobile,
# MAGIC   is_tablet,
# MAGIC   is_desktop,
# MAGIC
# MAGIC   -- Volume
# MAGIC   COUNT(*) as events,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT session_id) as unique_sessions,
# MAGIC
# MAGIC   -- Event Breakdown
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
# MAGIC   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
# MAGIC
# MAGIC   -- Conversions & Revenue
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id
# MAGIC   END) as conversions,
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)), 2) as revenue,
# MAGIC   ROUND(AVG(CASE WHEN revenue > 0 THEN revenue END), 2) as avg_order_value,
# MAGIC
# MAGIC   -- Rates
# MAGIC   ROUND(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0), 2) as ctr_pct,
# MAGIC   ROUND(COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END), 0), 2) as conversion_rate_pct
# MAGIC
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE device_type IS NOT NULL
# MAGIC GROUP BY date_est, device_type, is_mobile, is_tablet, is_desktop;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Device performance summary (last 7 days)
# MAGIC SELECT
# MAGIC   device_type,
# MAGIC   SUM(events) as total_events,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate
# MAGIC FROM gold_metrics_device_performance
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY device_type
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Geographic Performance Metrics View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_metrics_geographic_performance AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   country,
# MAGIC   state,
# MAGIC
# MAGIC   -- Volume
# MAGIC   COUNT(*) as events,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT session_id) as unique_sessions,
# MAGIC
# MAGIC   -- Event Breakdown
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
# MAGIC   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
# MAGIC
# MAGIC   -- Conversions & Revenue
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id
# MAGIC   END) as conversions,
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)), 2) as revenue,
# MAGIC
# MAGIC   -- Rates
# MAGIC   ROUND(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0), 2) as ctr_pct,
# MAGIC   ROUND(COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END), 0), 2) as conversion_rate_pct,
# MAGIC
# MAGIC   -- Revenue per customer
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)) / NULLIF(COUNT(DISTINCT customer_key), 0), 2) as revenue_per_customer
# MAGIC
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE country IS NOT NULL
# MAGIC GROUP BY date_est, country, state;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top states by revenue (last 7 days)
# MAGIC SELECT
# MAGIC   state,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate
# MAGIC FROM gold_metrics_geographic_performance
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC   AND country = 'US'
# MAGIC   AND state IS NOT NULL
# MAGIC GROUP BY state
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Traffic Source Metrics View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_metrics_traffic_source AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   partner_id,
# MAGIC   partner_name,
# MAGIC   traffic_partner_type,
# MAGIC   source_id,
# MAGIC
# MAGIC   -- Volume
# MAGIC   COUNT(*) as events,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT session_id) as unique_sessions,
# MAGIC
# MAGIC   -- Event Breakdown
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
# MAGIC   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
# MAGIC
# MAGIC   -- Conversions & Revenue
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id
# MAGIC   END) as conversions,
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)), 2) as revenue,
# MAGIC
# MAGIC   -- Rates
# MAGIC   ROUND(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0), 2) as ctr_pct,
# MAGIC   ROUND(COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END), 0), 2) as conversion_rate_pct,
# MAGIC
# MAGIC   -- Per Customer Metrics
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)) / NULLIF(COUNT(DISTINCT customer_key), 0), 2) as revenue_per_customer,
# MAGIC   ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT customer_key), 0), 2) as events_per_customer
# MAGIC
# MAGIC FROM silver_customer_events_enriched
# MAGIC WHERE partner_id IS NOT NULL
# MAGIC GROUP BY date_est, partner_id, partner_name, traffic_partner_type, source_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top traffic partners by revenue (last 7 days)
# MAGIC SELECT
# MAGIC   partner_name,
# MAGIC   traffic_partner_type,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate
# MAGIC FROM gold_metrics_traffic_source
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY partner_name, traffic_partner_type
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Day of Week / Temporal Metrics View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_metrics_temporal_patterns AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC   day_of_week,
# MAGIC   is_weekend,
# MAGIC   is_business_hours,
# MAGIC
# MAGIC   -- Volume
# MAGIC   COUNT(*) as events,
# MAGIC   COUNT(DISTINCT customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT session_id) as unique_sessions,
# MAGIC
# MAGIC   -- Event Breakdown
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
# MAGIC   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
# MAGIC
# MAGIC   -- Conversions & Revenue
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id
# MAGIC   END) as conversions,
# MAGIC   ROUND(SUM(COALESCE(revenue, 0)), 2) as revenue,
# MAGIC
# MAGIC   -- Rates
# MAGIC   ROUND(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0), 2) as ctr_pct,
# MAGIC   ROUND(COUNT(DISTINCT CASE
# MAGIC     WHEN source_reference = 'offer-convert' AND conversion_type_name != 'Click'
# MAGIC     THEN source_reference_id END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END), 0), 2) as conversion_rate_pct
# MAGIC
# MAGIC FROM silver_customer_events_enriched
# MAGIC GROUP BY date_est, day_of_week, is_weekend, is_business_hours;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Day of week performance summary (last 30 days)
# MAGIC SELECT
# MAGIC   day_of_week,
# MAGIC   is_weekend,
# MAGIC   SUM(events) as total_events,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate
# MAGIC FROM gold_metrics_temporal_patterns
# MAGIC WHERE date_est >= CURRENT_DATE - 30
# MAGIC GROUP BY day_of_week, is_weekend
# MAGIC ORDER BY
# MAGIC   CASE day_of_week
# MAGIC     WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
# MAGIC     WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6 ELSE 7
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Session Funnel Metrics View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_metrics_session_funnel AS
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC
# MAGIC   -- Session Counts
# MAGIC   COUNT(*) as total_sessions,
# MAGIC   SUM(CASE WHEN view_events > 0 THEN 1 ELSE 0 END) as sessions_with_view,
# MAGIC   SUM(CASE WHEN click_events > 0 THEN 1 ELSE 0 END) as sessions_with_click,
# MAGIC   SUM(CASE WHEN has_conversion THEN 1 ELSE 0 END) as sessions_with_conversion,
# MAGIC   SUM(CASE WHEN has_transaction THEN 1 ELSE 0 END) as sessions_with_transaction,
# MAGIC
# MAGIC   -- Funnel Rates
# MAGIC   ROUND(SUM(CASE WHEN click_events > 0 THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN view_events > 0 THEN 1 ELSE 0 END), 0), 2) as view_to_click_rate,
# MAGIC   ROUND(SUM(CASE WHEN has_conversion THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN click_events > 0 THEN 1 ELSE 0 END), 0), 2) as click_to_convert_rate,
# MAGIC   ROUND(SUM(CASE WHEN has_transaction THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN has_conversion THEN 1 ELSE 0 END), 0), 2) as convert_to_transaction_rate,
# MAGIC
# MAGIC   -- Overall Rate
# MAGIC   ROUND(SUM(CASE WHEN has_transaction THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as overall_transaction_rate,
# MAGIC
# MAGIC   -- Revenue
# MAGIC   ROUND(SUM(total_session_revenue), 2) as total_revenue,
# MAGIC   SUM(conversion_count) as total_conversions
# MAGIC
# MAGIC FROM silver_customer_sessions_enriched
# MAGIC GROUP BY date_est;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview session funnel metrics
# MAGIC SELECT * FROM gold_metrics_session_funnel
# MAGIC ORDER BY date_est DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Repeat User History Metrics Views
# MAGIC
# MAGIC Track returning customers and new vs returning performance.
# MAGIC
# MAGIC **Note:** For customer lifetime metrics, RFM segments, and visit frequency buckets,
# MAGIC use the more performant views from gold_customer_360_mv.py:
# MAGIC - `customer_360_metrics` - Customer lifetime with pre-calculated CLV and RFM
# MAGIC - `repeat_customer_summary` - Visit frequency distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily new vs returning customers metrics
# MAGIC CREATE OR REPLACE VIEW gold_metrics_repeat_users_daily AS
# MAGIC WITH customer_first_seen AS (
# MAGIC   SELECT
# MAGIC     customer_key,
# MAGIC     MIN(date_est) as first_seen_date
# MAGIC   FROM silver_customer_events_enriched
# MAGIC   WHERE is_identified_user = TRUE
# MAGIC   GROUP BY customer_key
# MAGIC ),
# MAGIC daily_customers AS (
# MAGIC   SELECT DISTINCT
# MAGIC     e.date_est,
# MAGIC     e.customer_key,
# MAGIC     f.first_seen_date,
# MAGIC     CASE WHEN e.date_est = f.first_seen_date THEN 'new' ELSE 'returning' END as customer_type
# MAGIC   FROM silver_customer_events_enriched e
# MAGIC   JOIN customer_first_seen f ON e.customer_key = f.customer_key
# MAGIC   WHERE e.is_identified_user = TRUE
# MAGIC )
# MAGIC SELECT
# MAGIC   date_est,
# MAGIC
# MAGIC   -- Customer Counts
# MAGIC   COUNT(DISTINCT customer_key) as total_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN customer_type = 'new' THEN customer_key END) as new_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN customer_type = 'returning' THEN customer_key END) as returning_customers,
# MAGIC
# MAGIC   -- Rates
# MAGIC   ROUND(COUNT(DISTINCT CASE WHEN customer_type = 'new' THEN customer_key END) * 100.0 /
# MAGIC         NULLIF(COUNT(DISTINCT customer_key), 0), 2) as new_customer_pct,
# MAGIC   ROUND(COUNT(DISTINCT CASE WHEN customer_type = 'returning' THEN customer_key END) * 100.0 /
# MAGIC         NULLIF(COUNT(DISTINCT customer_key), 0), 2) as returning_customer_pct
# MAGIC
# MAGIC FROM daily_customers
# MAGIC GROUP BY date_est;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview new vs returning
# MAGIC SELECT * FROM gold_metrics_repeat_users_daily
# MAGIC ORDER BY date_est DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Repeat user behavior comparison (new vs returning performance)
# MAGIC CREATE OR REPLACE VIEW gold_metrics_new_vs_returning_performance AS
# MAGIC WITH customer_first_seen AS (
# MAGIC   SELECT
# MAGIC     customer_key,
# MAGIC     MIN(date_est) as first_seen_date
# MAGIC   FROM silver_customer_events_enriched
# MAGIC   WHERE is_identified_user = TRUE
# MAGIC   GROUP BY customer_key
# MAGIC )
# MAGIC SELECT
# MAGIC   e.date_est,
# MAGIC   CASE WHEN e.date_est = f.first_seen_date THEN 'new' ELSE 'returning' END as customer_type,
# MAGIC
# MAGIC   -- Volume
# MAGIC   COUNT(*) as events,
# MAGIC   COUNT(DISTINCT e.customer_key) as unique_customers,
# MAGIC   COUNT(DISTINCT e.session_id) as unique_sessions,
# MAGIC
# MAGIC   -- Event Breakdown
# MAGIC   SUM(CASE WHEN e.event_type = 'view' THEN 1 ELSE 0 END) as views,
# MAGIC   SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) as clicks,
# MAGIC
# MAGIC   -- Conversions & Revenue
# MAGIC   COUNT(DISTINCT CASE
# MAGIC     WHEN e.source_reference = 'offer-convert' AND e.conversion_type_name != 'Click'
# MAGIC     THEN e.source_reference_id
# MAGIC   END) as conversions,
# MAGIC   ROUND(SUM(COALESCE(e.revenue, 0)), 2) as revenue,
# MAGIC
# MAGIC   -- Per Customer Metrics
# MAGIC   ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT e.customer_key), 0), 2) as events_per_customer,
# MAGIC   ROUND(SUM(COALESCE(e.revenue, 0)) / NULLIF(COUNT(DISTINCT e.customer_key), 0), 2) as revenue_per_customer,
# MAGIC
# MAGIC   -- Rates
# MAGIC   ROUND(SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN e.event_type = 'view' THEN 1 ELSE 0 END), 0), 2) as ctr_pct,
# MAGIC   ROUND(COUNT(DISTINCT CASE
# MAGIC     WHEN e.source_reference = 'offer-convert' AND e.conversion_type_name != 'Click'
# MAGIC     THEN e.source_reference_id END) * 100.0 /
# MAGIC         NULLIF(SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END), 0), 2) as conversion_rate_pct
# MAGIC
# MAGIC FROM silver_customer_events_enriched e
# MAGIC JOIN customer_first_seen f ON e.customer_key = f.customer_key
# MAGIC WHERE e.is_identified_user = TRUE
# MAGIC GROUP BY e.date_est, CASE WHEN e.date_est = f.first_seen_date THEN 'new' ELSE 'returning' END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare new vs returning performance (last 7 days)
# MAGIC SELECT
# MAGIC   customer_type,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(events) as total_events,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(events_per_customer), 2) as avg_events_per_customer,
# MAGIC   ROUND(AVG(revenue_per_customer), 2) as avg_revenue_per_customer,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate
# MAGIC FROM gold_metrics_new_vs_returning_performance
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY customer_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer cohort retention (monthly cohorts)
# MAGIC CREATE OR REPLACE VIEW gold_metrics_cohort_retention AS
# MAGIC WITH customer_cohort AS (
# MAGIC   SELECT
# MAGIC     customer_key,
# MAGIC     DATE_TRUNC('month', MIN(date_est)) as cohort_month
# MAGIC   FROM silver_customer_events_enriched
# MAGIC   WHERE is_identified_user = TRUE
# MAGIC   GROUP BY customer_key
# MAGIC ),
# MAGIC customer_activity AS (
# MAGIC   SELECT DISTINCT
# MAGIC     e.customer_key,
# MAGIC     c.cohort_month,
# MAGIC     DATE_TRUNC('month', e.date_est) as activity_month,
# MAGIC     MONTHS_BETWEEN(DATE_TRUNC('month', e.date_est), c.cohort_month) as months_since_cohort
# MAGIC   FROM silver_customer_events_enriched e
# MAGIC   JOIN customer_cohort c ON e.customer_key = c.customer_key
# MAGIC   WHERE e.is_identified_user = TRUE
# MAGIC )
# MAGIC SELECT
# MAGIC   cohort_month,
# MAGIC   CAST(months_since_cohort AS INT) as month_number,
# MAGIC   COUNT(DISTINCT customer_key) as active_customers,
# MAGIC   FIRST_VALUE(COUNT(DISTINCT customer_key)) OVER (
# MAGIC     PARTITION BY cohort_month ORDER BY months_since_cohort
# MAGIC   ) as cohort_size,
# MAGIC   ROUND(COUNT(DISTINCT customer_key) * 100.0 /
# MAGIC         FIRST_VALUE(COUNT(DISTINCT customer_key)) OVER (
# MAGIC           PARTITION BY cohort_month ORDER BY months_since_cohort
# MAGIC         ), 2) as retention_rate_pct
# MAGIC FROM customer_activity
# MAGIC WHERE months_since_cohort >= 0 AND months_since_cohort <= 12
# MAGIC GROUP BY cohort_month, months_since_cohort
# MAGIC ORDER BY cohort_month, months_since_cohort;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview cohort retention (recent cohorts)
# MAGIC SELECT * FROM gold_metrics_cohort_retention
# MAGIC WHERE cohort_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 6 MONTHS)
# MAGIC ORDER BY cohort_month DESC, month_number;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Summary: All Available Metrics Views

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all metrics views created
# MAGIC SHOW VIEWS LIKE 'gold_metrics_*';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics Views Summary
# MAGIC
# MAGIC ### Event-Level Metrics Views (10)
# MAGIC | View Name | Purpose | Key Metrics |
# MAGIC |-----------|---------|-------------|
# MAGIC | `gold_metrics_hourly_performance` | Time-of-day optimization | Hourly events, conversions, revenue |
# MAGIC | `gold_metrics_campaign_performance` | Campaign optimization | Campaign-level CTR, conversion rate, revenue |
# MAGIC | `gold_metrics_session_engagement` | Session behavior analysis | Duration, depth, engagement score, bounce rate |
# MAGIC | `gold_metrics_customer_identity` | Identity resolution tracking | Identity source breakdown, identification rates |
# MAGIC | `gold_metrics_conversion_funnel` | Funnel analysis | View→Click→Convert→Buy rates |
# MAGIC | `gold_metrics_device_performance` | Device optimization | Device-level metrics |
# MAGIC | `gold_metrics_geographic_performance` | Geographic targeting | Country/state performance |
# MAGIC | `gold_metrics_traffic_source` | Traffic source ROI | Partner/source performance |
# MAGIC | `gold_metrics_temporal_patterns` | Timing optimization | Day-of-week, business hours patterns |
# MAGIC | `gold_metrics_session_funnel` | Session-level funnel | Session conversion funnel |
# MAGIC
# MAGIC ### Repeat User Metrics (3)
# MAGIC | View Name | Purpose | Key Metrics |
# MAGIC |-----------|---------|-------------|
# MAGIC | `gold_metrics_repeat_users_daily` | Daily new vs returning | New/returning customer counts and percentages |
# MAGIC | `gold_metrics_new_vs_returning_performance` | Behavior comparison | New vs returning: events, revenue, CTR, conversion rate |
# MAGIC | `gold_metrics_cohort_retention` | Monthly cohort retention | Cohort size, retention rates by month |
# MAGIC
# MAGIC ### Use gold_customer_360_mv.py for (more performant):
# MAGIC | View Name | Purpose |
# MAGIC |-----------|---------|
# MAGIC | `customer_daily_kpis` | Daily executive KPIs (instead of gold_metrics_daily_summary) |
# MAGIC | `customer_360_metrics` | Customer lifetime + RFM (instead of gold_metrics_customer_lifetime, gold_metrics_rfm_segments) |
# MAGIC | `repeat_customer_summary` | Visit frequency buckets (instead of gold_metrics_visit_frequency) |

# COMMAND ----------

print("=" * 80)
print("GOLD METRICS LAYER - VIEWS CREATED SUCCESSFULLY")
print("=" * 80)
print("\nEvent-Level Metrics Views (10):")
print("  1.  gold_metrics_hourly_performance")
print("  2.  gold_metrics_campaign_performance")
print("  3.  gold_metrics_session_engagement")
print("  4.  gold_metrics_customer_identity")
print("  5.  gold_metrics_conversion_funnel")
print("  6.  gold_metrics_device_performance")
print("  7.  gold_metrics_geographic_performance")
print("  8.  gold_metrics_traffic_source")
print("  9.  gold_metrics_temporal_patterns")
print("  10. gold_metrics_session_funnel")
print("\nRepeat User Metrics (3):")
print("  11. gold_metrics_repeat_users_daily")
print("  12. gold_metrics_new_vs_returning_performance")
print("  13. gold_metrics_cohort_retention")
print("\nFor customer-level metrics (more performant), use gold_customer_360_mv.py:")
print("  - customer_daily_kpis (daily executive KPIs)")
print("  - customer_360_metrics (customer lifetime + RFM)")
print("  - repeat_customer_summary (visit frequency)")
print("\nUsage: SELECT * FROM gold_metrics_hourly_performance WHERE date_est = CURRENT_DATE")
print("=" * 80)
