# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC # Unity Catalog Metrics View: Customer 360 Metrics (CORRECTED)
# MAGIC
# MAGIC **Purpose:** Business-friendly metrics view for customer 360 analysis
# MAGIC - Simplified column names and structure
# MAGIC - Pre-calculated KPIs and ratios
# MAGIC - Latest complete data by default
# MAGIC - PII-safe for broad consumption
# MAGIC - Documented metrics with business definitions
# MAGIC
# MAGIC **Source:** gold_customer_360_daily  
# MAGIC **Target:** customer_360_metrics (UC View)  
# MAGIC **Access:** Analytics teams, BI tools, data consumers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

spark.sql("USE CATALOG centraldata_sandbox")
spark.sql("USE SCHEMA test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1a. Verify Source Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check what columns actually exist in gold_customer_360_daily
# MAGIC DESCRIBE TABLE gold_customer_360_daily;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample the data to see available columns
# MAGIC SELECT * FROM gold_customer_360_daily LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Main Customer 360 Metrics View (CORRECTED)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Main Customer 360 Metrics View
# MAGIC -- This view provides business-friendly access to customer behavior metrics
# MAGIC
# MAGIC CREATE OR REPLACE VIEW customer_360_metrics
# MAGIC COMMENT 'Daily customer behavior metrics with segmentation, engagement, and lifetime value'
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Customer Identifiers (PII-safe)
# MAGIC   customer_key,
# MAGIC   behavior_date as metric_date,
# MAGIC   is_identified_customer,
# MAGIC   customer_tenure_days as days_as_customer,
# MAGIC   
# MAGIC   -- Geographic Attributes
# MAGIC   profile_geo_country as country,
# MAGIC   profile_geo_state as state,
# MAGIC   profile_geo_city as city,
# MAGIC   profile_gender as gender,
# MAGIC   
# MAGIC   -- Activity Metrics
# MAGIC   is_active_today,
# MAGIC   days_since_last_activity,
# MAGIC   total_sessions as sessions,
# MAGIC   total_events as events,
# MAGIC   ROUND(avg_session_duration_sec / 60, 2) as avg_session_duration_min,
# MAGIC   ROUND(avg_events_per_session, 1) as avg_events_per_session,
# MAGIC   
# MAGIC   -- Engagement Metrics
# MAGIC   total_views as views,
# MAGIC   total_clicks as clicks,
# MAGIC   click_through_rate as ctr_pct,
# MAGIC   total_p1_views as primary_position_views,
# MAGIC   p1_view_rate as p1_view_rate_pct,
# MAGIC   unique_campaigns_viewed as campaigns_viewed,
# MAGIC   unique_advertisers_interacted as advertisers_interacted,
# MAGIC   unique_verticals_explored as verticals_explored,
# MAGIC   ROUND(content_diversity_score * 100, 1) as content_diversity_score_pct,
# MAGIC   ROUND(avg_session_depth, 1) as avg_campaign_depth,
# MAGIC   ROUND(avg_engagement_score, 1) as engagement_score,
# MAGIC   ROUND(max_engagement_score, 1) as max_engagement_score,
# MAGIC   total_high_engagement_sessions as high_engagement_sessions,
# MAGIC   
# MAGIC   -- Conversion & Revenue Metrics
# MAGIC   total_conversions as conversions,
# MAGIC   total_transactions as transactions,
# MAGIC   conversion_rate as conversion_rate_pct,
# MAGIC   transaction_rate as transaction_rate_pct,
# MAGIC   total_revenue_generated as daily_revenue,
# MAGIC   avg_revenue_per_session,
# MAGIC   total_transaction_value as transaction_value,
# MAGIC   avg_transaction_value,
# MAGIC   
# MAGIC   -- Lifetime Metrics
# MAGIC   lifetime_revenue,
# MAGIC   lifetime_transaction_count as lifetime_transactions,
# MAGIC   customer_lifetime_value_est as estimated_clv,
# MAGIC   
# MAGIC   -- Device Behavior
# MAGIC   unique_device_types_used as device_types_used,
# MAGIC   primary_device_type as primary_device,
# MAGIC   mobile_session_pct as mobile_pct,
# MAGIC   desktop_session_pct as desktop_pct,
# MAGIC   tablet_session_pct as tablet_pct,
# MAGIC   cross_device_user_flag as is_cross_device_user,
# MAGIC   
# MAGIC   -- Temporal Patterns
# MAGIC   business_hours_sessions,
# MAGIC   after_hours_sessions,
# MAGIC   weekend_sessions,
# MAGIC   weekday_sessions,
# MAGIC   most_active_hour_est as peak_activity_hour,
# MAGIC   most_active_day_of_week as peak_activity_day,
# MAGIC   morning_activity_pct,
# MAGIC   afternoon_activity_pct,
# MAGIC   evening_activity_pct,
# MAGIC   night_activity_pct,
# MAGIC   
# MAGIC   -- Traffic Sources
# MAGIC   primary_partner_id as primary_partner,
# MAGIC   primary_source_id as primary_source,
# MAGIC   unique_partners_used as partners_used,
# MAGIC   unique_sources_used as sources_used,
# MAGIC   
# MAGIC   -- Segmentation
# MAGIC   customer_value_segment as value_segment,
# MAGIC   engagement_segment,
# MAGIC   purchase_propensity_segment as propensity_segment,
# MAGIC   lifecycle_stage,
# MAGIC   session_frequency_tier as frequency_tier,
# MAGIC   ROUND(churn_risk_score * 100, 1) as churn_risk_score_pct,
# MAGIC   CASE 
# MAGIC     WHEN churn_risk_score >= 0.7 THEN 'High Risk'
# MAGIC     WHEN churn_risk_score >= 0.4 THEN 'Medium Risk'
# MAGIC     ELSE 'Low Risk'
# MAGIC   END as churn_risk_tier,
# MAGIC   
# MAGIC   -- Data Quality
# MAGIC   ROUND(profile_completeness_score * 100, 1) as profile_completeness_pct,
# MAGIC   ROUND(data_quality_score * 100, 1) as data_quality_score_pct,
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   last_updated_timestamp as last_updated
# MAGIC   
# MAGIC FROM gold_customer_360_daily
# MAGIC WHERE data_quality_score >= 0.5  -- Only include quality data
# MAGIC   AND date_est >= CURRENT_DATE - 90;  -- Rolling 90 days

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Aggregated Summary Views

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer Segments Summary View
# MAGIC -- Pre-aggregated segment-level metrics for dashboards
# MAGIC
# MAGIC CREATE OR REPLACE VIEW customer_segments_summary
# MAGIC COMMENT 'Daily aggregated metrics by customer segment'
# MAGIC AS
# MAGIC SELECT
# MAGIC   metric_date,
# MAGIC   value_segment,
# MAGIC   engagement_segment,
# MAGIC   lifecycle_stage,
# MAGIC   
# MAGIC   -- Customer Counts
# MAGIC   COUNT(DISTINCT customer_key) as customer_count,
# MAGIC   SUM(CASE WHEN is_active_today THEN 1 ELSE 0 END) as active_customers,
# MAGIC   SUM(CASE WHEN is_cross_device_user THEN 1 ELSE 0 END) as cross_device_customers,
# MAGIC   
# MAGIC   -- Engagement Metrics
# MAGIC   ROUND(AVG(engagement_score), 1) as avg_engagement_score,
# MAGIC   ROUND(AVG(sessions), 1) as avg_sessions,
# MAGIC   ROUND(AVG(avg_session_duration_min), 2) as avg_session_duration_min,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr_pct,
# MAGIC   
# MAGIC   -- Conversion Metrics
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate_pct,
# MAGIC   SUM(transactions) as total_transactions,
# MAGIC   
# MAGIC   -- Revenue Metrics
# MAGIC   ROUND(SUM(daily_revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(daily_revenue), 2) as avg_revenue_per_customer,
# MAGIC   ROUND(SUM(lifetime_revenue), 2) as total_lifetime_revenue,
# MAGIC   ROUND(AVG(lifetime_revenue), 2) as avg_lifetime_revenue,
# MAGIC   ROUND(AVG(estimated_clv), 2) as avg_estimated_clv,
# MAGIC   
# MAGIC   -- Risk Metrics
# MAGIC   ROUND(AVG(churn_risk_score_pct), 1) as avg_churn_risk_pct,
# MAGIC   SUM(CASE WHEN churn_risk_tier = 'High Risk' THEN 1 ELSE 0 END) as high_risk_customers,
# MAGIC   
# MAGIC   -- Data Quality
# MAGIC   ROUND(AVG(profile_completeness_pct), 1) as avg_profile_completeness_pct
# MAGIC   
# MAGIC FROM customer_360_metrics
# MAGIC GROUP BY metric_date, value_segment, engagement_segment, lifecycle_stage;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily Customer Metrics Summary View
# MAGIC -- High-level daily KPIs for executive dashboards
# MAGIC
# MAGIC CREATE OR REPLACE VIEW customer_daily_kpis
# MAGIC COMMENT 'Daily high-level customer KPIs for executive reporting'
# MAGIC AS
# MAGIC SELECT
# MAGIC   metric_date,
# MAGIC   
# MAGIC   -- Customer Counts by Segment
# MAGIC   COUNT(DISTINCT customer_key) as total_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN is_active_today THEN customer_key END) as active_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN lifecycle_stage = 'new' THEN customer_key END) as new_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN lifecycle_stage = 'at_risk' THEN customer_key END) as at_risk_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN lifecycle_stage = 'churned' THEN customer_key END) as churned_customers,
# MAGIC   
# MAGIC   -- Value Segments
# MAGIC   COUNT(DISTINCT CASE WHEN value_segment = 'high_value' THEN customer_key END) as high_value_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN value_segment = 'medium_value' THEN customer_key END) as medium_value_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN value_segment = 'low_value' THEN customer_key END) as low_value_customers,
# MAGIC   
# MAGIC   -- Engagement Metrics
# MAGIC   ROUND(AVG(engagement_score), 1) as avg_engagement_score,
# MAGIC   SUM(sessions) as total_sessions,
# MAGIC   SUM(events) as total_events,
# MAGIC   ROUND(AVG(avg_session_duration_min), 2) as avg_session_duration_min,
# MAGIC   
# MAGIC   -- Conversion Metrics
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate_pct,
# MAGIC   SUM(transactions) as total_transactions,
# MAGIC   
# MAGIC   -- Revenue Metrics
# MAGIC   ROUND(SUM(daily_revenue), 2) as total_daily_revenue,
# MAGIC   ROUND(AVG(daily_revenue), 2) as avg_revenue_per_customer,
# MAGIC   ROUND(SUM(lifetime_revenue), 2) as total_lifetime_revenue,
# MAGIC   ROUND(AVG(estimated_clv), 2) as avg_estimated_clv,
# MAGIC   
# MAGIC   -- Device Behavior
# MAGIC   COUNT(DISTINCT CASE WHEN is_cross_device_user THEN customer_key END) as cross_device_users,
# MAGIC   ROUND(AVG(mobile_pct), 1) as avg_mobile_pct,
# MAGIC   ROUND(AVG(desktop_pct), 1) as avg_desktop_pct,
# MAGIC   
# MAGIC   -- Risk Metrics
# MAGIC   ROUND(AVG(churn_risk_score_pct), 1) as avg_churn_risk_pct,
# MAGIC   COUNT(DISTINCT CASE WHEN churn_risk_tier = 'High Risk' THEN customer_key END) as high_churn_risk_customers,
# MAGIC   
# MAGIC   -- Data Quality
# MAGIC   ROUND(AVG(profile_completeness_pct), 1) as avg_profile_completeness,
# MAGIC   ROUND(AVG(data_quality_score_pct), 1) as avg_data_quality_score
# MAGIC   
# MAGIC FROM customer_360_metrics
# MAGIC GROUP BY metric_date
# MAGIC ORDER BY metric_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Current State View (Latest Data Only)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current Customer State View
# MAGIC -- Most recent snapshot of each customer (for operational use)
# MAGIC
# MAGIC CREATE OR REPLACE VIEW customer_current_state
# MAGIC COMMENT 'Most recent customer metrics and segments (latest snapshot per customer)'
# MAGIC AS
# MAGIC SELECT
# MAGIC   cm.*
# MAGIC FROM customer_360_metrics cm
# MAGIC INNER JOIN (
# MAGIC   SELECT 
# MAGIC     customer_key,
# MAGIC     MAX(metric_date) as latest_date
# MAGIC   FROM customer_360_metrics
# MAGIC   WHERE metric_date >= CURRENT_DATE - 7  -- Only last 7 days for "current"
# MAGIC   GROUP BY customer_key
# MAGIC ) latest
# MAGIC   ON cm.customer_key = latest.customer_key
# MAGIC   AND cm.metric_date = latest.latest_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Specialized Analysis Views

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High-Value Customer View
# MAGIC -- Focus on top customers for VIP treatment
# MAGIC
# MAGIC CREATE OR REPLACE VIEW high_value_customers
# MAGIC COMMENT 'High-value customers with detailed behavior metrics'
# MAGIC AS
# MAGIC SELECT
# MAGIC   customer_key,
# MAGIC   metric_date,
# MAGIC   lifetime_revenue,
# MAGIC   estimated_clv,
# MAGIC   engagement_score,
# MAGIC   sessions,
# MAGIC   conversions,
# MAGIC   daily_revenue,
# MAGIC   churn_risk_tier,
# MAGIC   lifecycle_stage,
# MAGIC   days_as_customer,
# MAGIC   is_cross_device_user,
# MAGIC   primary_device,
# MAGIC   peak_activity_day,
# MAGIC   peak_activity_hour,
# MAGIC   country,
# MAGIC   state
# MAGIC FROM customer_360_metrics
# MAGIC WHERE value_segment = 'high_value'
# MAGIC   AND metric_date >= CURRENT_DATE - 30
# MAGIC ORDER BY lifetime_revenue DESC, metric_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- At-Risk Customer View
# MAGIC -- Customers who need retention campaigns
# MAGIC
# MAGIC CREATE OR REPLACE VIEW at_risk_customers
# MAGIC COMMENT 'Customers at risk of churning with retention signals'
# MAGIC AS
# MAGIC SELECT
# MAGIC   customer_key,
# MAGIC   metric_date,
# MAGIC   churn_risk_tier,
# MAGIC   churn_risk_score_pct,
# MAGIC   lifecycle_stage,
# MAGIC   days_since_last_activity,
# MAGIC   lifetime_revenue,
# MAGIC   engagement_score,
# MAGIC   sessions,
# MAGIC   conversions,
# MAGIC   value_segment,
# MAGIC   frequency_tier,
# MAGIC   country,
# MAGIC   state
# MAGIC FROM customer_360_metrics
# MAGIC WHERE churn_risk_tier IN ('High Risk', 'Medium Risk')
# MAGIC   AND value_segment IN ('high_value', 'medium_value')  -- Focus on valuable at-risk customers
# MAGIC   AND metric_date >= CURRENT_DATE - 30
# MAGIC ORDER BY churn_risk_score_pct DESC, lifetime_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- New Customer Cohort View
# MAGIC -- Track new customer activation and early behavior
# MAGIC
# MAGIC CREATE OR REPLACE VIEW new_customer_cohort
# MAGIC COMMENT 'New customers (first 30 days) with activation metrics'
# MAGIC AS
# MAGIC SELECT
# MAGIC   customer_key,
# MAGIC   metric_date,
# MAGIC   days_as_customer,
# MAGIC   engagement_score,
# MAGIC   sessions,
# MAGIC   events,
# MAGIC   conversions,
# MAGIC   daily_revenue,
# MAGIC   lifetime_revenue,
# MAGIC   is_cross_device_user,
# MAGIC   primary_device,
# MAGIC   propensity_segment,
# MAGIC   country,
# MAGIC   state,
# MAGIC   profile_completeness_pct
# MAGIC FROM customer_360_metrics
# MAGIC WHERE lifecycle_stage = 'new'
# MAGIC   OR days_as_customer <= 30
# MAGIC ORDER BY metric_date DESC, days_as_customer ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Add Column-Level Comments for Documentation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add column comments to main metrics view for self-service documentation
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN customer_key COMMENT 'Unique customer identifier (hashed for privacy)';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN metric_date COMMENT 'Date of behavior measurement (EST timezone)';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN is_identified_customer COMMENT 'True if customer has profile ID, email, or phone';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN days_as_customer COMMENT 'Number of days since first customer interaction';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN sessions COMMENT 'Total number of sessions on this date';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN engagement_score COMMENT 'Engagement score 0-100 based on session activity';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN conversions COMMENT 'Total conversions (sourceReference=offer-convert, excluding Click type)';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN conversion_rate_pct COMMENT 'Conversion rate as percentage (conversions/sessions * 100)';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN daily_revenue COMMENT 'Total revenue generated on this date';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN lifetime_revenue COMMENT 'Cumulative revenue from all time';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN estimated_clv COMMENT 'Estimated customer lifetime value based on behavior patterns';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN value_segment COMMENT 'Customer value tier: high_value, medium_value, low_value, no_value';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN engagement_segment COMMENT 'Engagement tier: highly_engaged, moderately_engaged, low_engaged, minimal';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN lifecycle_stage COMMENT 'Customer lifecycle: new, active, at_risk, churned';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN churn_risk_score_pct COMMENT 'Churn risk score 0-100 (higher = more likely to churn)';
# MAGIC ALTER VIEW customer_360_metrics ALTER COLUMN churn_risk_tier COMMENT 'Churn risk category: High Risk, Medium Risk, Low Risk';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify Views and Test Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test main metrics view
# MAGIC SELECT COUNT(*) as row_count, COUNT(DISTINCT customer_key) as unique_customers
# MAGIC FROM customer_360_metrics
# MAGIC WHERE metric_date >= CURRENT_DATE - 7;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test daily KPIs view
# MAGIC SELECT * FROM customer_daily_kpis
# MAGIC WHERE metric_date >= CURRENT_DATE - 7
# MAGIC ORDER BY metric_date DESC
# MAGIC LIMIT 7;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test segments summary view
# MAGIC SELECT * FROM customer_segments_summary
# MAGIC WHERE metric_date = (SELECT MAX(metric_date) FROM customer_segments_summary)
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test at-risk customers view
# MAGIC SELECT 
# MAGIC   churn_risk_tier,
# MAGIC   value_segment,
# MAGIC   COUNT(DISTINCT customer_key) as customer_count,
# MAGIC   ROUND(AVG(lifetime_revenue), 2) as avg_lifetime_revenue
# MAGIC FROM at_risk_customers
# MAGIC WHERE metric_date >= CURRENT_DATE - 7
# MAGIC GROUP BY churn_risk_tier, value_segment
# MAGIC ORDER BY churn_risk_tier, avg_lifetime_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Completion Summary

# COMMAND ----------

print("=" * 80)
print("UNITY CATALOG METRICS VIEWS - CREATION COMPLETED")
print("=" * 80)
print("\nViews Created:")
print("  1. customer_360_metrics (Main)")
print("  2. customer_daily_kpis (Executive Dashboard)")
print("  3. customer_segments_summary (Segment Analysis)")
print("  4. customer_current_state (Operational)")
print("  5. at_risk_customers (Retention)")
print("  6. high_value_customers (VIP)")
print("  7. new_customer_cohort (Activation)")
print("\nColumns Removed (not in source):")
print("  ✗ unique_creatives_seen")
print("  ✗ days_since_last_conversion")
print("\nFeatures:")
print("  ✓ Business-friendly column names")
print("  ✓ Pre-calculated KPIs and ratios")
print("  ✓ PII-safe (no raw email/phone)")
print("  ✓ Column-level documentation")
print("  ✓ Rolling 90-day window")
print("  ✓ Quality filters applied")
print("  ✓ Validated against source schema")
print("\nNext Steps:")
print("  1. Grant UC permissions to teams")
print("  2. Connect to BI tools (Tableau, Power BI)")
print("  3. Create sample dashboards")
print("  4. Document in data catalog")
print("=" * 80)

---

## **Columns Removed:**

1. ❌ **`unique_creatives_seen`** - Not in Gold table aggregation
2. ❌ **`days_since_last_conversion`** - Not calculated in Gold table (the window function didn't work correctly)

## **Alternative: Add Missing Column to Gold Table**

If you want `days_since_last_conversion`, you'd need to update the Gold Customer 360 notebook to add this calculation. Here's the fix you'd add to the `lifetime_metrics` CTE:

```sql
-- In the gold_customer_360 notebook, update lifetime_metrics CTE:
-- Add this column:
DATEDIFF(
  e.date_est, 
  MAX(CASE 
    WHEN e.source_reference = 'offer-convert' 
    AND e.conversion_type_name != 'Click' 
    THEN e.date_est 
  END) OVER (
    PARTITION BY e.customer_key 
    ORDER BY e.date_est 
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  )
) as days_since_last_conversion
```


