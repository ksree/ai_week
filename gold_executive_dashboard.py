# Databricks notebook source
# MAGIC %md
# MAGIC # Executive Dashboard: Customer 360 Analytics
# MAGIC
# MAGIC **Purpose:** Comprehensive dashboard pulling from all Gold Layer views to surface key business insights.
# MAGIC
# MAGIC **Data Sources (25 Views):**
# MAGIC
# MAGIC | Category | Views |
# MAGIC |----------|-------|
# MAGIC | Customer 360 (9) | customer_360_metrics, customer_segments_summary, customer_daily_kpis, customer_current_state, high_value_customers, at_risk_customers, new_customer_cohort, repeat_customer_analysis, repeat_customer_summary |
# MAGIC | Event Metrics (13) | gold_metrics_hourly_performance, gold_metrics_campaign_performance, gold_metrics_session_engagement, gold_metrics_customer_identity, gold_metrics_conversion_funnel, gold_metrics_device_performance, gold_metrics_geographic_performance, gold_metrics_traffic_source, gold_metrics_temporal_patterns, gold_metrics_session_funnel, gold_metrics_repeat_users_daily, gold_metrics_new_vs_returning_performance, gold_metrics_cohort_retention |
# MAGIC | Campaign (3) | campaign_top_customers, campaign_performance_summary, campaign_daily_performance |
# MAGIC
# MAGIC **Refresh:** Run daily after Gold layer processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

spark.sql("USE CATALOG centraldata_sandbox")
spark.sql("USE SCHEMA test")

# COMMAND ----------

# MAGIC %md
# MAGIC # SECTION 1: EXECUTIVE SUMMARY KPIs
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Today's Snapshot vs Yesterday

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily Executive Summary: Today vs Yesterday
# MAGIC WITH today_metrics AS (
# MAGIC   SELECT * FROM customer_daily_kpis
# MAGIC   WHERE metric_date = (SELECT MAX(metric_date) FROM customer_daily_kpis)
# MAGIC ),
# MAGIC yesterday_metrics AS (
# MAGIC   SELECT * FROM customer_daily_kpis
# MAGIC   WHERE metric_date = (SELECT MAX(metric_date) - 1 FROM customer_daily_kpis)
# MAGIC )
# MAGIC SELECT
# MAGIC   '📊 DAILY SNAPSHOT' as section,
# MAGIC   t.metric_date as report_date,
# MAGIC
# MAGIC   -- Customer Metrics
# MAGIC   t.total_customers,
# MAGIC   t.active_customers,
# MAGIC   ROUND((t.active_customers - y.active_customers) * 100.0 / NULLIF(y.active_customers, 0), 1) as active_customers_change_pct,
# MAGIC   t.new_customers,
# MAGIC   ROUND((t.new_customers - y.new_customers) * 100.0 / NULLIF(y.new_customers, 0), 1) as new_customers_change_pct,
# MAGIC
# MAGIC   -- Revenue Metrics
# MAGIC   t.total_daily_revenue,
# MAGIC   ROUND((t.total_daily_revenue - y.total_daily_revenue) * 100.0 / NULLIF(y.total_daily_revenue, 0), 1) as revenue_change_pct,
# MAGIC   t.avg_revenue_per_customer,
# MAGIC
# MAGIC   -- Conversion Metrics
# MAGIC   t.total_conversions,
# MAGIC   t.avg_conversion_rate_pct,
# MAGIC   ROUND(t.avg_conversion_rate_pct - y.avg_conversion_rate_pct, 2) as conversion_rate_change_pts,
# MAGIC
# MAGIC   -- Engagement
# MAGIC   t.avg_engagement_score,
# MAGIC   t.total_sessions
# MAGIC
# MAGIC FROM today_metrics t
# MAGIC LEFT JOIN yesterday_metrics y ON 1=1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Week-over-Week Trends

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Week-over-Week Trend Analysis
# MAGIC WITH weekly_metrics AS (
# MAGIC   SELECT
# MAGIC     DATE_TRUNC('week', metric_date) as week_start,
# MAGIC     SUM(total_customers) as total_customers,
# MAGIC     SUM(active_customers) as active_customers,
# MAGIC     SUM(new_customers) as new_customers,
# MAGIC     SUM(total_daily_revenue) as total_revenue,
# MAGIC     SUM(total_conversions) as total_conversions,
# MAGIC     SUM(total_sessions) as total_sessions,
# MAGIC     AVG(avg_engagement_score) as avg_engagement_score,
# MAGIC     AVG(avg_conversion_rate_pct) as avg_conversion_rate
# MAGIC   FROM customer_daily_kpis
# MAGIC   WHERE metric_date >= CURRENT_DATE - 28
# MAGIC   GROUP BY DATE_TRUNC('week', metric_date)
# MAGIC )
# MAGIC SELECT
# MAGIC   week_start,
# MAGIC   total_customers,
# MAGIC   active_customers,
# MAGIC   new_customers,
# MAGIC   ROUND(total_revenue, 2) as total_revenue,
# MAGIC   total_conversions,
# MAGIC   total_sessions,
# MAGIC   ROUND(avg_engagement_score, 1) as avg_engagement,
# MAGIC   ROUND(avg_conversion_rate, 2) as avg_conversion_rate,
# MAGIC   -- WoW Changes
# MAGIC   ROUND((total_revenue - LAG(total_revenue) OVER (ORDER BY week_start)) * 100.0 /
# MAGIC         NULLIF(LAG(total_revenue) OVER (ORDER BY week_start), 0), 1) as revenue_wow_change_pct
# MAGIC FROM weekly_metrics
# MAGIC ORDER BY week_start DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # SECTION 2: CUSTOMER INSIGHTS
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Customer Value Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer Value Segment Distribution (Latest Date)
# MAGIC SELECT
# MAGIC   '💰 VALUE SEGMENTS' as section,
# MAGIC   value_segment,
# MAGIC   COUNT(DISTINCT customer_key) as customer_count,
# MAGIC   ROUND(COUNT(DISTINCT customer_key) * 100.0 / SUM(COUNT(DISTINCT customer_key)) OVER (), 1) as pct_of_customers,
# MAGIC   ROUND(SUM(lifetime_revenue), 2) as total_lifetime_revenue,
# MAGIC   ROUND(SUM(lifetime_revenue) * 100.0 / NULLIF(SUM(SUM(lifetime_revenue)) OVER (), 0), 1) as pct_of_revenue,
# MAGIC   ROUND(AVG(estimated_clv), 2) as avg_clv,
# MAGIC   ROUND(AVG(engagement_score), 1) as avg_engagement
# MAGIC FROM customer_current_state
# MAGIC GROUP BY value_segment
# MAGIC ORDER BY
# MAGIC   CASE value_segment
# MAGIC     WHEN 'high_value' THEN 1
# MAGIC     WHEN 'medium_value' THEN 2
# MAGIC     WHEN 'low_value' THEN 3
# MAGIC     ELSE 4
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Customer Lifecycle Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer Lifecycle Stage Distribution
# MAGIC SELECT
# MAGIC   '🔄 LIFECYCLE STAGES' as section,
# MAGIC   lifecycle_stage,
# MAGIC   COUNT(DISTINCT customer_key) as customer_count,
# MAGIC   ROUND(COUNT(DISTINCT customer_key) * 100.0 / SUM(COUNT(DISTINCT customer_key)) OVER (), 1) as pct_of_total,
# MAGIC   ROUND(AVG(days_as_customer), 0) as avg_tenure_days,
# MAGIC   ROUND(SUM(daily_revenue), 2) as total_daily_revenue,
# MAGIC   ROUND(AVG(churn_risk_score_pct), 1) as avg_churn_risk,
# MAGIC   ROUND(AVG(engagement_score), 1) as avg_engagement
# MAGIC FROM customer_current_state
# MAGIC GROUP BY lifecycle_stage
# MAGIC ORDER BY
# MAGIC   CASE lifecycle_stage
# MAGIC     WHEN 'new' THEN 1
# MAGIC     WHEN 'active' THEN 2
# MAGIC     WHEN 'at_risk' THEN 3
# MAGIC     WHEN 'churned' THEN 4
# MAGIC     ELSE 5
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Top 20 High-Value Customers at Risk

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High-Value Customers at Risk of Churning (Action Required!)
# MAGIC SELECT
# MAGIC   '⚠️ HIGH-VALUE AT RISK' as alert,
# MAGIC   customer_key,
# MAGIC   lifetime_revenue,
# MAGIC   churn_risk_score_pct,
# MAGIC   churn_risk_tier,
# MAGIC   days_since_last_activity,
# MAGIC   engagement_score,
# MAGIC   sessions as recent_sessions,
# MAGIC   value_segment,
# MAGIC   country,
# MAGIC   state
# MAGIC FROM at_risk_customers
# MAGIC WHERE value_segment = 'high_value'
# MAGIC ORDER BY lifetime_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Repeat Customer Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Repeat Customer Summary: Visit Frequency Distribution
# MAGIC SELECT
# MAGIC   '🔁 REPEAT CUSTOMERS' as section,
# MAGIC   visit_frequency_segment,
# MAGIC   recency_tier,
# MAGIC   customer_count,
# MAGIC   ROUND(avg_visit_days, 1) as avg_visit_days,
# MAGIC   ROUND(avg_lifetime_sessions, 1) as avg_sessions,
# MAGIC   ROUND(total_revenue, 2) as total_revenue,
# MAGIC   ROUND(avg_revenue_per_customer, 2) as avg_revenue_per_customer,
# MAGIC   ROUND(pct_of_customers, 1) as pct_of_customers,
# MAGIC   ROUND(pct_of_revenue, 1) as pct_of_revenue
# MAGIC FROM repeat_customer_summary
# MAGIC ORDER BY
# MAGIC   CASE visit_frequency_segment
# MAGIC     WHEN 'power_user' THEN 1
# MAGIC     WHEN 'highly_regular' THEN 2
# MAGIC     WHEN 'regular' THEN 3
# MAGIC     WHEN 'occasional' THEN 4
# MAGIC     ELSE 5
# MAGIC   END,
# MAGIC   CASE recency_tier
# MAGIC     WHEN 'active' THEN 1
# MAGIC     WHEN 'recent' THEN 2
# MAGIC     WHEN 'lapsed' THEN 3
# MAGIC     ELSE 4
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 New vs Returning Customer Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- New vs Returning Customer Performance (Last 7 Days)
# MAGIC SELECT
# MAGIC   '👥 NEW vs RETURNING' as section,
# MAGIC   customer_type,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(events) as total_events,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(events_per_customer), 1) as avg_events_per_customer,
# MAGIC   ROUND(AVG(revenue_per_customer), 2) as avg_revenue_per_customer,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate,
# MAGIC   -- Revenue contribution
# MAGIC   ROUND(SUM(revenue) * 100.0 / NULLIF(SUM(SUM(revenue)) OVER (), 0), 1) as pct_of_revenue
# MAGIC FROM gold_metrics_new_vs_returning_performance
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY customer_type;

# COMMAND ----------

# MAGIC %md
# MAGIC # SECTION 3: CONVERSION & FUNNEL ANALYSIS
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Daily Conversion Funnel

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Conversion Funnel Analysis (Last 7 Days)
# MAGIC SELECT
# MAGIC   '🔄 CONVERSION FUNNEL' as section,
# MAGIC   date_est,
# MAGIC   total_customers,
# MAGIC   viewers,
# MAGIC   clickers,
# MAGIC   converters,
# MAGIC   buyers,
# MAGIC   ROUND(view_to_click_rate, 2) as view_to_click_pct,
# MAGIC   ROUND(click_to_convert_rate, 2) as click_to_convert_pct,
# MAGIC   ROUND(convert_to_buy_rate, 2) as convert_to_buy_pct,
# MAGIC   ROUND(overall_conversion_rate, 2) as overall_funnel_pct,
# MAGIC   ROUND(total_revenue, 2) as revenue,
# MAGIC   ROUND(avg_revenue_per_buyer, 2) as avg_order_value
# MAGIC FROM gold_metrics_conversion_funnel
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC ORDER BY date_est DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Session Funnel Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Session-Level Funnel (Last 7 Days)
# MAGIC SELECT
# MAGIC   '📈 SESSION FUNNEL' as section,
# MAGIC   date_est,
# MAGIC   total_sessions,
# MAGIC   sessions_with_view,
# MAGIC   sessions_with_click,
# MAGIC   sessions_with_conversion,
# MAGIC   sessions_with_transaction,
# MAGIC   ROUND(view_to_click_rate, 2) as view_to_click_rate,
# MAGIC   ROUND(click_to_convert_rate, 2) as click_to_convert_rate,
# MAGIC   ROUND(overall_transaction_rate, 2) as overall_transaction_rate,
# MAGIC   ROUND(total_revenue, 2) as total_revenue
# MAGIC FROM gold_metrics_session_funnel
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC ORDER BY date_est DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # SECTION 4: CAMPAIGN PERFORMANCE
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Top 20 Campaigns by Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 20 Campaigns by Revenue (Last 30 Days)
# MAGIC SELECT
# MAGIC   '🎯 TOP CAMPAIGNS' as section,
# MAGIC   campaign_name,
# MAGIC   advertiser_name,
# MAGIC   total_customers,
# MAGIC   ROUND(total_revenue, 2) as total_revenue,
# MAGIC   ROUND(revenue_per_customer, 2) as revenue_per_customer,
# MAGIC   total_conversions,
# MAGIC   ROUND(overall_ctr, 2) as ctr_pct,
# MAGIC   ROUND(overall_conversion_rate, 2) as conversion_rate_pct,
# MAGIC   ROUND(avg_order_value, 2) as avg_order_value
# MAGIC FROM campaign_performance_summary
# MAGIC WHERE total_revenue > 0
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Campaign Performance by Vertical

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Campaign Performance by Vertical (Last 7 Days)
# MAGIC SELECT
# MAGIC   '📊 BY VERTICAL' as section,
# MAGIC   campaign_vertical as vertical,
# MAGIC   COUNT(DISTINCT campaign_id) as campaigns,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(views) as total_views,
# MAGIC   SUM(clicks) as total_clicks,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate
# MAGIC FROM gold_metrics_campaign_performance
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC   AND campaign_vertical IS NOT NULL
# MAGIC GROUP BY campaign_vertical
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # SECTION 5: CHANNEL & SOURCE ANALYSIS
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Device Performance Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Device Performance (Last 7 Days)
# MAGIC SELECT
# MAGIC   '📱 DEVICE PERFORMANCE' as section,
# MAGIC   device_type,
# MAGIC   SUM(events) as total_events,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(SUM(revenue) * 100.0 / NULLIF(SUM(SUM(revenue)) OVER (), 0), 1) as pct_of_revenue,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate,
# MAGIC   ROUND(AVG(avg_order_value), 2) as avg_order_value
# MAGIC FROM gold_metrics_device_performance
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY device_type
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Top Traffic Sources

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 15 Traffic Sources by Revenue (Last 7 Days)
# MAGIC SELECT
# MAGIC   '🔗 TRAFFIC SOURCES' as section,
# MAGIC   partner_name,
# MAGIC   traffic_partner_type,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate,
# MAGIC   ROUND(AVG(revenue_per_customer), 2) as revenue_per_customer
# MAGIC FROM gold_metrics_traffic_source
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY partner_name, traffic_partner_type
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Geographic Performance - Top States

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 15 States by Revenue (Last 7 Days, US Only)
# MAGIC SELECT
# MAGIC   '🌎 TOP STATES' as section,
# MAGIC   state,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(SUM(revenue) * 100.0 / NULLIF(SUM(SUM(revenue)) OVER (), 0), 1) as pct_of_revenue,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate,
# MAGIC   ROUND(AVG(revenue_per_customer), 2) as revenue_per_customer
# MAGIC FROM gold_metrics_geographic_performance
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC   AND country = 'US'
# MAGIC   AND state IS NOT NULL
# MAGIC GROUP BY state
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC # SECTION 6: ENGAGEMENT & TIMING PATTERNS
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Session Engagement Trends

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Session Engagement Metrics (Last 7 Days)
# MAGIC SELECT
# MAGIC   '⚡ SESSION ENGAGEMENT' as section,
# MAGIC   date_est,
# MAGIC   total_sessions,
# MAGIC   unique_customers,
# MAGIC   ROUND(avg_session_duration_sec / 60, 2) as avg_duration_min,
# MAGIC   ROUND(avg_events_per_session, 1) as avg_events_per_session,
# MAGIC   ROUND(avg_engagement_score, 1) as avg_engagement_score,
# MAGIC   ROUND(bounce_rate_pct, 2) as bounce_rate_pct,
# MAGIC   high_engagement_sessions,
# MAGIC   sessions_with_conversion,
# MAGIC   ROUND(session_conversion_rate_pct, 2) as session_conversion_rate,
# MAGIC   ROUND(total_revenue, 2) as total_revenue
# MAGIC FROM gold_metrics_session_engagement
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC ORDER BY date_est DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Day of Week Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Day of Week Performance (Last 30 Days)
# MAGIC SELECT
# MAGIC   '📅 DAY OF WEEK' as section,
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
# MAGIC ## 6.3 Hourly Performance Heatmap Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hourly Performance (Last 7 Days) - For Heatmap Visualization
# MAGIC SELECT
# MAGIC   '⏰ HOURLY PERFORMANCE' as section,
# MAGIC   hour_est,
# MAGIC   is_business_hours,
# MAGIC   SUM(events) as total_events,
# MAGIC   SUM(unique_customers) as total_customers,
# MAGIC   SUM(conversions) as total_conversions,
# MAGIC   ROUND(SUM(revenue), 2) as total_revenue,
# MAGIC   ROUND(AVG(ctr_pct), 2) as avg_ctr,
# MAGIC   ROUND(AVG(conversion_rate_pct), 2) as avg_conversion_rate
# MAGIC FROM gold_metrics_hourly_performance
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC GROUP BY hour_est, is_business_hours
# MAGIC ORDER BY hour_est;

# COMMAND ----------

# MAGIC %md
# MAGIC # SECTION 7: CUSTOMER IDENTITY & DATA QUALITY
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Identity Resolution Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identity Resolution Effectiveness (Last 7 Days)
# MAGIC SELECT
# MAGIC   '🔐 IDENTITY RESOLUTION' as section,
# MAGIC   date_est,
# MAGIC   total_events,
# MAGIC   unique_customers,
# MAGIC   ROUND(customer_identification_rate_pct, 2) as customer_id_rate_pct,
# MAGIC   ROUND(event_identification_rate_pct, 2) as event_id_rate_pct,
# MAGIC   -- Identity Source Breakdown
# MAGIC   email_sha256_customers,
# MAGIC   email_md5_customers,
# MAGIC   email_customers,
# MAGIC   phone_customers,
# MAGIC   anonymous_customers,
# MAGIC   -- Revenue by Identity
# MAGIC   ROUND(identified_revenue, 2) as identified_revenue,
# MAGIC   ROUND(anonymous_revenue, 2) as anonymous_revenue,
# MAGIC   ROUND(identified_revenue * 100.0 / NULLIF(identified_revenue + anonymous_revenue, 0), 1) as identified_revenue_pct
# MAGIC FROM gold_metrics_customer_identity
# MAGIC WHERE date_est >= CURRENT_DATE - 7
# MAGIC ORDER BY date_est DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Data Quality Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data Quality Metrics from Customer 360
# MAGIC SELECT
# MAGIC   '📊 DATA QUALITY' as section,
# MAGIC   metric_date,
# MAGIC   COUNT(DISTINCT customer_key) as total_customers,
# MAGIC   ROUND(AVG(profile_completeness_pct), 1) as avg_profile_completeness,
# MAGIC   ROUND(AVG(data_quality_score_pct), 1) as avg_data_quality_score,
# MAGIC   COUNT(DISTINCT CASE WHEN profile_completeness_pct >= 80 THEN customer_key END) as high_quality_profiles,
# MAGIC   COUNT(DISTINCT CASE WHEN profile_completeness_pct < 50 THEN customer_key END) as low_quality_profiles
# MAGIC FROM customer_360_metrics
# MAGIC WHERE metric_date >= CURRENT_DATE - 7
# MAGIC GROUP BY metric_date
# MAGIC ORDER BY metric_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # SECTION 8: COHORT & RETENTION ANALYSIS
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 Monthly Cohort Retention

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cohort Retention Matrix (Last 6 Months)
# MAGIC SELECT
# MAGIC   '📈 COHORT RETENTION' as section,
# MAGIC   DATE_FORMAT(cohort_month, 'yyyy-MM') as cohort,
# MAGIC   cohort_size,
# MAGIC   month_number,
# MAGIC   active_customers,
# MAGIC   ROUND(retention_rate_pct, 1) as retention_rate_pct
# MAGIC FROM gold_metrics_cohort_retention
# MAGIC WHERE cohort_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 6 MONTHS)
# MAGIC   AND month_number <= 6
# MAGIC ORDER BY cohort_month DESC, month_number;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Cohort Retention Pivot View

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cohort Retention Pivot (For visualization)
# MAGIC SELECT
# MAGIC   DATE_FORMAT(cohort_month, 'yyyy-MM') as cohort,
# MAGIC   cohort_size,
# MAGIC   MAX(CASE WHEN month_number = 0 THEN retention_rate_pct END) as month_0,
# MAGIC   MAX(CASE WHEN month_number = 1 THEN retention_rate_pct END) as month_1,
# MAGIC   MAX(CASE WHEN month_number = 2 THEN retention_rate_pct END) as month_2,
# MAGIC   MAX(CASE WHEN month_number = 3 THEN retention_rate_pct END) as month_3,
# MAGIC   MAX(CASE WHEN month_number = 4 THEN retention_rate_pct END) as month_4,
# MAGIC   MAX(CASE WHEN month_number = 5 THEN retention_rate_pct END) as month_5,
# MAGIC   MAX(CASE WHEN month_number = 6 THEN retention_rate_pct END) as month_6
# MAGIC FROM gold_metrics_cohort_retention
# MAGIC WHERE cohort_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 6 MONTHS)
# MAGIC GROUP BY cohort_month, cohort_size
# MAGIC ORDER BY cohort_month DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # SECTION 9: KEY INSIGHTS & ALERTS
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 Key Business Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate Key Insights Summary
# MAGIC WITH latest_kpis AS (
# MAGIC   SELECT * FROM customer_daily_kpis
# MAGIC   WHERE metric_date = (SELECT MAX(metric_date) FROM customer_daily_kpis)
# MAGIC ),
# MAGIC repeat_stats AS (
# MAGIC   SELECT
# MAGIC     SUM(CASE WHEN visit_frequency_segment != 'one_time' THEN customer_count ELSE 0 END) as repeat_customers,
# MAGIC     SUM(customer_count) as total_customers,
# MAGIC     SUM(CASE WHEN visit_frequency_segment IN ('power_user', 'highly_regular') THEN total_revenue ELSE 0 END) as power_user_revenue,
# MAGIC     SUM(total_revenue) as total_revenue
# MAGIC   FROM repeat_customer_summary
# MAGIC ),
# MAGIC device_winner AS (
# MAGIC   SELECT device_type, SUM(revenue) as revenue
# MAGIC   FROM gold_metrics_device_performance
# MAGIC   WHERE date_est >= CURRENT_DATE - 7
# MAGIC   GROUP BY device_type
# MAGIC   ORDER BY revenue DESC
# MAGIC   LIMIT 1
# MAGIC ),
# MAGIC top_campaign AS (
# MAGIC   SELECT campaign_name, total_revenue
# MAGIC   FROM campaign_performance_summary
# MAGIC   ORDER BY total_revenue DESC
# MAGIC   LIMIT 1
# MAGIC ),
# MAGIC churn_risk AS (
# MAGIC   SELECT
# MAGIC     COUNT(DISTINCT customer_key) as at_risk_count,
# MAGIC     SUM(lifetime_revenue) as at_risk_revenue
# MAGIC   FROM at_risk_customers
# MAGIC   WHERE churn_risk_tier = 'High Risk'
# MAGIC   AND value_segment = 'high_value'
# MAGIC )
# MAGIC SELECT
# MAGIC   '🔍 KEY INSIGHTS' as section,
# MAGIC
# MAGIC   -- Customer Insights
# MAGIC   CONCAT('Active Customers Today: ', k.active_customers) as insight_1,
# MAGIC   CONCAT('Repeat Customer Rate: ', ROUND(r.repeat_customers * 100.0 / NULLIF(r.total_customers, 0), 1), '%') as insight_2,
# MAGIC   CONCAT('Power Users Drive: ', ROUND(r.power_user_revenue * 100.0 / NULLIF(r.total_revenue, 0), 1), '% of Revenue') as insight_3,
# MAGIC
# MAGIC   -- Performance Insights
# MAGIC   CONCAT('Top Device: ', d.device_type) as insight_4,
# MAGIC   CONCAT('Top Campaign: ', c.campaign_name, ' ($', ROUND(c.total_revenue, 0), ')') as insight_5,
# MAGIC
# MAGIC   -- Risk Alert
# MAGIC   CONCAT('⚠️ ', cr.at_risk_count, ' High-Value Customers at Risk ($', ROUND(cr.at_risk_revenue, 0), ' lifetime value)') as alert_1
# MAGIC
# MAGIC FROM latest_kpis k
# MAGIC CROSS JOIN repeat_stats r
# MAGIC CROSS JOIN device_winner d
# MAGIC CROSS JOIN top_campaign c
# MAGIC CROSS JOIN churn_risk cr;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 Anomaly Detection (Day-over-Day Changes > 20%)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Anomaly Detection: Significant Day-over-Day Changes
# MAGIC WITH daily_changes AS (
# MAGIC   SELECT
# MAGIC     metric_date,
# MAGIC     total_daily_revenue,
# MAGIC     LAG(total_daily_revenue) OVER (ORDER BY metric_date) as prev_revenue,
# MAGIC     total_conversions,
# MAGIC     LAG(total_conversions) OVER (ORDER BY metric_date) as prev_conversions,
# MAGIC     active_customers,
# MAGIC     LAG(active_customers) OVER (ORDER BY metric_date) as prev_customers
# MAGIC   FROM customer_daily_kpis
# MAGIC   WHERE metric_date >= CURRENT_DATE - 14
# MAGIC )
# MAGIC SELECT
# MAGIC   '⚡ ANOMALIES' as section,
# MAGIC   metric_date,
# MAGIC   CASE
# MAGIC     WHEN ABS((total_daily_revenue - prev_revenue) * 100.0 / NULLIF(prev_revenue, 0)) > 20
# MAGIC     THEN CONCAT('Revenue: ', ROUND((total_daily_revenue - prev_revenue) * 100.0 / NULLIF(prev_revenue, 0), 1), '% change')
# MAGIC   END as revenue_anomaly,
# MAGIC   CASE
# MAGIC     WHEN ABS((total_conversions - prev_conversions) * 100.0 / NULLIF(prev_conversions, 0)) > 20
# MAGIC     THEN CONCAT('Conversions: ', ROUND((total_conversions - prev_conversions) * 100.0 / NULLIF(prev_conversions, 0), 1), '% change')
# MAGIC   END as conversion_anomaly,
# MAGIC   CASE
# MAGIC     WHEN ABS((active_customers - prev_customers) * 100.0 / NULLIF(prev_customers, 0)) > 20
# MAGIC     THEN CONCAT('Active Customers: ', ROUND((active_customers - prev_customers) * 100.0 / NULLIF(prev_customers, 0), 1), '% change')
# MAGIC   END as customer_anomaly
# MAGIC FROM daily_changes
# MAGIC WHERE
# MAGIC   ABS((total_daily_revenue - prev_revenue) * 100.0 / NULLIF(prev_revenue, 0)) > 20
# MAGIC   OR ABS((total_conversions - prev_conversions) * 100.0 / NULLIF(prev_conversions, 0)) > 20
# MAGIC   OR ABS((active_customers - prev_customers) * 100.0 / NULLIF(prev_customers, 0)) > 20
# MAGIC ORDER BY metric_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # SECTION 10: DASHBOARD SUMMARY
# MAGIC ---

# COMMAND ----------

print("=" * 100)
print("EXECUTIVE DASHBOARD - CUSTOMER 360 ANALYTICS")
print("=" * 100)
print("\n📊 DASHBOARD SECTIONS:")
print("-" * 50)
print("1. Executive Summary KPIs")
print("   - Today vs Yesterday comparison")
print("   - Week-over-Week trends")
print("")
print("2. Customer Insights")
print("   - Value segment distribution")
print("   - Lifecycle stage breakdown")
print("   - High-value customers at risk")
print("   - Repeat customer analysis")
print("   - New vs returning performance")
print("")
print("3. Conversion & Funnel Analysis")
print("   - Daily conversion funnel")
print("   - Session funnel metrics")
print("")
print("4. Campaign Performance")
print("   - Top 20 campaigns by revenue")
print("   - Performance by vertical")
print("")
print("5. Channel & Source Analysis")
print("   - Device performance comparison")
print("   - Top traffic sources")
print("   - Geographic performance (top states)")
print("")
print("6. Engagement & Timing Patterns")
print("   - Session engagement trends")
print("   - Day of week performance")
print("   - Hourly performance heatmap")
print("")
print("7. Customer Identity & Data Quality")
print("   - Identity resolution metrics")
print("   - Data quality summary")
print("")
print("8. Cohort & Retention Analysis")
print("   - Monthly cohort retention")
print("   - Retention pivot view")
print("")
print("9. Key Insights & Alerts")
print("   - Auto-generated business insights")
print("   - Anomaly detection (>20% changes)")
print("")
print("=" * 100)
print("\n🔗 DATA SOURCES USED:")
print("-" * 50)
print("Customer 360 Views (9):")
print("  • customer_360_metrics")
print("  • customer_segments_summary")
print("  • customer_daily_kpis")
print("  • customer_current_state")
print("  • high_value_customers")
print("  • at_risk_customers")
print("  • new_customer_cohort")
print("  • repeat_customer_analysis")
print("  • repeat_customer_summary")
print("")
print("Event Metrics Views (13):")
print("  • gold_metrics_hourly_performance")
print("  • gold_metrics_campaign_performance")
print("  • gold_metrics_session_engagement")
print("  • gold_metrics_customer_identity")
print("  • gold_metrics_conversion_funnel")
print("  • gold_metrics_device_performance")
print("  • gold_metrics_geographic_performance")
print("  • gold_metrics_traffic_source")
print("  • gold_metrics_temporal_patterns")
print("  • gold_metrics_session_funnel")
print("  • gold_metrics_repeat_users_daily")
print("  • gold_metrics_new_vs_returning_performance")
print("  • gold_metrics_cohort_retention")
print("")
print("Campaign Views (3):")
print("  • campaign_top_customers")
print("  • campaign_performance_summary")
print("  • campaign_daily_performance")
print("")
print("=" * 100)
print("\n💡 INTERESTING FACTS THIS DASHBOARD REVEALS:")
print("-" * 50)
print("1. 💰 Revenue concentration: See what % of revenue comes from power users")
print("2. ⚠️  At-risk value: Total $ at risk from high-value churning customers")
print("3. 📱 Device winner: Which device drives the most revenue")
print("4. 🔄 Repeat rate: What % of customers return for multiple visits")
print("5. 📈 Funnel leakage: Where customers drop off (view→click→convert→buy)")
print("6. ⏰ Peak hours: Best hours/days for engagement and conversions")
print("7. 🌎 Geographic hotspots: Top performing states")
print("8. 🔐 Identity rates: How many customers are identified vs anonymous")
print("9. 📊 Cohort health: How well each monthly cohort retains over time")
print("10. ⚡ Anomalies: Automatic detection of unusual patterns")
print("=" * 100)
