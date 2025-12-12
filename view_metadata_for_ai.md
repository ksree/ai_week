# Customer 360 Analytics - View Metadata for AI Dashboard Generation

**Catalog:** `centraldata_sandbox`
**Schema:** `test`
**Total Views:** 25

---

## CATEGORY 1: CUSTOMER 360 VIEWS (9 Views)

### 1. customer_360_metrics
**Description:** Daily customer behavior metrics with segmentation, engagement scores, and lifetime value. This is the main customer-level metrics view providing comprehensive behavioral and transactional data for each customer on each day.

**Business Use Cases:**
- Customer segmentation analysis
- Engagement scoring and tracking
- Lifetime value estimation
- Churn risk assessment
- Cross-device behavior analysis

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| customer_key | STRING | Unique customer identifier (hashed for privacy) |
| fluent_id | STRING | Fluent ID for customer |
| metric_date | DATE | Date of behavior measurement (EST timezone) |
| is_identified_customer | BOOLEAN | True if customer has profile ID, email, or phone |
| days_as_customer | INT | Number of days since first customer interaction |
| country | STRING | Customer's country |
| state | STRING | Customer's state |
| city | STRING | Customer's city |
| gender | STRING | Customer's gender |
| is_active_today | BOOLEAN | Whether customer was active on this date |
| days_since_last_activity | INT | Days since last activity |
| sessions | BIGINT | Total sessions on this date |
| events | BIGINT | Total events on this date |
| avg_session_duration_min | DOUBLE | Average session duration in minutes |
| avg_events_per_session | DOUBLE | Average events per session |
| views | BIGINT | Total offer views |
| clicks | BIGINT | Total clicks |
| ctr_pct | DOUBLE | Click-through rate percentage |
| primary_position_views | BIGINT | P1 position views |
| p1_view_rate_pct | DOUBLE | P1 view rate percentage |
| campaigns_viewed | INT | Unique campaigns viewed |
| advertisers_interacted | INT | Unique advertisers interacted with |
| verticals_explored | INT | Unique verticals explored |
| content_diversity_score_pct | DOUBLE | Content diversity score (0-100) |
| avg_campaign_depth | DOUBLE | Average campaign depth per session |
| engagement_score | DOUBLE | Engagement score (0-100) |
| max_engagement_score | DOUBLE | Maximum engagement score achieved |
| high_engagement_sessions | BIGINT | Sessions with high engagement |
| conversions | BIGINT | Total conversions (sourceReference=offer-convert, excluding Click type) |
| transactions | BIGINT | Total transactions |
| conversion_rate_pct | DOUBLE | Conversion rate percentage |
| transaction_rate_pct | DOUBLE | Transaction rate percentage |
| daily_revenue | DOUBLE | Revenue generated on this date |
| avg_revenue_per_session | DOUBLE | Average revenue per session |
| transaction_value | DOUBLE | Total transaction value |
| avg_transaction_value | DOUBLE | Average transaction value |
| lifetime_revenue | DOUBLE | Cumulative lifetime revenue |
| lifetime_transactions | BIGINT | Cumulative lifetime transactions |
| estimated_clv | DOUBLE | Estimated customer lifetime value |
| device_types_used | INT | Number of device types used |
| primary_device | STRING | Primary device type |
| mobile_pct | DOUBLE | Percentage of mobile sessions |
| desktop_pct | DOUBLE | Percentage of desktop sessions |
| tablet_pct | DOUBLE | Percentage of tablet sessions |
| is_cross_device_user | BOOLEAN | Whether user uses multiple devices |
| business_hours_sessions | BIGINT | Sessions during business hours |
| after_hours_sessions | BIGINT | Sessions outside business hours |
| weekend_sessions | BIGINT | Weekend sessions |
| weekday_sessions | BIGINT | Weekday sessions |
| peak_activity_hour | INT | Most active hour (EST) |
| peak_activity_day | STRING | Most active day of week |
| morning_activity_pct | DOUBLE | Morning activity percentage |
| afternoon_activity_pct | DOUBLE | Afternoon activity percentage |
| evening_activity_pct | DOUBLE | Evening activity percentage |
| night_activity_pct | DOUBLE | Night activity percentage |
| primary_partner | STRING | Primary traffic partner |
| primary_source | STRING | Primary traffic source |
| partners_used | INT | Number of partners used |
| sources_used | INT | Number of sources used |
| campaigns_list | ARRAY | Campaigns interacted (last 30 days) |
| advertisers_list | ARRAY | Advertisers interacted (last 30 days) |
| verticals_list | ARRAY | Verticals preferred (last 30 days) |
| value_segment | STRING | Customer value tier: high_value, medium_value, low_value, no_value |
| engagement_segment | STRING | Engagement tier: highly_engaged, moderately_engaged, low_engaged, minimal |
| propensity_segment | STRING | Purchase propensity segment |
| lifecycle_stage | STRING | Customer lifecycle: new, active, at_risk, churned |
| frequency_tier | STRING | Session frequency tier |
| churn_risk_score_pct | DOUBLE | Churn risk score (0-100, higher = more likely to churn) |
| churn_risk_tier | STRING | Churn risk category: High Risk, Medium Risk, Low Risk |
| profile_completeness_pct | DOUBLE | Profile completeness percentage |
| data_quality_score_pct | DOUBLE | Data quality score percentage |
| last_updated | TIMESTAMP | Last update timestamp |

---

### 2. customer_segments_summary
**Description:** Daily aggregated metrics by customer segment. Pre-aggregated segment-level metrics for dashboards showing performance by value segment, engagement segment, and lifecycle stage.

**Business Use Cases:**
- Segment performance comparison
- Cross-segment trend analysis
- Segment-level KPI tracking

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| metric_date | DATE | Date of metrics |
| value_segment | STRING | Customer value tier |
| engagement_segment | STRING | Engagement tier |
| lifecycle_stage | STRING | Customer lifecycle stage |
| customer_count | BIGINT | Number of customers in segment |
| active_customers | BIGINT | Active customers in segment |
| cross_device_customers | BIGINT | Cross-device users in segment |
| avg_engagement_score | DOUBLE | Average engagement score |
| avg_sessions | DOUBLE | Average sessions per customer |
| avg_session_duration_min | DOUBLE | Average session duration |
| avg_ctr_pct | DOUBLE | Average click-through rate |
| total_conversions | BIGINT | Total conversions |
| avg_conversion_rate_pct | DOUBLE | Average conversion rate |
| total_transactions | BIGINT | Total transactions |
| total_revenue | DOUBLE | Total revenue |
| avg_revenue_per_customer | DOUBLE | Average revenue per customer |
| total_lifetime_revenue | DOUBLE | Total lifetime revenue |
| avg_lifetime_revenue | DOUBLE | Average lifetime revenue |
| avg_estimated_clv | DOUBLE | Average estimated CLV |
| avg_churn_risk_pct | DOUBLE | Average churn risk percentage |
| high_risk_customers | BIGINT | Count of high-risk customers |
| avg_profile_completeness_pct | DOUBLE | Average profile completeness |

---

### 3. customer_daily_kpis
**Description:** Daily high-level customer KPIs for executive reporting. Provides aggregated daily metrics for quick executive-level insights.

**Business Use Cases:**
- Executive dashboard KPIs
- Daily performance monitoring
- Trend analysis
- KPI cards and counters

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| metric_date | DATE | Date of metrics |
| total_customers | BIGINT | Total unique customers |
| active_customers | BIGINT | Active customers on this date |
| new_customers | BIGINT | New customers (lifecycle_stage = 'new') |
| at_risk_customers | BIGINT | At-risk customers |
| churned_customers | BIGINT | Churned customers |
| high_value_customers | BIGINT | High-value segment customers |
| medium_value_customers | BIGINT | Medium-value segment customers |
| low_value_customers | BIGINT | Low-value segment customers |
| avg_engagement_score | DOUBLE | Average engagement score |
| total_sessions | BIGINT | Total sessions |
| total_events | BIGINT | Total events |
| avg_session_duration_min | DOUBLE | Average session duration |
| total_conversions | BIGINT | Total conversions |
| avg_conversion_rate_pct | DOUBLE | Average conversion rate |
| total_transactions | BIGINT | Total transactions |
| total_daily_revenue | DOUBLE | Total daily revenue |
| avg_revenue_per_customer | DOUBLE | Average revenue per customer |
| total_lifetime_revenue | DOUBLE | Total lifetime revenue |
| avg_estimated_clv | DOUBLE | Average estimated CLV |
| cross_device_users | BIGINT | Cross-device users count |
| avg_mobile_pct | DOUBLE | Average mobile usage percentage |
| avg_desktop_pct | DOUBLE | Average desktop usage percentage |
| avg_churn_risk_pct | DOUBLE | Average churn risk percentage |
| high_churn_risk_customers | BIGINT | High churn risk count |
| avg_profile_completeness | DOUBLE | Average profile completeness |
| avg_data_quality_score | DOUBLE | Average data quality score |

---

### 4. customer_current_state
**Description:** Most recent customer metrics and segments (latest snapshot per customer). Provides the current state of each customer based on their most recent activity within the last 7 days.

**Business Use Cases:**
- Operational customer lookup
- Real-time customer segmentation
- Current customer status queries
- CRM integration

**Columns:** Same as customer_360_metrics (latest row per customer)

---

### 5. high_value_customers
**Description:** High-value customers with detailed behavior metrics. Filtered view showing only customers in the high_value segment for VIP treatment and analysis.

**Business Use Cases:**
- VIP customer management
- High-value customer retention
- Premium customer analysis
- Targeted marketing for top customers

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| customer_key | STRING | Customer identifier |
| metric_date | DATE | Date of metrics |
| lifetime_revenue | DOUBLE | Total lifetime revenue |
| estimated_clv | DOUBLE | Estimated customer lifetime value |
| engagement_score | DOUBLE | Engagement score |
| sessions | BIGINT | Number of sessions |
| conversions | BIGINT | Number of conversions |
| daily_revenue | DOUBLE | Daily revenue |
| churn_risk_tier | STRING | Churn risk category |
| lifecycle_stage | STRING | Customer lifecycle stage |
| days_as_customer | INT | Customer tenure in days |
| is_cross_device_user | BOOLEAN | Cross-device flag |
| primary_device | STRING | Primary device type |
| peak_activity_day | STRING | Most active day |
| peak_activity_hour | INT | Most active hour |
| country | STRING | Country |
| state | STRING | State |

---

### 6. at_risk_customers
**Description:** Customers at risk of churning with retention signals. Shows customers with High Risk or Medium Risk churn tier who are also in high_value or medium_value segments.

**Business Use Cases:**
- Churn prevention campaigns
- Retention targeting
- Win-back initiatives
- Customer health monitoring

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| customer_key | STRING | Customer identifier |
| metric_date | DATE | Date of metrics |
| churn_risk_tier | STRING | Churn risk category |
| churn_risk_score_pct | DOUBLE | Churn risk score (0-100) |
| lifecycle_stage | STRING | Customer lifecycle stage |
| days_since_last_activity | INT | Days since last activity |
| lifetime_revenue | DOUBLE | Total lifetime revenue |
| engagement_score | DOUBLE | Engagement score |
| sessions | BIGINT | Number of sessions |
| conversions | BIGINT | Number of conversions |
| value_segment | STRING | Customer value segment |
| frequency_tier | STRING | Visit frequency tier |
| country | STRING | Country |
| state | STRING | State |

---

### 7. new_customer_cohort
**Description:** New customers (first 30 days) with activation metrics. Tracks newly acquired customers and their early behavior patterns.

**Business Use Cases:**
- New customer activation tracking
- Onboarding effectiveness
- Early engagement monitoring
- Cohort analysis for new acquisitions

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| customer_key | STRING | Customer identifier |
| metric_date | DATE | Date of metrics |
| days_as_customer | INT | Days since first interaction |
| engagement_score | DOUBLE | Engagement score |
| sessions | BIGINT | Number of sessions |
| events | BIGINT | Number of events |
| conversions | BIGINT | Number of conversions |
| daily_revenue | DOUBLE | Daily revenue |
| lifetime_revenue | DOUBLE | Lifetime revenue |
| is_cross_device_user | BOOLEAN | Cross-device flag |
| primary_device | STRING | Primary device type |
| propensity_segment | STRING | Purchase propensity segment |
| country | STRING | Country |
| state | STRING | State |
| profile_completeness_pct | DOUBLE | Profile completeness percentage |

---

### 8. repeat_customer_analysis
**Description:** Repeat customer identification with visit frequency metrics. Shows how many times customers visited the network, calculating visit frequency segments and recency tiers.

**Business Use Cases:**
- Repeat purchase analysis
- Customer loyalty measurement
- Visit frequency distribution
- Retention rate calculation

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| customer_key | STRING | Customer identifier |
| customer_value_segment | STRING | Customer value segment |
| lifecycle_stage | STRING | Lifecycle stage |
| country | STRING | Country |
| state | STRING | State |
| total_unique_visit_days | BIGINT | Total unique days with activity |
| lifetime_sessions | BIGINT | Total lifetime sessions |
| lifetime_events | BIGINT | Total lifetime events |
| lifetime_conversions | BIGINT | Total lifetime conversions |
| avg_sessions_per_visit_day | DOUBLE | Average sessions per visit day |
| first_visit_date | DATE | First visit date |
| last_visit_date | DATE | Last visit date |
| customer_lifespan_days | INT | Days between first and last visit |
| days_since_last_visit | INT | Days since last visit |
| avg_days_between_visits | DOUBLE | Average days between visits |
| is_repeat_customer | BOOLEAN | True if visited more than once |
| is_frequent_visitor | BOOLEAN | True if 5+ visit days |
| lifetime_revenue | DOUBLE | Total lifetime revenue |
| lifetime_transactions | BIGINT | Total lifetime transactions |
| avg_revenue_per_visit_day | DOUBLE | Average revenue per visit day |
| visit_frequency_segment | STRING | power_user, highly_regular, regular, occasional, one_time |
| recency_tier | STRING | active, recent, lapsed, dormant |

---

### 9. repeat_customer_summary
**Description:** Summary statistics comparing repeat vs one-time customers. Aggregated view showing distribution of customers by visit frequency segment and recency tier.

**Business Use Cases:**
- Repeat customer benchmarking
- Loyalty program analysis
- Revenue concentration analysis
- Customer frequency distribution

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| visit_frequency_segment | STRING | power_user, highly_regular, regular, occasional, one_time |
| recency_tier | STRING | active, recent, lapsed, dormant |
| customer_count | BIGINT | Number of customers |
| avg_visit_days | DOUBLE | Average number of visit days |
| avg_lifetime_sessions | DOUBLE | Average lifetime sessions |
| avg_days_between_visits | DOUBLE | Average days between visits |
| total_revenue | DOUBLE | Total revenue for segment |
| avg_revenue_per_customer | DOUBLE | Average revenue per customer |
| total_conversions | BIGINT | Total conversions |
| pct_of_customers | DOUBLE | Percentage of total customers |
| pct_of_revenue | DOUBLE | Percentage of total revenue |

---

## CATEGORY 2: EVENT METRICS VIEWS (13 Views)

### 10. gold_metrics_hourly_performance
**Description:** Hourly performance metrics for time-of-day optimization. Shows event volume, conversions, and revenue broken down by hour of day.

**Business Use Cases:**
- Peak hour identification
- Time-of-day targeting optimization
- Real-time performance monitoring
- Scheduling optimization

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| hour_est | INT | Hour of day (0-23, EST) |
| is_business_hours | BOOLEAN | True if 9am-5pm EST |
| events | BIGINT | Total events |
| unique_customers | BIGINT | Unique customers |
| unique_sessions | BIGINT | Unique sessions |
| views | BIGINT | Total views |
| clicks | BIGINT | Total clicks |
| conversions | BIGINT | Total conversions |
| revenue | DOUBLE | Total revenue |
| ctr_pct | DOUBLE | Click-through rate percentage |
| conversion_rate_pct | DOUBLE | Conversion rate percentage |

---

### 11. gold_metrics_campaign_performance
**Description:** Campaign-level performance metrics for campaign optimization and advertiser reporting. Daily metrics for each campaign including views, clicks, conversions, and revenue.

**Business Use Cases:**
- Campaign performance comparison
- Advertiser reporting
- Campaign optimization
- ROI analysis by campaign

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| campaign_id | STRING | Campaign identifier |
| campaign_name | STRING | Campaign name |
| advertiser_id | STRING | Advertiser identifier |
| advertiser_name | STRING | Advertiser name |
| campaign_type | STRING | Campaign type |
| campaign_vertical | STRING | Campaign vertical/category |
| events | BIGINT | Total events |
| unique_customers | BIGINT | Unique customers |
| unique_sessions | BIGINT | Unique sessions |
| views | BIGINT | Total views |
| clicks | BIGINT | Total clicks |
| p1_views | BIGINT | Primary position views |
| conversions | BIGINT | Total conversions |
| revenue | DOUBLE | Total revenue |
| avg_order_value | DOUBLE | Average order value |
| ctr_pct | DOUBLE | Click-through rate percentage |
| conversion_rate_pct | DOUBLE | Conversion rate percentage |
| p1_rate_pct | DOUBLE | P1 view rate percentage |
| revenue_per_customer | DOUBLE | Revenue per customer |

---

### 12. gold_metrics_session_engagement
**Description:** Session-level engagement metrics for behavior analysis. Daily aggregated session quality metrics including duration, depth, engagement scores, and bounce rates.

**Business Use Cases:**
- Session quality analysis
- Bounce rate monitoring
- Engagement trend tracking
- User experience optimization

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| total_sessions | BIGINT | Total sessions |
| unique_customers | BIGINT | Unique customers |
| identified_sessions | BIGINT | Identified sessions |
| avg_session_duration_sec | DOUBLE | Average session duration (seconds) |
| avg_events_per_session | DOUBLE | Average events per session |
| avg_session_depth | DOUBLE | Average session depth |
| avg_engagement_score | DOUBLE | Average engagement score |
| bounce_sessions | BIGINT | Single-event sessions |
| bounce_rate_pct | DOUBLE | Bounce rate percentage |
| sessions_with_conversion | BIGINT | Sessions with conversion |
| total_conversions | BIGINT | Total conversions |
| session_conversion_rate_pct | DOUBLE | Session conversion rate |
| total_revenue | DOUBLE | Total revenue |
| avg_revenue_per_converting_session | DOUBLE | Average revenue per converting session |
| sessions_with_device_switch | BIGINT | Sessions with device switch |
| device_switch_rate_pct | DOUBLE | Device switch rate percentage |
| high_engagement_sessions | BIGINT | Sessions with score >= 75 |
| medium_engagement_sessions | BIGINT | Sessions with score 50-74 |
| low_engagement_sessions | BIGINT | Sessions with score 25-49 |
| very_low_engagement_sessions | BIGINT | Sessions with score < 25 |

---

### 13. gold_metrics_customer_identity
**Description:** Identity resolution effectiveness metrics. Tracks how customers are identified across different identity sources (email, phone, anonymous).

**Business Use Cases:**
- Identity resolution monitoring
- Data quality tracking
- Anonymous vs identified analysis
- Identity source effectiveness

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| total_events | BIGINT | Total events |
| unique_customers | BIGINT | Unique customers |
| email_sha256_customers | BIGINT | Customers identified by email SHA256 |
| email_md5_customers | BIGINT | Customers identified by email MD5 |
| email_customers | BIGINT | Customers identified by email |
| phone_customers | BIGINT | Customers identified by phone |
| telephone_customers | BIGINT | Customers identified by telephone |
| anonymous_customers | BIGINT | Anonymous customers (session_id only) |
| email_sha256_events | BIGINT | Events from email SHA256 customers |
| email_md5_events | BIGINT | Events from email MD5 customers |
| email_events | BIGINT | Events from email customers |
| phone_events | BIGINT | Events from phone customers |
| telephone_events | BIGINT | Events from telephone customers |
| anonymous_events | BIGINT | Events from anonymous customers |
| customer_identification_rate_pct | DOUBLE | Percentage of identified customers |
| event_identification_rate_pct | DOUBLE | Percentage of identified events |
| identified_conversions | BIGINT | Conversions from identified customers |
| anonymous_conversions | BIGINT | Conversions from anonymous customers |
| identified_revenue | DOUBLE | Revenue from identified customers |
| anonymous_revenue | DOUBLE | Revenue from anonymous customers |

---

### 14. gold_metrics_conversion_funnel
**Description:** Daily conversion funnel metrics showing customer progression from view to click to convert to buy.

**Business Use Cases:**
- Funnel analysis
- Conversion optimization
- Drop-off identification
- Funnel stage comparison

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| total_customers | BIGINT | Total unique customers |
| viewers | BIGINT | Customers who viewed |
| clickers | BIGINT | Customers who clicked |
| converters | BIGINT | Customers who converted |
| buyers | BIGINT | Customers who purchased |
| view_events | BIGINT | Total view events |
| click_events | BIGINT | Total click events |
| conversion_events | BIGINT | Total conversion events |
| view_to_click_rate | DOUBLE | View to click rate percentage |
| click_to_convert_rate | DOUBLE | Click to convert rate percentage |
| convert_to_buy_rate | DOUBLE | Convert to buy rate percentage |
| overall_conversion_rate | DOUBLE | Overall funnel conversion rate |
| total_revenue | DOUBLE | Total revenue |
| avg_revenue_per_buyer | DOUBLE | Average revenue per buyer |

---

### 15. gold_metrics_device_performance
**Description:** Device-type performance metrics for device optimization.

**Business Use Cases:**
- Device optimization
- Mobile vs desktop analysis
- Cross-device strategy
- Device-specific targeting

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| device_type | STRING | Device type (mobile, desktop, tablet) |
| is_mobile | BOOLEAN | Mobile device flag |
| is_tablet | BOOLEAN | Tablet device flag |
| is_desktop | BOOLEAN | Desktop device flag |
| events | BIGINT | Total events |
| unique_customers | BIGINT | Unique customers |
| unique_sessions | BIGINT | Unique sessions |
| views | BIGINT | Total views |
| clicks | BIGINT | Total clicks |
| conversions | BIGINT | Total conversions |
| revenue | DOUBLE | Total revenue |
| avg_order_value | DOUBLE | Average order value |
| ctr_pct | DOUBLE | Click-through rate percentage |
| conversion_rate_pct | DOUBLE | Conversion rate percentage |

---

### 16. gold_metrics_geographic_performance
**Description:** Geographic performance metrics by country and state.

**Business Use Cases:**
- Geographic targeting
- Regional performance analysis
- Market expansion analysis
- Location-based optimization

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| country | STRING | Country |
| state | STRING | State/Province |
| events | BIGINT | Total events |
| unique_customers | BIGINT | Unique customers |
| unique_sessions | BIGINT | Unique sessions |
| views | BIGINT | Total views |
| clicks | BIGINT | Total clicks |
| conversions | BIGINT | Total conversions |
| revenue | DOUBLE | Total revenue |
| ctr_pct | DOUBLE | Click-through rate percentage |
| conversion_rate_pct | DOUBLE | Conversion rate percentage |
| revenue_per_customer | DOUBLE | Revenue per customer |

---

### 17. gold_metrics_traffic_source
**Description:** Traffic source and partner performance metrics.

**Business Use Cases:**
- Partner performance analysis
- Traffic source ROI
- Channel optimization
- Partner comparison

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| partner_id | STRING | Partner identifier |
| partner_name | STRING | Partner name |
| traffic_partner_type | STRING | Partner type |
| source_id | STRING | Source identifier |
| events | BIGINT | Total events |
| unique_customers | BIGINT | Unique customers |
| unique_sessions | BIGINT | Unique sessions |
| views | BIGINT | Total views |
| clicks | BIGINT | Total clicks |
| conversions | BIGINT | Total conversions |
| revenue | DOUBLE | Total revenue |
| ctr_pct | DOUBLE | Click-through rate percentage |
| conversion_rate_pct | DOUBLE | Conversion rate percentage |
| revenue_per_customer | DOUBLE | Revenue per customer |
| events_per_customer | DOUBLE | Events per customer |

---

### 18. gold_metrics_temporal_patterns
**Description:** Day-of-week and temporal pattern metrics.

**Business Use Cases:**
- Day-of-week optimization
- Weekend vs weekday analysis
- Business hours analysis
- Scheduling optimization

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| day_of_week | STRING | Day name (Monday, Tuesday, etc.) |
| is_weekend | BOOLEAN | Weekend flag |
| is_business_hours | BOOLEAN | Business hours flag |
| events | BIGINT | Total events |
| unique_customers | BIGINT | Unique customers |
| unique_sessions | BIGINT | Unique sessions |
| views | BIGINT | Total views |
| clicks | BIGINT | Total clicks |
| conversions | BIGINT | Total conversions |
| revenue | DOUBLE | Total revenue |
| ctr_pct | DOUBLE | Click-through rate percentage |
| conversion_rate_pct | DOUBLE | Conversion rate percentage |

---

### 19. gold_metrics_session_funnel
**Description:** Session-level funnel metrics showing session progression through view, click, convert, and transaction stages.

**Business Use Cases:**
- Session funnel optimization
- Session conversion analysis
- User journey analysis
- Session-level drop-off analysis

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| total_sessions | BIGINT | Total sessions |
| sessions_with_view | BIGINT | Sessions with at least one view |
| sessions_with_click | BIGINT | Sessions with at least one click |
| sessions_with_conversion | BIGINT | Sessions with conversion |
| sessions_with_transaction | BIGINT | Sessions with transaction |
| view_to_click_rate | DOUBLE | View to click rate percentage |
| click_to_convert_rate | DOUBLE | Click to convert rate percentage |
| convert_to_transaction_rate | DOUBLE | Convert to transaction rate percentage |
| overall_transaction_rate | DOUBLE | Overall session transaction rate |
| total_revenue | DOUBLE | Total revenue |
| total_conversions | BIGINT | Total conversions |

---

### 20. gold_metrics_repeat_users_daily
**Description:** Daily breakdown of new vs returning customers.

**Business Use Cases:**
- New customer acquisition tracking
- Returning customer monitoring
- Customer mix analysis
- Acquisition vs retention balance

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| total_customers | BIGINT | Total unique customers |
| new_customers | BIGINT | First-time customers on this date |
| returning_customers | BIGINT | Returning customers |
| new_customer_pct | DOUBLE | Percentage of new customers |
| returning_customer_pct | DOUBLE | Percentage of returning customers |

---

### 21. gold_metrics_new_vs_returning_performance
**Description:** Performance comparison between new and returning customers including events, revenue, and conversion rates.

**Business Use Cases:**
- New vs returning customer comparison
- Customer type performance analysis
- Acquisition vs retention ROI
- Lifetime value comparison

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| customer_type | STRING | 'new' or 'returning' |
| events | BIGINT | Total events |
| unique_customers | BIGINT | Unique customers |
| unique_sessions | BIGINT | Unique sessions |
| views | BIGINT | Total views |
| clicks | BIGINT | Total clicks |
| conversions | BIGINT | Total conversions |
| revenue | DOUBLE | Total revenue |
| events_per_customer | DOUBLE | Events per customer |
| revenue_per_customer | DOUBLE | Revenue per customer |
| ctr_pct | DOUBLE | Click-through rate percentage |
| conversion_rate_pct | DOUBLE | Conversion rate percentage |

---

### 22. gold_metrics_cohort_retention
**Description:** Monthly cohort retention metrics showing how cohorts retain over time.

**Business Use Cases:**
- Cohort retention analysis
- Customer stickiness measurement
- Long-term engagement tracking
- Retention curve visualization

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| cohort_month | DATE | Month of customer's first activity |
| month_number | INT | Months since cohort start (0 = first month) |
| active_customers | BIGINT | Active customers in this month |
| cohort_size | BIGINT | Original cohort size |
| retention_rate_pct | DOUBLE | Retention rate percentage |

---

## CATEGORY 3: CAMPAIGN VIEWS (3 Views)

### 23. campaign_top_customers
**Description:** Top customers ranked by revenue for each campaign. Enables answering "Who are the top customers for Campaign X?"

**Business Use Cases:**
- Campaign-level customer ranking
- Top customer identification by campaign
- Campaign attribution analysis
- VIP customer identification per campaign

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| campaign_id | STRING | Campaign identifier |
| campaign_name | STRING | Campaign name |
| advertiser_id | STRING | Advertiser identifier |
| advertiser_name | STRING | Advertiser name |
| vertical | STRING | Campaign vertical |
| customer_key | STRING | Customer identifier |
| is_identified_customer | BOOLEAN | Identified customer flag |
| fluent_id | STRING | Fluent ID |
| total_impressions | BIGINT | Total impressions |
| total_views | BIGINT | Total views |
| total_clicks | BIGINT | Total clicks |
| total_conversions | BIGINT | Total conversions |
| total_transactions | BIGINT | Total transactions |
| total_revenue | DOUBLE | Total revenue |
| total_transaction_value | DOUBLE | Total transaction value |
| total_sessions | BIGINT | Total sessions |
| active_days | BIGINT | Number of active days |
| first_interaction | TIMESTAMP | First interaction timestamp |
| last_interaction | TIMESTAMP | Last interaction timestamp |
| revenue_rank | INT | Rank by revenue within campaign (1 = highest) |
| overall_ctr | DOUBLE | Overall click-through rate |
| overall_conversion_rate | DOUBLE | Overall conversion rate |
| avg_order_value | DOUBLE | Average order value |

---

### 24. campaign_performance_summary
**Description:** Campaign-level performance summary with aggregated customer and revenue metrics.

**Business Use Cases:**
- Campaign comparison
- Campaign ROI analysis
- Advertiser reporting
- Campaign leaderboard

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| campaign_id | STRING | Campaign identifier |
| campaign_name | STRING | Campaign name |
| advertiser_id | STRING | Advertiser identifier |
| advertiser_name | STRING | Advertiser name |
| vertical | STRING | Campaign vertical |
| total_customers | BIGINT | Total unique customers |
| identified_customers | BIGINT | Identified customers |
| total_impressions | BIGINT | Total impressions |
| total_views | BIGINT | Total views |
| total_clicks | BIGINT | Total clicks |
| overall_ctr | DOUBLE | Overall click-through rate |
| total_conversions | BIGINT | Total conversions |
| overall_conversion_rate | DOUBLE | Overall conversion rate |
| total_transactions | BIGINT | Total transactions |
| total_revenue | DOUBLE | Total revenue |
| total_transaction_value | DOUBLE | Total transaction value |
| revenue_per_customer | DOUBLE | Revenue per customer |
| avg_order_value | DOUBLE | Average order value |
| total_sessions | BIGINT | Total sessions |
| sessions_per_customer | DOUBLE | Sessions per customer |
| first_date | DATE | First activity date |
| last_date | DATE | Last activity date |
| active_days | BIGINT | Number of active days |

---

### 25. campaign_daily_performance
**Description:** Daily campaign metrics for time-series analysis.

**Business Use Cases:**
- Campaign trend analysis
- Daily campaign monitoring
- Campaign performance over time
- Time-series visualization

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| date_est | DATE | Date (EST timezone) |
| campaign_id | STRING | Campaign identifier |
| campaign_name | STRING | Campaign name |
| advertiser_name | STRING | Advertiser name |
| vertical | STRING | Campaign vertical |
| daily_customers | BIGINT | Unique customers on this date |
| impressions | BIGINT | Total impressions |
| clicks | BIGINT | Total clicks |
| ctr | DOUBLE | Click-through rate |
| conversions | BIGINT | Total conversions |
| transactions | BIGINT | Total transactions |
| revenue | DOUBLE | Total revenue |
| revenue_per_customer | DOUBLE | Revenue per customer |

---

## QUICK REFERENCE: COMMON DASHBOARD QUERIES

### KPI Cards (use customer_daily_kpis)
```sql
SELECT metric_date, total_customers, active_customers, total_daily_revenue, total_conversions
FROM customer_daily_kpis
WHERE metric_date = (SELECT MAX(metric_date) FROM customer_daily_kpis)
```

### Revenue Trend (use customer_daily_kpis)
```sql
SELECT metric_date, total_daily_revenue
FROM customer_daily_kpis
WHERE metric_date >= CURRENT_DATE - 30
ORDER BY metric_date
```

### Customer Segments (use customer_current_state)
```sql
SELECT value_segment, COUNT(*) as customer_count, SUM(lifetime_revenue) as total_revenue
FROM customer_current_state
GROUP BY value_segment
```

### Conversion Funnel (use gold_metrics_conversion_funnel)
```sql
SELECT date_est, viewers, clickers, converters, buyers
FROM gold_metrics_conversion_funnel
WHERE date_est >= CURRENT_DATE - 7
```

### Device Performance (use gold_metrics_device_performance)
```sql
SELECT device_type, SUM(revenue) as total_revenue, AVG(conversion_rate_pct) as avg_conv_rate
FROM gold_metrics_device_performance
WHERE date_est >= CURRENT_DATE - 7
GROUP BY device_type
```

### Top Campaigns (use campaign_performance_summary)
```sql
SELECT campaign_name, total_revenue, total_customers, overall_conversion_rate
FROM campaign_performance_summary
ORDER BY total_revenue DESC
LIMIT 10
```

### At-Risk Customers (use at_risk_customers)
```sql
SELECT customer_key, lifetime_revenue, churn_risk_score_pct, days_since_last_activity
FROM at_risk_customers
WHERE value_segment = 'high_value'
ORDER BY lifetime_revenue DESC
LIMIT 20
```

### Cohort Retention (use gold_metrics_cohort_retention)
```sql
SELECT cohort_month, month_number, retention_rate_pct
FROM gold_metrics_cohort_retention
WHERE cohort_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 6 MONTHS)
```
