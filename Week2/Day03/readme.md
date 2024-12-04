# Building Reduced Facts for Metric Aggregation

This demonstrates how to build a reduced facts table for efficient metric storage and aggregation using PostgreSQL. The primary goal is to track user activity metrics over time and reduce data redundancy by storing aggregated arrays of daily metrics.



## Overview

The reduced facts approach uses PostgreSQL arrays to store daily metrics for each user within a specific month. Key highlights include:
1. **Compact Metric Storage**: Aggregates daily metrics into arrays for each month.
2. **Dynamic Updates**: Dynamically appends new metrics to existing arrays.
3. **Efficient Aggregation**: Enables fast metric aggregation using SQL functions like `UNNEST` and `SUM`.


## Table Structure

### 1. Array Metrics Table
The `array_metrics` table stores metrics as arrays for each user, grouped by month and metric name.

```sql
CREATE TABLE array_metrics (
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT,
    metric_array REAL[],
    PRIMARY KEY (user_id, month_start, metric_name)
);
```


## Insertion and Updates

### Insert Daily Metrics
Daily metrics are appended to the existing array for each user. If no data exists, an array is initialized.

```sql
INSERT INTO array_metrics
WITH daily_aggregates AS (
    SELECT 
        user_id,
        DATE(event_time) AS date,
        COUNT(1) AS num_site_hits
    FROM events e 
    WHERE DATE(event_time) = DATE('2023-01-04') AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
),
yesterday_array AS (
    SELECT * FROM array_metrics WHERE month_start = DATE('2023-01-01')
)
SELECT
    COALESCE(da.user_id, ya.user_id) AS user_id,
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    'site_hits' AS metric_name,
    CASE 
        WHEN ya.metric_array IS NOT NULL THEN 
            ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.metric_array IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(da.num_site_hits, 0)]
    END AS metric_array
FROM
    daily_aggregates da
FULL OUTER JOIN yesterday_array ya 
ON da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO 
    UPDATE SET metric_array = EXCLUDED.metric_array;
```


## Metric Analysis

### Count Array Lengths
Analyze the cardinality (length) of the metric arrays.

```sql
SELECT CARDINALITY(metric_array), COUNT(1)
FROM array_metrics
GROUP BY 1;
```

### Aggregate Metrics Across Days
Summarize daily metrics across all users for each month and metric.

```sql
WITH agg_cte AS (
    SELECT
        metric_name,
        month_start,
        ARRAY[
            SUM(metric_array[1]),
            SUM(metric_array[2]),
            SUM(metric_array[3]),
            SUM(metric_array[4])
        ] AS summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
SELECT
    metric_name,
    month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) AS date,
    elem AS value
FROM agg_cte CROSS JOIN UNNEST(agg_cte.summed_array) WITH ORDINALITY AS a(elem, index);
```


### Aggregate Metrics Across Days query result

https://github.com/Shankar297/Data-Engineering-Bootcamp/blob/main/Week2/Day03/SQL%20Queries/result.jpg


## Key Features

1. **Reduced Storage Overhead**: Daily metrics are stored in arrays, reducing row count.
2. **Dynamic Updates**: Metrics can be dynamically updated and appended without schema changes.
3. **Efficient Aggregation**: Aggregated metrics can be computed efficiently across all days in a month.
