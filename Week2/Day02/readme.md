# Building a DateList Data Type for User Activity Analysis

This project demonstrates how to implement a `datelist`-like data type in PostgreSQL. The primary use case is tracking user activity over time and generating insights such as whether a user is active monthly or weekly.

## Overview

The implementation uses PostgreSQL arrays to store lists of active dates for each user, enabling advanced analysis of user activity. Key features include:
1. **Cumulative User Activity Tracking**: Store and update user activity dates dynamically.
2. **Generate Activity Series**: Leverage date series for analytical calculations.
3. **Dimensional Analysis**: Determine monthly or weekly activity using bitwise operations.


## Table Structure

### 1. Events Table
The `events` table stores event timestamps and user IDs.

```sql
SELECT MAX(event_time), MIN(event_time) FROM events e;
SELECT * FROM events e;
```

### 2. Cumulative User Activity Table
The `users_cumulated` table stores each user's cumulative activity dates.

```sql
CREATE TABLE users_cumulated (
    user_id TEXT,
    dates_active DATE[],
    date DATE,
    PRIMARY KEY (user_id, date)
);
```


## Insertion and Updates

### Insert User Activity
Daily user activity is updated by combining data from the previous day and the current day.

```sql
INSERT INTO users_cumulated
WITH yesterday AS (
    SELECT * FROM users_cumulated WHERE date = DATE('2023-01-30')
),
today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events e
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = '2023-01-31'
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    CASE
        WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE ARRAY[t.date_active] || y.dates_active
    END AS dates_active,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y ON t.user_id = y.user_id;
```

### Query Cumulative Data
Fetch user activity for a specific date.

```sql
SELECT * FROM users_cumulated WHERE date = '2023-01-31';
```


## Generating a Date Series

A date series is used to analyze activity over a specific period.

```sql
SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS date;
```



## Activity Analysis

### Dimensional Activity Analysis
Analyze whether users are monthly or weekly active using bitwise operations.

```sql
WITH users AS (
    SELECT * FROM users_cumulated WHERE date = '2023-01-31'
),
series AS (
    SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
),
place_holder_ints AS (
    SELECT
        CASE
            WHEN dates_active @> ARRAY[DATE(series_date)]
            THEN CAST(POW(2, 32 - (u.date - DATE(series_date))) AS BIGINT)
            ELSE 0
        END AS placeholder_int_value,
        *
    FROM users u CROSS JOIN series
)
SELECT
    user_id,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS activity_bitmap,
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
    BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) AS dim_is_weekly_active
FROM place_holder_ints
GROUP BY user_id;
```

### Output Metrics
- **Monthly Active**: Determines if a user is active at least once in the month.
- **Weekly Active**: Determines if a user is active in the past week based on the bitmap.


## Key Features

1. **Dynamic Date Tracking**: Track user activity dates dynamically with efficient data structures.
2. **Bitwise Activity Analysis**: Use bitwise operations for fast dimensional activity analysis.
3. **Scalable Queries**: Process large datasets using optimized SQL operations.

3. **Performance Optimization**: Introduce indexing strategies for better query performance.