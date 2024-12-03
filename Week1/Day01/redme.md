# README: Player Data Management System

## Overview

This pipeline processes and manages player data using a relational database schema enhanced with user-defined types, arrays, and enumerations. The system stores player details, season-specific statistics, and their scoring classification, enabling advanced querying and analytics.

---

## SQL Workflow

### 1. **Create Custom Data Types**

#### **`season_stats` Type**
Represents season-specific player statistics.

```sql
CREATE TYPE season_stats AS (
    season INTEGER,
    pts REAL,
    ast REAL,
    reb REAL,
    weight INTEGER
);
```

#### **`scoring_class` Type**
Categorizes players based on their scoring performance.

```sql
CREATE TYPE scoring_class AS ENUM ('bad', 'average', 'good', 'star');
```

---

### 2. **Create `players` Table**

The `players` table stores comprehensive details about players, including their biographical data, seasonal performance, and scoring classification.

```sql
CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    seasons season_stats[],
    scoring_class scoring_class,
    is_active BOOLEAN,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);
```

#### Key Features:
- **Primary Key:** Combines `player_name` and `current_season` to ensure uniqueness.
- **Array Field:** `seasons` is an array of `season_stats` type, allowing multiple season records per player.
- **Enumeration Field:** `scoring_class` categorizes player performance.

---

### 3. **Delete the `players` Table**

```sql
DROP TABLE players;
```

---

### 4. **Insert Data into `players` Table**

The `INSERT` query populates the `players` table by combining historical and current season data using a full outer join. It handles missing values with `COALESCE` and constructs new season records using the `season_stats` type.

```sql
INSERT INTO players
WITH yesterday AS (
    SELECT * FROM players
    WHERE current_season = 2000
),
today AS (
    SELECT * FROM player_seasons
    WHERE season = 2001
)
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
    COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
    CASE
        WHEN y.seasons IS NULL THEN ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
        WHEN t.season IS NOT NULL THEN y.seasons || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
        ELSE y.seasons
    END AS seasons,
    CASE
        WHEN t.season IS NOT NULL THEN 
            CASE
                WHEN t.pts > 20 THEN 'star'
                WHEN t.pts > 15 THEN 'good'
                WHEN t.pts > 10 THEN 'average'
                ELSE 'bad'
            END::scoring_class
        ELSE y.scoring_class
    END AS scoring_class,
    CASE
        WHEN t.season IS NOT NULL THEN 0
        ELSE y.years_since_last_active + 1
    END AS years_since_last_active,
    COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t
FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;
```

---

### 5. **Query Players by Season**

Fetch player details for a specific season, such as `2000`.

```sql
SELECT * 
FROM players
WHERE current_season = 2000 
AND player_name = 'Michael Jordan';
```

---

### 6. **Analyze Performance Progression**

Calculate the performance ratio between the first and last recorded seasons.

```sql
SELECT
    player_name,
    (seasons[1]::season_stats).pts / (seasons[CARDINALITY(seasons)]::season_stats).pts AS performance_ratio
FROM
    players
WHERE
    current_season = 2001
ORDER BY performance_ratio DESC;
```

---

### 7. **Unnest and Query Season Statistics**

Unnest the `seasons` array to retrieve detailed statistics for each season.

```sql
WITH unnested AS (
    SELECT
        player_name,
        UNNEST(seasons) AS season_stats
    FROM
        players
    WHERE
        current_season = 2000
)
SELECT
    player_name,
    (season_stats::season_stats).*
FROM
    unnested;
```

---

## Table Structure

### **`players` Table**

| Column              | Type              | Description                                   |
|----------------------|-------------------|-----------------------------------------------|
| player_name          | TEXT              | Name of the player.                          |
| height               | TEXT              | Height of the player.                        |
| college              | TEXT              | College attended by the player.              |
| country              | TEXT              | Country of origin.                           |
| draft_year           | TEXT              | Year of the player's draft.                  |
| draft_round          | TEXT              | Draft round.                                 |
| draft_number         | TEXT              | Draft number.                                |
| seasons              | `season_stats[]`  | Array of season statistics.                  |
| scoring_class        | `scoring_class`   | Scoring classification.                      |
| is_active            | BOOLEAN           | Indicates if the player is active.           |
| current_season       | INTEGER           | Current season for the player.               |

### **`season_stats` Type**

| Field    | Type    | Description                     |
|----------|---------|---------------------------------|
| season   | INTEGER | Season year.                   |
| pts      | REAL    | Points scored.                 |
| ast      | REAL    | Assists made.                  |
| reb      | REAL    | Rebounds made.                 |
| weight   | INTEGER | Player's weight.               |

---

## Future Enhancements
- Integrate advanced metrics such as shooting percentages and efficiency ratings.
- Automate data ingestion from external APIs or sources.
- Implement views for common queries and analytics.

---

## Notes
- Ensure compatibility of `CREATE TYPE` syntax with the database system in use (e.g., PostgreSQL).
- Adjust column types or constraints as necessary to match your specific data requirements.