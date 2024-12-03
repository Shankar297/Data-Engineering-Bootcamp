# README: Data Processing Pipeline for Game Details

## Overview

This pipeline is designed to process game data by deduplicating records, calculating metrics, and creating a fact table (`fct_game_details`) for further analysis. The pipeline ensures data quality by removing duplicates, computing derived metrics, and storing the processed data in a structured format.

---

## SQL Workflow

### 1. **Check for Duplicates**

The query identifies duplicates in the `game_details` table based on the combination of `game_id`, `team_id`, and `player_id`.

```sql
SELECT
    game_id,
    team_id,
    player_id,
    count(1)
FROM
    game_details gd
GROUP BY 1, 2, 3
HAVING
    count(1) > 1;
```

### 2. **Deduplication and Data Insertion**

A Common Table Expression (CTE) named `deduped` is used to remove duplicates. The deduplication logic utilizes `ROW_NUMBER()` to keep the first occurrence of each record based on `game_date_est`.

The cleaned data is then inserted into the `fct_game_details` table.

#### Key Operations:
- **Derived Metrics:** Compute additional columns like `dim_did_not_play`, `dim_did_not_dress`, and `dim_not_with_team` based on the `comment` field.
- **Game Metrics:** Calculate metrics like minutes played (`m_minutes`), field goals (`m_fgm`, `m_fga`), assists (`m_ast`), rebounds (`m_reb`), and more.

```sql
INSERT INTO fct_game_details
WITH deduped AS (
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM
        game_details gd
    JOIN games g 
        ON gd.game_id = g.game_id
)
SELECT
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' IN "comment") > 0) AS dim_did_not_play,
    COALESCE(POSITION('DND' IN "comment") > 0) AS dim_did_not_dress,
    COALESCE(POSITION('NWT' IN "comment") > 0) AS dim_not_with_team,
    CAST(split_part(min, ':', 1) AS REAL) + CAST(split_part(min, ':', 2) AS REAL)/ 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_muns
FROM
    deduped
WHERE
    row_num = 1;
```

### 3. **Create the `fct_game_details` Table**

This table stores the processed game details. It is designed with a primary key on `dim_game_date`, `dim_team_id`, and `dim_player_id` to ensure data uniqueness.

```sql
CREATE TABLE fct_game_details (
    dim_game_date DATE,
    dim_season INTEGER,
    dim_team_id INTEGER,
    dim_player_id INTEGER,
    dim_player_name TEXT,
    dim_start_position TEXT,
    dim_is_playing_at_home BOOLEAN,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOLEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_fgm INTEGER,
    m_fga INTEGER,
    m_fg3m INTEGER,
    m_fg3a INTEGER,
    m_ftm INTEGER,
    m_fta INTEGER,
    m_oreb INTEGER,
    m_dreb INTEGER,
    m_reb INTEGER,
    m_ast INTEGER,
    m_stl INTEGER,
    m_blk INTEGER,
    m_turnovers INTEGER,
    m_pf INTEGER,
    m_pts INTEGER,
    m_plus_muns INTEGER,
    PRIMARY KEY(dim_game_date, dim_team_id, dim_player_id)
);
```

### 4. **Query Processed Data**

Fetch all data from the `fct_game_details` table:

```sql
SELECT * FROM fct_game_details;
```

### 5. **Analyze Player Performance**

This query aggregates player performance metrics, including total points scored, number of games, and percentage of games where players were marked as "Not With Team."

```sql
SELECT
    dim_player_name, 
    dim_is_playing_at_home,
    count(1) AS num_games,
    SUM(m_pts) AS total_points,
    COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num,
    CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL) / COUNT(1) AS bail_pct
FROM
    fct_game_details
GROUP BY 1, 2
ORDER BY bail_pct DESC;
```

---

## Table Structure

### **fct_game_details**

| Column                | Type     | Description                                       |
|------------------------|----------|---------------------------------------------------|
| dim_game_date          | DATE     | Game date.                                       |
| dim_season             | INTEGER  | Season of the game.                              |
| dim_team_id            | INTEGER  | Team ID.                                         |
| dim_player_id          | INTEGER  | Player ID.                                       |
| dim_player_name        | TEXT     | Player name.                                     |
| dim_start_position     | TEXT     | Player's start position.                         |
| dim_is_playing_at_home | BOOLEAN  | Indicates if the player is playing at home.      |
| dim_did_not_play       | BOOLEAN  | Indicates if the player did not play.            |
| dim_did_not_dress      | BOOLEAN  | Indicates if the player did not dress.           |
| dim_not_with_team      | BOOLEAN  | Indicates if the player was not with the team.   |
| m_minutes              | REAL     | Minutes played.                                  |
| m_fgm                  | INTEGER  | Field goals made.                                |
| m_fga                  | INTEGER  | Field goals attempted.                           |
| m_fg3m                 | INTEGER  | Three-point field goals made.                    |
| m_fg3a                 | INTEGER  | Three-point field goals attempted.               |
| m_ftm                  | INTEGER  | Free throws made.                                |
| m_fta                  | INTEGER  | Free throws attempted.                           |
| m_oreb                 | INTEGER  | Offensive rebounds.                              |
| m_dreb                 | INTEGER  | Defensive rebounds.                              |
| m_reb                  | INTEGER  | Total rebounds.                                  |
| m_ast                  | INTEGER  | Assists.                                         |
| m_stl                  | INTEGER  | Steals.                                          |
| m_blk                  | INTEGER  | Blocks.                                          |
| m_turnovers            | INTEGER  | Turnovers.                                       |
| m_pf                   | INTEGER  | Personal fouls.                                  |
| m_pts                  | INTEGER  | Points scored.                                   |
| m_plus_muns            | INTEGER  | Plus/minus score.                                |
