
# Graph Database Schema for Sports Analytics

This project implements a graph-based database schema for analyzing sports-related data, including players, teams, and games. The schema uses `vertices` and `edges` to represent relationships and interactions between entities.

---

## Database Structure

### 1. Vertex Types
The schema defines three types of vertices:
- **Player**: Represents individual players.
- **Team**: Represents sports teams.
- **Game**: Represents individual games.

```sql
CREATE TYPE vertex_type AS ENUM('player', 'team', 'game');
```

### 2. Vertices Table
This table stores all vertices along with their properties as JSON.

```sql
CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);
```

### 3. Edge Types
The schema defines four types of edges to represent relationships between entities:
- `plays_against`: Interaction between players on opposing teams.
- `shares_team`: Interaction between players on the same team.
- `plays_in`: Player's participation in a game.
- `plays_on`: Player's association with a team.

```sql
CREATE TYPE edge_type AS ENUM('plays_againt', 'shares_team', 'plays_in', 'plays_on');
```

### 4. Edges Table
This table stores relationships between vertices along with their properties as JSON.

```sql
CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
);
```

---

## Data Insertion

### 1. Adding Player Relationships (Edges)
The following query calculates relationships between players, such as games played against or as teammates.

```sql
INSERT INTO edges
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
filtered AS (
    SELECT * FROM deduped WHERE row_num = 1
),
aggregated AS (
    SELECT
        f1.player_id AS subject_player_id,
        MAX(f1.player_name) AS subject_player_name,
        MAX(f2.player_id) AS object_player_id,
        MAX(f2.player_name) AS object_player_name,
        CASE
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
            ELSE 'plays_againt'::edge_type
        END AS edge_type,
        COUNT(1) AS num_games,
        SUM(f1.pts) AS subject_points,
        SUM(f2.pts) AS object_points
    FROM filtered f1
    JOIN filtered f2 ON f1.game_id = f2.game_id AND f1.player_name <> f2.player_name
    WHERE f1.player_id > f2.player_id
    GROUP BY f1.player_id, CASE
        WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
        ELSE 'plays_againt'::edge_type
    END
)
SELECT 
    subject_player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    object_player_id AS object_identifier,
    'player'::vertex_type AS object_type,
    edge_type,
    json_build_object('num_games', num_games, 'subject_points', subject_points, 'object_points', object_points)
FROM aggregated;
```

### 2. Adding Game Vertices
The following query adds game vertices with properties such as points scored and the winning team.

```sql
INSERT INTO vertices
SELECT
    game_id AS identifier,
    'game'::vertex_type AS type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', CASE
            WHEN home_team_wins = 1 THEN home_team_id
            ELSE visitor_team_id
        END
    ) AS properties
FROM games g;
```

### 3. Adding Player Vertices
This query aggregates player data, such as total points and teams played for, and stores it as vertices.

```sql
INSERT INTO vertices
WITH player_agg AS (
    SELECT
        player_id AS identifier,
        MAX(player_name) AS player_name,
        COUNT(1) AS number_of_games,
        SUM(pts) AS total_points,
        ARRAY_AGG(DISTINCT team_id) AS teams
    FROM game_details gd
    GROUP BY player_id
)
SELECT
    identifier,
    'player'::vertex_type AS type,
    json_build_object('player_name', player_name, 'number_of_games', number_of_games, 'total_points', total_points, 'teams', teams)
FROM player_agg;
```

### 4. Adding Team Vertices
This query inserts team data with properties like city, arena, and founding year.

```sql
INSERT INTO vertices
WITH team_deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY team_id) AS row_num
    FROM teams
)
SELECT
    team_id AS identifier,
    'team'::vertex_type AS type,
    json_build_object('abbreviation', abbreviation, 'nickname', nickname, 'city', city, 'arena', arena, 'year_founded', yearfounded)
FROM team_deduped
WHERE row_num = 1;
```

---

## Data Analysis

The following query computes a player's performance ratio and their relationships with other players.

```sql
SELECT 
    v.properties->>'player_name' AS player_name,
    e.object_identifier AS opponent_id,
    CAST(v.properties->>'number_of_games' AS REAL) /
    CASE
        WHEN CAST(v.properties->>'total_points' AS REAL) = 0 THEN 1
        ELSE CAST(v.properties->>'total_points' AS REAL)
    END AS games_to_points_ratio,
    e.properties->>'subject_points' AS subject_points,
    e.properties->>'num_games' AS num_games
FROM vertices v
JOIN edges e ON v.identifier = e.subject_identifier AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;
```

---

## Key Features
1. **Graph Representation**: Models player, team, and game relationships using edges and vertices.
2. **Flexible Data Storage**: Uses JSON fields to accommodate diverse properties for entities.
3. **Efficient Querying**: Supports complex aggregations for relationship analysis.
