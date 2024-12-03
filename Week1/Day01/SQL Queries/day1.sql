CREATE TYPE season_stats AS (
                         season Integer,
                         pts REAL,
                         ast REAL,
                         reb REAL,
                         weight INTEGER
                       );

CREATE TYPE scoring_class AS
     ENUM ('bad', 'average', 'good', 'star');

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
	 is_active boolean,
	current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );




DROP TABLE players 



INSERT INTO
	players
WITH yesterday AS (
	SELECT
		*
	FROM
		players
	WHERE
		current_season = 2000
),
today AS (
	SELECT
		*
	FROM
		player_seasons
	WHERE
		season = 2001)
SELECT
	COALESCE(t.player_name, y.player_name) AS player_name,
	COALESCE(t.height, y.height) AS height,
	COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
	COALESCE(t.draft_year, y.draft_year) AS draft_year,
	COALESCE(t.draft_round, y.draft_round) AS draft_round,
	COALESCE(t.draft_number, y.draft_number) AS draft_number,
	CASE
		WHEN y.seasons IS NULL 
		THEN ARRAY[ROW(t.season,
		t.gp,
		t.pts,
		t.reb,
		t.ast)::season_stats]
		WHEN t.season IS NOT NULL THEN y.seasons || ARRAY[ROW(t.season,
		t.gp,
		t.pts,
		t.reb,
		t.ast)::season_stats]
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
FROM
	today t
FULL OUTER JOIN yesterday y 
	ON t.player_name = y.player_name


SELECT
	*
FROM
	players
WHERE
	current_season = 2000
	AND player_name = 'Michael Jordan'
	-- and country like 'St%'


SELECT
	player_name ,
	(seasons[1]::season_stats).pts /(seasons[CARDINALITY(seasons)]::season_stats).pts
FROM
	PLAYERS
WHERE
	CURRENT_SEASON = 2001
ORDER BY
	2 DESC


WITH unnested AS (
	SELECT
		player_name,
		UNNEST(seasons) AS season_stats
	FROM
		players p
	WHERE
		current_season = 2000
		-- and player_name = 'michael jordan'
)
SELECT
	player_name,
	(season_stats::season_stats).*
FROM
	unnested
