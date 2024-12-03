WITH last_season AS (
		SELECT
			*
		FROM
			players
		WHERE
			current_season = 2000
), 
this_season AS (
		SELECT
			*
		FROM
			player_seasons
		WHERE
			season = 2001
)
INSERT INTO
	players
SELECT
	COALESCE(ls.player_name, ts.player_name) AS player_name,
	COALESCE(ls.height, ts.height) AS height,
	COALESCE(ls.college, ts.college) AS college,
	COALESCE(ls.country, ts.country) AS country,
	COALESCE(ls.draft_year, ts.draft_year) AS draft_year,
	COALESCE(ls.draft_round, ts.draft_round) AS draft_round,
	COALESCE(ls.draft_number, ts.draft_number) AS draft_number,
	COALESCE(ls.seasons),
	ARRAY[]::season_stats[]
            ) || CASE
		WHEN ts.season IS NOT NULL THEN
                ARRAY[ROW(
                ts.season,
		ts.pts,
		ts.ast,
		ts.reb,
		ts.weight)::season_stats]
		ELSE ARRAY[]::season_stats[]
	END
            AS seasons,
	CASE
		WHEN ts.season IS NOT NULL THEN
                 (CASE
			WHEN ts.pts > 20 THEN 'star'
			WHEN ts.pts > 15 THEN 'good'
			WHEN ts.pts > 10 THEN 'average'
			ELSE 'bad'
		END)::scoring_class
		ELSE ls.scoring_class
	END AS scoring_class,
	ts.season IS NOT NULL AS is_active,
	2001 AS current_season
FROM
	last_season ls
FULL OUTER JOIN this_season ts
    ON
	ls.player_name = ts.player_name
	-- drop table player_scd

SELECT
	*
FROM
	players p 


CREATE TABLE player_scd (
	player_name TEXT,
	scoring_class scoring_class,
	is_active boolean,
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, start_season)
)

SELECT
	*
FROM
	player_scd



INSERT INTO player_scd
WITH with_previous AS (
	SELECT
		player_name ,
		scoring_class,
		current_season,
		is_active,
		LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
		LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
	FROM
		players p
	WHERE
		current_season <= 2022),
with_indicator AS (
	SELECT
		*,
		CASE
			WHEN scoring_class <> previous_scoring_class THEN 1
			WHEN is_active <> previous_is_active THEN 1
			ELSE 0
		END AS change_indicator
	FROM
		with_previous),
with_streaks AS (
	SELECT
		*,
		SUM(change_indicator) OVER(PARTITION BY player_name ORDER BY current_season) AS streak_identifier
	FROM
		with_indicator)
SELECT
	player_name,
	scoring_class,
	is_active,
	min(current_season) AS start_season,
	max(current_season) AS end_season,
	2022 AS current_season
FROM
	with_streaks
GROUP BY
	player_name ,
	streak_identifier,
	is_active,
	scoring_class
ORDER BY
	player_name,
	streak_identifier



CREATE TYPE scd_type AS (
		scoring_class scoring_class,
		is_active boolean,
		start_season integer,
		end_season integer
)
	
	
	
WITH last_season_scd AS (
	SELECT
		*
	FROM
		player_scd
	WHERE
		current_season = 2021
		AND end_season = 2021
),
historical_scd AS (
	SELECT 
		player_name,
		scoring_class,
		is_active,
		start_season,
		end_season
	FROM
		player_scd
	WHERE
		current_season = 2021
		AND end_season < 2021
),
this_season_scd AS (
	SELECT
		*
	FROM
		player_scd
	WHERE
		current_season = 2022
),
unchanged_records AS (
	SELECT 
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ls.start_season,
		ls.current_season AS end_season
	FROM
		last_season_scd ls
	JOIN this_season_scd ts
	ON
		ls.player_name = ts.player_name
	WHERE
		ts.scoring_class = ls.scoring_class
		AND ts.is_active = ls.is_active
),
changed_records AS (
	SELECT 
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ls.start_season,
		ts.current_season AS end_season,
		UNNEST(ARRAY[
			ROW(
				ls.scoring_class,
				ls.is_active,
				ls.start_season,
				ls.end_season
				)::scd_type,
			ROW(
				ts.scoring_class,
				ts.is_active,
				ts.current_season,
				ts.current_season
				)::scd_type
		]) AS records
	FROM
		last_season_scd ls
	LEFT JOIN this_season_scd ts
	ON
		ls.player_name = ts.player_name
	WHERE
		(ts.scoring_class <> ls.scoring_class
			OR ts.is_active <> ls.is_active)
),
unnested_changed_records AS (
	SELECT 
		player_name,
		(records::scd_type).scoring_class,
		(records::scd_type).is_active,
		(records::scd_type).start_season,
		(records::scd_type).end_season
	FROM
		changed_records 
),
new_records AS (
	SELECT 
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ts.current_season AS start_season,
		ts.current_season AS end_season
	FROM
		this_season_scd ts
	LEFT JOIN last_season_scd ls
	ON
		ts.player_name = ls.player_name
	WHERE
		ls.player_name IS NULL
	)
SELECT
	*
FROM
	historical_scd
UNION ALL
SELECT
	*
FROM
	unchanged_records
UNION ALL
SELECT
	*
FROM
	unnested_changed_records
UNION ALL
SELECT
	*
FROM
	new_records