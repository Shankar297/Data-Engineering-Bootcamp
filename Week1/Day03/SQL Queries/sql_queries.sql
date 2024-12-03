CREATE TYPE vertex_type
	AS ENUM('player', 'team', 'game');



CREATE TABLE vertices (
	identifier TEXT,
	TYPE vertex_type,
	properties JSON,
	PRIMARY KEY (identifier, TYPE)
)


CREATE TYPE edge_type
	AS ENUM('plays_againt', 'shares_team', 'plays_in', 'plays_on')
	
	
create table edges ( 
	subject_identifier TEXT,
	subject_type vertex_type,
	object_identifier TEXT,
	object_type vertex_type,
	edge_type edge_type,
	properties JSON,
	PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
)



INSERT INTO edges
WITH deduped AS (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num
	FROM
		game_details 
),
filtered AS (
	SELECT
		*
	FROM
		deduped
	WHERE
		row_num = 1
),
aggregated AS (
	SELECT
		f1.player_id AS subject_player_id,
		max(f1.player_name) AS subject_player_name,
		max(f2.player_id) AS object_player_id,
		max(f2.player_name) AS object_player_name,
		CASE
			WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
			ELSE 'plays_againt'::edge_type
		END AS edge_type,
		count(1) AS num_games,
		sum(f1.pts) AS subject_points,
		sum(f2.pts) AS object_points
	FROM
		filtered f1
	JOIN filtered f2
		ON
			f1.game_id = f2.game_id
		AND f1.player_name <> f2.player_name
	WHERE
		f1.player_id > f2.player_id
	GROUP BY
		f1.player_id,
		f1.player_id,
		CASE
			WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
			ELSE 'plays_againt'::edge_type
		END
)
SELECT 
	subject_player_id AS subject_identifier,
	'player'::vertex_type AS subject_type,
	object_player_id AS object_identifier,
	'player'::vertex_type AS object_type,
	edge_type AS edge_type,
	json_build_object(
		'num_games', num_games,
		'subject_points', subject_points,
		'object_points', object_points )
FROM
	aggregated




INSERT INTO vertices 
SELECT
	game_id AS identifier,
	'game'::vertex_type AS TYPE,
	json_build_object(
			'pts_home', pts_home,
			'pts_away', pts_away,
			'winning_team',
							CASE
								WHEN home_team_wins = 1 THEN home_team_id
								ELSE visitor_team_id
							END
			) AS properties
FROM
	games g
	

INSERT INTO vertices 
WITH player_agg AS(
	SELECT
		player_id AS identifier,
		max(player_name) AS player_name ,
		count(1) AS number_of_games,
		sum(pts) AS total_points,
		array_agg(DISTINCT team_id) AS teams
	FROM
		game_details gd
	GROUP BY
		player_id 
)
SELECT
	identifier,
	'player'::vertex_type,
	json_build_object(
			'player_name', player_name ,
			'number_of_games', number_of_games,
			'total_points', total_points,
			'teams', teams)
FROM
	player_agg
	
	

INSERT INTO vertices 
WITH team_deduped AS (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY team_id) AS row_num
	FROM
		teams 
	)
SELECT
	team_id AS identifier,
	'team'::vertex_type AS TYPE,
	json_build_object(
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena ,
		'year_founded', yearfounded )
FROM
	team_deduped
WHERE
	row_num = 1



SELECT 
	v.properties->>'player_name',
	e.object_identifier,
	CAST(v.properties->>'number_of_games' AS REAL)/
	CASE
		WHEN CAST(v.properties->>'total_points' AS REAL) = 0 THEN 1
		ELSE
	CAST(v.properties->>'total_points' AS REAL)
	END,
	e.properties->>'subject_points',
	e.properties->>'num_games'
FROM
	vertices v
JOIN edges e
ON
	v.identifier = e.subject_identifier
	AND v.type = e.subject_type
WHERE
	e.object_type = 'player'::vertex_type