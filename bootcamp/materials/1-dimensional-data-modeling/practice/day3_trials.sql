CREATE TYPE vertex_type AS ENUM ('player','team','game');

CREATE TABLE vertex (
    identifier INTEGER,
    vertex_type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, vertex_type)

);

CREATE TYPE edge_type AS ENUM ('played_for', 'played_against', 'played_with', 'played_in');

CREATE TABLE edges (
    object_identifier INTEGER,
    object_type vertex_type,
    subject_identifier INTEGER,
    subject_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (object_identifier, object_type, subject_identifier, subject_type, edge_type)
);

-- INSERT INTO vertex
-- SELECT
--     game_id as identifier,
--     'game'::vertex_type,
--     json_build_object(
--     'home_team', home_team_id,
--     'away_team', visitor_team_id,
--     'pts_home', pts_home,
--     'pts_away', pts_away,
--     'winner', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
--     )
-- FROM games;
--
-- INSERT INTO vertex
-- WITH teams_dedupded AS (SELECT *, row_number() over (PARTITION BY team_id) as rn
--                         FROM teams)
-- SELECT team_id AS identifier,
--        'team'::vertex_type,
--        json_build_object(
--        'city', city,
--                'name', nickname,
--        'abbreviation', abbreviation,
--        'arena',arena
--        )
-- FROM teams_dedupded
-- WHERE rn = 1;
--
-- INSERT INTO vertex
-- WITH game_deets_deduped AS (
--     SELECT *, row_number() over (PARTITION BY game_id, player_id) AS rn FROM game_details
-- ),
-- player_agg AS (
-- SELECT player_id AS identifier,
--         MAX(player_name) AS player_name,
--         COUNT(*) AS games_played,
--         SUM(pts) AS total_points,
--         ARRAY_AGG(DISTINCT team_id) AS teams
-- FROM game_deets_deduped
-- WHERE rn = 1
-- GROUP BY player_id
-- )
-- SELECT identifier AS identifier,
--        'player'::vertex_type,
--        json_build_object(
--        'player_name', player_name,
--        'games_played', games_played,
--        'total_points', total_points,
--        'teams', teams
--        )
-- FROM player_agg;

SELECT vertex.vertex_type, COUNT(*) FROM vertex GROUP BY 1

INSERT INTO edges
WITH game_deets AS (
    SELECT *, row_number() over (PARTITION BY game_id, player_id) AS rn FROM game_details
), game_deets_dedupe AS (SELECT *
                         FROM game_deets
                         WHERE rn = 1),
aggregated AS (SELECT g1.player_id                                                                    AS subject_player_id,
                      g2.player_id                                                                    AS object_player_id,
                      MAX(g1.player_name)                                                                  AS subject_player_name,
                      MAX(g2.player_name)                                                                  AS object_player_name,
                      CASE WHEN g1.team_id <> g2.team_id THEN 'played_against'::edge_type
                          ELSE 'played_with'::edge_type END                                           AS reln,
                      SUM(g1.pts)                                                                     AS subject_tot_pts,
                      SUM(g2.pts)                                                                     AS object_tot_pts,
                      COUNT(*)                                                                        AS num_games
               FROM game_deets_dedupe g1
                        JOIN game_deets_dedupe g2
                             ON g1.game_id = g2.game_id AND g1.player_id <> g2.player_id
               WHERE g1.player_name > g2.player_name
               GROUP BY g1.player_id, g2.player_id, reln)
SELECT subject_player_id AS subject_identifier,
       'player'::vertex_type,
       object_player_id AS object_identifier,
        'player'::vertex_type,
        reln AS edge_type,
        json_build_object(
        'subject_tot_pts',subject_tot_pts,
        'object_tot_pts',object_tot_pts,
        'num_games',num_games
        )
FROM aggregated;

SELECT v1.properties->>'player_name' AS sub_player,
       v2.properties->>'player_name' AS obj_player,
    (e.properties->>'num_games')::INTEGER AS games_played_against
FROM edges e JOIN vertex v1
    ON v1.identifier = e.subject_identifier
    AND v1.vertex_type = e.subject_type
JOIN vertex v2
    ON v2.identifier = e.object_identifier
    AND v2.vertex_type = e.object_type
WHERE e.edge_type = 'played_against'::edge_type
ORDER BY games_played_against DESC
