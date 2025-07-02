SELECT player_name, scoring_class, is_active FROM players
WHERE current_season = 2022;

CREATE TABLE player_scd (
    player_name TEXT,
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);

WITH player_char_prev AS(
SELECT player_name, scoring_class, is_active, current_season,
LAG(scoring_class,1) OVER (PARTITION BY player_name ORDER BY current_season) AS prev_scoring_class,
LAG(is_active,1) OVER (PARTITION BY player_name ORDER BY current_season) AS prev_is_active
FROM players
WHERE current_season < 2022
),
player_char_change AS(
SELECT player_name, scoring_class, is_active, current_season,
CASE WHEN scoring_class != prev_scoring_class OR is_active != prev_is_active THEN 1 ELSE 0 END AS is_change
FROM player_char_prev
),
player_char_change_streak AS(
SELECT *, 
SUM(is_change) OVER (PARTITION BY player_name ORDER BY current_season) AS change_streak
FROM player_char_change
)
SELECT player_name, scoring_class, is_active, current_season, change_streak,
MIN(current_season) AS start_season,
MAX(current_season) AS end_season
FROM player_char_change_streak
GROUP BY player_name, scoring_class, is_active, change_streak
ORDER BY player_name, scoring_class




