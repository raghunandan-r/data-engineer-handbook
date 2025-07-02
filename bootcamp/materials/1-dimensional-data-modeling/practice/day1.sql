SELECT * FROM player_seasons LIMIT 100;




CREATE TYPE season_stats AS (
  season INTEGER,
  gp INTEGER,
  pts INTEGER,
  reb INTEGER,
  ast INTEGER
)

CREATE TYPE scoring_class AS ENUM ('star','good','average','bad')

DROP TABLE IF EXISTS players; 
CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
    scoring_class scoring_class,
    years_since_last_season INTEGER,    
    current_season INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY (player_name, current_season)
)


CREATE OR REPLACE FUNCTION update_players()
RETURNS void AS $$
DECLARE current_season_i INTEGER:= 1996;
BEGIN
    WHILE current_season_i <= 2022 LOOP        
        
        INSERT INTO players (
            player_name, height, college, country, draft_year, draft_round, draft_number, season_stats, scoring_class, years_since_last_season, current_season, is_active
        )
        WITH yesterday AS (
            SELECT * FROM players
            WHERE current_season = current_season_i - 1
        ),
        today AS (
            SELECT * FROM player_seasons
            WHERE season = current_season_i
        )
        SELECT
            COALESCE(y.player_name, t.player_name) AS player_name,
            COALESCE(y.height, t.height) AS height,
            COALESCE(y.college, t.college) AS college,
            COALESCE(y.country, t.country) AS country,
            COALESCE(y.draft_year, t.draft_year) AS draft_year,
            COALESCE(y.draft_round, t.draft_round) AS draft_round,
            COALESCE(y.draft_number, t.draft_number) AS draft_number,
            CASE 
                WHEN y.season_stats IS NULL THEN ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
                WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
                ELSE y.season_stats 
            END AS season_stats, 
            CASE WHEN t.season IS NOT NULL THEN 
                CASE WHEN t.pts > 20 THEN 'star'
                WHEN t.pts > 15 THEN 'good'
                WHEN t.pts > 10 THEN 'average'
                ELSE 'bad'
                END::scoring_class
            ELSE y.scoring_class 
            END AS scoring_class,
            CASE WHEN t.season IS NOT NULL THEN 0
            ELSE y.years_since_last_season + 1
            END AS years_since_last_season,
            COALESCE(t.season, y.current_season + 1) AS current_season,
            CASE WHEN t.season = current_season_i THEN TRUE ELSE FALSE END AS is_active
        FROM yesterday y FULL OUTER JOIN today t ON y.player_name = t.player_name
        
        ON CONFLICT (player_name, current_season) DO UPDATE SET 
            height = EXCLUDED.height,
            college = EXCLUDED.college,
            country = EXCLUDED.country,
            draft_year = EXCLUDED.draft_year,
            draft_round = EXCLUDED.draft_round,
            draft_number = EXCLUDED.draft_number,
            season_stats = EXCLUDED.season_stats,
            scoring_class = EXCLUDED.scoring_class,
            years_since_last_season = EXCLUDED.years_since_last_season
        ;
        
        current_season_i := current_season_i + 1;
        RAISE NOTICE 'Updated players for season %', current_season_i;

    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT update_players();

WITH unnested AS (
    SELECT player_name, current_season, is_active,
    (season_stats) AS season_stats
    FROM players
     WHERE player_name = 'Michael Jordan'
)
SELECT player_name, scoring_class,
(season_stats::season_stats).*
FROM unnested

-- SELECT 
-- player_name,
-- 1.0 * (season_stats[cardinality(season_stats)]::season_stats).pts / 
-- CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1 ELSE (season_stats[1]::season_stats).pts END AS pts_over_time
-- FROM players
-- WHERE current_season = 2001
-- ORDER BY 2 DESC
