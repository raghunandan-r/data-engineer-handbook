SELECT * FROM events;

DROP TABLE cumulative_users;

CREATE TABLE cumulative_users(
    user_id TEXT,
    active_dates DATE[],
    date DATE,
    PRIMARY KEY (user_id, date)
);

CREATE OR REPLACE FUNCTION full_cum()
    RETURNS void AS $$
    DECLARE current_date_i DATE:='2023-01-31';
        BEGIN
        WHILE current_date_i <= '2023-01-31' LOOP

            INSERT INTO cumulative_users

            WITH yesterday AS (SELECT *
                               FROM cumulative_users
                               WHERE DATE(date) = current_date_i-1),
                today AS (
                    SELECT
                        CAST(user_id AS TEXT) as user_id,
                        DATE(CAST(event_time AS timestamp)) AS event_date
                        FROM events
                    WHERE DATE(CAST(event_time AS TIMESTAMP)) = current_date_i
                    AND user_id IS NOT NULL
                    GROUP BY user_id, event_date
                )
            SELECT
                COALESCE(y.user_id, t.user_id) AS user_id,
                CASE
                    WHEN y.active_dates IS NULL THEN ARRAY[t.event_date]
                    WHEN t.event_date IS NULL THEN y.active_dates
                    ELSE y.active_dates || ARRAY[t.event_date]
                END AS active_dates,
                COALESCE(t.event_date, y.date + Interval '1 day') AS date
            FROM yesterday y FULL OUTER JOIN today t
            ON y.user_id = t.user_id
            ON CONFLICT (user_id, date) DO UPDATE
            SET active_dates = excluded.active_dates
            ;
            current_date_i = current_date_i + Interval '1 day';
        end loop;
    end;
$$ language plpgsql;

SELECT full_cum();

SELECT * from cumulative_users;

-- Calculate the MAU, WAU & DAU dimensions

WITH users AS (SELECT *
               from cumulative_users
--                where date = '2023-01-30'
               ),
series AS (SELECT * from generate_series('2023-01-01', '2023-01-31', interval '1 day') as series_date),
placeholder_ AS (SELECT *,
                        CASE
                            WHEN active_dates @> ARRAY [DATE(series_date)]
                                THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
                            ELSE 0
                            END AS placeholder
                 FROM users
                          CROSS JOIN series
--                  WHERE user_id = '137925124111668560'
                 )
SELECT user_id,
        CAST(CAST(SUM(placeholder) AS BIGINT) AS BIT(32)) AS dates_active_representation,
        BIT_COUNT(CAST(CAST(SUM(placeholder) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
        BIT_COUNT( CAST('11111110000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active,
        BIT_COUNT( CAST('10000000000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active
FROM placeholder_
GROUP BY user_id

