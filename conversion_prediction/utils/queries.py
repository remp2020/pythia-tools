queries = dict()
queries['user_profiles_by_date'] = '''
                WITH time_filtered_aggregations AS (
                    SELECT
                        *
                    FROM
                        aggregated_browser_days
                    WHERE
                        -- We go :moving_window_length days back so that we could have the correct window sum at the beginning of the time period
                        date >= :min_date - INTERVAL ':moving_window_length DAYS'
                        AND date <= :max_date
                )
                SELECT
                    summable_metrics_with_outcome_by_browser_id_by_date.date AS date,
                    summable_metrics_with_outcome_by_browser_id_by_date.browser_id AS browser_id,
                    summable_metrics_with_outcome_by_browser_id_by_date.user_id AS user_id,
                    pageview_count,
                    timespent_sum,
                    direct_visit_count,
                    visit_count,
                    CAST(pageview_count AS FLOAT) / CAST(visit_count AS FLOAT) AS pageviews_per_visit,
                    CAST(visit_count AS FLOAT) / CAST(days_active_count AS FLOAT) AS visits_per_day_active,
                    CAST(direct_visit_count AS FLOAT) / CAST(visit_count AS FLOAT) AS direct_visits_share,
                    CAST(timespent_sum AS FLOAT) / CAST(visit_count AS FLOAT) AS timespent_per_visit,
                    CAST(timespent_sum AS FLOAT) / CAST(pageview_count AS FLOAT) AS timespent_per_pageview,
                    device_brand AS device,
                    browser_family AS browser,
                    is_desktop,
                    is_mobile,
                    os_family AS os,
                    days_active_count,
                    CASE
                      WHEN active_on_date THEN outcome_original
                      WHEN NOT active_on_date AND outcome_filled IS NOT NULL THEN outcome_filled
                      ELSE 'no conversion'
                    END AS outcome,
                    active_on_date AS is_active_on_date
                FROM (
                    -- extract sums of metrics for each browser within the "last 7 days window" per each day
                    -- first day of selected date range includes only its own data (no pre :min_date data are loaded)x
                    SELECT
                        date_gap_filler AS date,
                        active_browser_id AS browser_id,
                        active_user_id AS user_id,
                        SUM(pageviews) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW) AS pageview_count,
                        SUM(timespent) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW) AS timespent_sum,
                        SUM(sessions_without_ref) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW) AS direct_visit_count,
                        SUM(sessions) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW) AS visit_count,
                        SUM(
                            CASE
                                WHEN date IS NULL THEN 0
                                ELSE 1
                            END) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW) AS days_active_count,
                        CASE
                            WHEN pageviews > 0 THEN True
                            ELSE False
                        END AS active_on_date,
                        unique_event_times.next_7_days_event AS outcome_filled,
                        all_date_browsers_combinations.next_7_days_event AS outcome_original,
                        ROW_NUMBER() OVER (PARTITION BY all_date_browsers_combinations.browser_id, all_date_browsers_combinations.date ORDER BY all_date_browsers_combinations.browser_id, all_date_browsers_combinations.date) AS row_number
                    FROM (
                        -- get aggregated browser data record for each day (even those where browser didn't have a pageview)
                        -- date_gap_filler (date) and browser_id are guaranteed to be present for each day within selected timerange
                        SELECT
                            *
                        FROM (
                            SELECT
                                GENERATE_SERIES(
                                    (SELECT MIN(date) FROM time_filtered_aggregations),
                                    (SELECT MAX(date) FROM time_filtered_aggregations),
                                    '1 DAY')::DATE AS date_gap_filler) date_filler
                        CROSS JOIN (
                            SELECT
                                DISTINCT(browser_id) AS active_browser_id,
                                user_id as active_user_id
                            FROM
                                time_filtered_aggregations
                            ) active_browser_ids
                        LEFT OUTER JOIN
                            time_filtered_aggregations
                        ON (active_browser_ids.active_browser_id = time_filtered_aggregations.browser_id)
                            AND date_filler.date_gap_filler = time_filtered_aggregations.date) all_date_browsers_combinations
                        LEFT JOIN (
                            SELECT
                              browser_id,
                              next_event_time,
                              next_7_days_event
                            FROM
                              time_filtered_aggregations
                            WHERE
                              next_event_time IS NOT NULL
                            GROUP BY
                              browser_id,
                              next_event_time,
                              next_7_days_event) unique_event_times
                        ON (unique_event_times.browser_id = all_date_browsers_combinations.browser_id
                            AND unique_event_times.next_event_time > all_date_browsers_combinations.date - INTERVAL '7 DAYS'
                            AND all_date_browsers_combinations.date  <= unique_event_times.next_event_time)
                        WHERE
                            date_gap_filler = date
                            OR date IS NULL
                        ) summable_metrics_with_outcome_by_browser_id_by_date
                        LEFT JOIN (
                            SELECT
                                DISTINCT ON (browser_id)
                                CASE
                                  WHEN device_brand IS NULL THEN 'Desktop'
                                  ELSE device_brand
                                END AS device_brand,
                                browser_family,
                                is_desktop,
                                is_mobile,
                                os_family,
                                browser_id
                            FROM
                                time_filtered_aggregations
                            ORDER BY
                                browser_id,
                                date DESC
                        ) user_agent_columns
                        ON (summable_metrics_with_outcome_by_browser_id_by_date.browser_id = user_agent_columns.browser_id)
                        WHERE pageview_count > 0
                        -- Merging with outcome dates produces duplicate entries, the following filter deduplicates
                        AND row_number = 1
                        -- We throw away the :moving_window_length potentially incomplete days we've added in the beginning
                        AND summable_metrics_with_outcome_by_browser_id_by_date.date >= :min_date
                '''
queries['upsert_predictions'] = '''
        INSERT INTO
               conversion_predictions_daily (
                   date,
                   browser_id,
                   user_id,
                   predicted_outcome, 
                   conversion_probability, 
                   no_conversion_probability,
                   shared_account_login_probability,
                   model_version,
                   created_at,
                   updated_at)
           VALUES (
               :date,
               :browser_id,
               :user_id,
               :predicted_outcome, 
               :conversion_probability, 
               :no_conversion_probability,
               :shared_account_login_probability,
               :model_version,
               :created_at,
               :updated_at)
           ON CONFLICT
               (browser_id, date)
           DO UPDATE SET
               conversion_probability = :conversion_probability,
               no_conversion_probability = :no_conversion_probability,
               shared_account_login_probability = :shared_account_login_probability,
               model_version = :model_version,
               updated_at = :updated_at
    '''

queries['upsert_prediction_job_log'] = '''
        INSERT INTO
               prediction_job_log (
                   date,
                   rows_predicted,
                   model_version,
                   created_at,
                   updated_at)
           VALUES (
               :date,
               :rows_predicted,
               :model_version,
               :created_at,
               :updated_at)
           ON CONFLICT
               (date, model_version)
           DO UPDATE SET
               rows_predicted = :rows_predicted,
               updated_at = :updated_at
    '''