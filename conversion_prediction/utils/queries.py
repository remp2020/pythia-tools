import re
import pandas as pd
import sqlalchemy
from sqlalchemy.types import TIMESTAMP, Float
from sqlalchemy.sql.expression import literal, extract
from sqlalchemy import and_
from sqlalchemy import func, case
from sqlalchemy.sql.expression import cast
from datetime import timedelta, datetime
from .db_utils import get_aggregated_browser_days_w_session
from .config import DERIVED_METRICS_CONFIG

aggregated_browser_days, session = get_aggregated_browser_days_w_session()


def get_feature_frame_via_sqlalchemy(
    start_time: datetime,
    end_time: datetime,
    moving_window_length: int=7
):
    full_query = get_full_features_query(
        start_time,
        end_time,
        moving_window_length
    )

    feature_frame = pd.read_sql(full_query.statement,full_query.session.bind)

    return feature_frame


def get_full_features_query(
        start_time: datetime,
        end_time: datetime,
        moving_window_length: int=7
):
    filtered_data = get_filtered_cte(start_time, end_time)

    browser_ids, generated_time_series = get_subqueries_for_non_gapped_time_series(
        filtered_data,
        start_time,
        end_time
    )

    unique_events = get_unique_events_subquery(filtered_data)

    device_information = get_device_information_subquery(filtered_data)

    joined_partial_queries = join_all_partial_queries(
        filtered_data,
        browser_ids,
        generated_time_series,
        unique_events,
        device_information,
        moving_window_length
    )

    filtered_w_derived_metrics = filter_joined_queries_adding_derived_metrics(
        joined_partial_queries,
        start_time
    )

    all_time_delta_columns = add_all_time_delta_columns(
        filtered_w_derived_metrics
    )

    return all_time_delta_columns


def get_filtered_cte(
    start_time: datetime,
    end_time: datetime,
):
    filtered_data = session.query(
        aggregated_browser_days).filter(
        aggregated_browser_days.c['date'] >= cast(start_time, TIMESTAMP),
        aggregated_browser_days.c['date'] <= cast(end_time, TIMESTAMP)
    ).cte(
        name="time_filtered_aggregations"
    )

    return filtered_data


def get_subqueries_for_non_gapped_time_series(
        filtered_data,
        start_time: datetime,
        end_time: datetime,
):
    generated_time_series = session.query(
        func.generate_series(
            start_time,
            end_time,
            timedelta(days=1)
        ).label('date_gap_filler')
    ).subquery()

    browser_ids = session.query(
        filtered_data.c['browser_id'],
        filtered_data.c['user_id']
    ).distinct(filtered_data.c['browser_id'], filtered_data.c['user_id']).subquery()

    return browser_ids, generated_time_series


def get_unique_events_subquery(
        filtered_data
):
    unique_events = session.query(
            filtered_data.c['browser_id'].label('event_browser_id'),
            filtered_data.c['next_event_time'].label('next_event_time_filled'),
            filtered_data.c['next_7_days_event'].label('outcome_filled')
        ).filter(
            filtered_data.c['next_event_time'] != None
        ).group_by(
            filtered_data.c['browser_id'],
            filtered_data.c['next_event_time'],
            filtered_data.c['next_7_days_event']).subquery('unique_events')

    return unique_events


def get_device_information_subquery(
        filtered_data
):
    device_information = session.query(
        case(
            [
                (filtered_data.c['device_brand'] == None,
                 'Desktop')
            ],
            else_= filtered_data.c['device_brand']
        ).label('device_brand').label('device'),
        filtered_data.c['browser_family'].label('browser'),
        filtered_data.c['is_desktop'],
        filtered_data.c['is_mobile'],
        filtered_data.c['os_family'].label('os'),
        filtered_data.c['is_mobile'],
        filtered_data.c['browser_id']
    ).order_by(
        filtered_data.c['browser_id'],
        filtered_data.c['date'].desc()
    ).distinct(
        filtered_data.c['browser_id'],
    ).subquery('device_information')

    return device_information


def create_rolling_agg_function(
    moving_window_length: int,
    half_window: bool,
    agg_function,
    column,
    partitioning_column,
    ordering_column
):
    window_look_back_adjustment = 1 if moving_window_length % 2 > 0 or half_window is False else 0
    half_window_adjustment = 2 if half_window else 1
    lower_bound = int(-1 * (moving_window_length / half_window_adjustment - window_look_back_adjustment))
    result_function = agg_function(column).over(
        partition_by=partitioning_column,
        order_by=ordering_column,
        rows=(lower_bound, 0)
    )

    return result_function


def join_all_partial_queries(
        filtered_data,
        browser_ids,
        generated_time_series,
        unique_events,
        device_information,
        moving_window_length: int
):
    # {name of the resulting column : source / calculation}
    column_source_to_name_mapping = {
        'pageview': filtered_data.c['pageviews'],
        'timespent': filtered_data.c['timespent'],
        'direct_visit': filtered_data.c['sessions_without_ref'],
        'visit': filtered_data.c['sessions'],
        'days_active': case(
            [
                (
                    filtered_data.c['date'] == None,
                    0
                )
            ],
            else_= 1)
    }
    # {naming suffix : related parameter for determining part of full window}
    rolling_agg_variants = {
        'count': False,
        'count_last_window_half': True
    }
    # this generates all basic rolling sum columns for both full and second half of the window
    rolling_agg_columns = [
        create_rolling_agg_function(
            moving_window_length,
            is_half_window,
            func.sum,
            column_source,
            browser_ids.c['browser_id'],
            generated_time_series.c['date_gap_filler']
        ).cast(Float).label(f'{column_name}_{suffix}')
        for column_name, column_source  in column_source_to_name_mapping.items()
        for suffix, is_half_window in rolling_agg_variants.items()
    ]

    joined_partial_queries = session.query(
        browser_ids.c['browser_id'].label('browser_id'),
        browser_ids.c['user_id'].label('user_id'),
        generated_time_series.c['date_gap_filler'].label('date'),
        *rolling_agg_columns,
        (filtered_data.c['pageviews'] > 0.0).label('is_active_on_date'),
        extract('day',
            # last day in the current window
            create_rolling_agg_function(
                moving_window_length,
                False,
                func.max,
                generated_time_series.c['date_gap_filler'],
                browser_ids.c['browser_id'],
                generated_time_series.c['date_gap_filler'])
            # last day active in current window
            - create_rolling_agg_function(
                moving_window_length,
                False,
                func.max,
                filtered_data.c['date'],
                browser_ids.c['browser_id'],
                generated_time_series.c['date_gap_filler'])
        ).label('days_since_last_active'),
        unique_events.c['outcome_filled'],
        filtered_data.c['next_7_days_event'].label('outcome_original'),
        # unpack all device information columns except ones already present in other queries
        *[device_information.c[column.name] for column in device_information.columns if column.name != 'browser_id'],
        # row number in case deduplication is needed
        func.row_number().over(
            partition_by=[
                browser_ids.c['browser_id'],
                generated_time_series.c['date_gap_filler']],
            order_by=[
                browser_ids.c['browser_id'],
                generated_time_series.c['date_gap_filler']]
        ).label('row_number')
    ).outerjoin(
        generated_time_series, literal(True)
    ).outerjoin(
        filtered_data,
        and_(
            browser_ids.c['browser_id'] == filtered_data.c['browser_id'],
            generated_time_series.c['date_gap_filler'] == filtered_data.c['date'])
     ).outerjoin(
        unique_events,
        and_(
            unique_events.c['event_browser_id'] == filtered_data.c['browser_id'],
            unique_events.c['next_event_time_filled'] > filtered_data.c['date'] - timedelta(days=7),
            filtered_data.c['date'] <= unique_events.c['next_event_time_filled']
        ),
    ).outerjoin(
        device_information,
        device_information.c['browser_id'] == browser_ids.c['browser_id']
    ).subquery()

    return joined_partial_queries


def filter_joined_queries_adding_derived_metrics(
    joined_partial_queries,
    start_time: datetime
):
    filtered_w_derived_metrics = session.query(
        *[column.label(re.sub('timespent_count', 'timespent_sum', column.name))
          for column in joined_partial_queries.columns
          if not re.search('outcome', column.name)
          and column.name != 'row_number'],
        case(
            [
                (joined_partial_queries.c['is_active_on_date'] == True,
                 joined_partial_queries.c['outcome_original']),
                (joined_partial_queries.c['is_active_on_date'] == False,
                 joined_partial_queries.c['outcome_filled']),
            ],
            else_='no_conversion'
        ).label('outcome'),
        *[
            func.coalesce((
                joined_partial_queries.c[DERIVED_METRICS_CONFIG[key]['nominator'] + suffix] /
                joined_partial_queries.c[DERIVED_METRICS_CONFIG[key]['denominator'] + suffix]
            ), 0.0).label(key + suffix)
            for key in DERIVED_METRICS_CONFIG.keys()
            for suffix in ['', '_last_window_half']
        ]
    ).filter(
        and_(
            joined_partial_queries.c['pageview_count'] > 0,
            joined_partial_queries.c['date'] >= start_time + timedelta(days=7),
            joined_partial_queries.c['row_number'] == 1)
    ).subquery()

    return filtered_w_derived_metrics


def add_all_time_delta_columns(
        filtered_w_derived_metrics
):
    all_time_delta_columns = session.query(
        filtered_w_derived_metrics,
        *[
            (
                filtered_w_derived_metrics.c[re.sub('_last_window_half', '', column.name)] - filtered_w_derived_metrics.c[column.name]
            ).label(re.sub('_last_window_half', '_first_window_half', column.name))
            for column in filtered_w_derived_metrics.columns
            if re.search('_last_window_half', column.name)
        ],
        *[
            case(
                    [
                        # If the 2nd half of period has 0 for its value, we assign 100 % decline
                        (filtered_w_derived_metrics.c[column.name] == 0, -1),
                        # If the 1st half of period has 0 for its value, we assign 100 % growth
                        (
                            filtered_w_derived_metrics.c[re.sub('_last_window_half', '', column.name)]
                            - filtered_w_derived_metrics.c[column.name] == 0, 1)
                    ],
                else_=(filtered_w_derived_metrics.c[column.name] / (
                    filtered_w_derived_metrics.c[re.sub('_last_window_half', '', column.name)] -
                    filtered_w_derived_metrics.c[column.name]))
            ).label(f'relative_{re.sub("_last_window_half", "", column.name)}_change_first_and_second_half')
            for column in filtered_w_derived_metrics.columns
            if re.search('_last_window_half', column.name)
         ]
    )

    return all_time_delta_columns


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
                        AND date <= :max_date)
                SELECT
                  *,
                  -- calculate change between windows for derived metrics
                  CASE
                    WHEN pageviews_per_visit_last_window_half = 0 THEN -1
                    WHEN pageviews_per_visit_first_window_half = 0 THEN 1
                    ELSE pageviews_per_visit_last_window_half / pageviews_per_visit_first_window_half
                  END AS relative_pageviews_per_visit_change_first_and_second_half,
                  CASE
                    WHEN visits_per_day_active_last_window_half = 0 THEN -1
                    WHEN visits_per_day_active_first_window_half = 0 THEN 1
                    ELSE visits_per_day_active_last_window_half / visits_per_day_active_first_window_half
                  END AS relative_visits_per_day_active_change_first_and_second_half,
                  CASE
                    WHEN direct_visits_share_last_window_half = 0 THEN -1
                    WHEN direct_visits_share_first_window_half = 0 THEN 1
                    ELSE direct_visits_share_last_window_half / direct_visits_share_first_window_half
                  END AS relative_direct_visits_share_change_first_and_second_half,
                  CASE
                    WHEN timespent_per_visit_last_window_half = 0 THEN -1
                    WHEN timespent_per_visit_first_window_half = 0 THEN 1
                    ELSE timespent_per_visit_last_window_half / timespent_per_visit_first_window_half
                  END AS relative_timespent_per_visit_change_first_and_second_half,
                  CASE
                    WHEN timespent_per_pageview_last_window_half = 0 THEN -1
                    WHEN timespent_per_pageview_first_window_half = 0 THEN 1
                    ELSE timespent_per_pageview_last_window_half / timespent_per_pageview_first_window_half
                  END AS relative_timespent_per_pageview_change_first_and_second_half
                FROM (
                  SELECT
                    *,
                    -- calculate change between windows for base metrics
                    CASE
                      WHEN pageview_count_last_window_half = 0 THEN -1
                      WHEN pageview_count_first_window_half = 0 THEN 1
                      ELSE pageview_count_last_window_half / pageview_count_first_window_half
                    END AS relative_pageview_count_change_first_and_second_half,
                    CASE
                      WHEN timespent_sum_last_window_half = 0 THEN -1
                      WHEN timespent_sum_first_window_half = 0 THEN 1
                      ELSE timespent_sum_last_window_half / timespent_sum_first_window_half
                    END AS relative_timespent_sum_change_first_and_second_half,
                    CASE
                      WHEN direct_visit_count_last_window_half = 0 THEN -1
                      WHEN direct_visit_count_first_window_half = 0 THEN 1
                      ELSE direct_visit_count_last_window_half / direct_visit_count_first_window_half
                    END AS relative_direct_visit_count_change_first_and_second_half,
                    CASE
                      WHEN visit_count_last_window_half = 0 THEN -1
                      WHEN visit_count_first_window_half = 0 THEN 1
                      ELSE visit_count_last_window_half / visit_count_first_window_half
                    END AS relative_visit_count_change_first_and_second_half,
                    CASE
                      WHEN days_active_count_last_window_half = 0 THEN -1
                      WHEN days_active_count_first_window_half = 0 THEN 1
                      ELSE days_active_count_last_window_half / days_active_count_first_window_half
                    END AS relative_days_active_count_change_first_and_second_half,
                    -- derived metrics first / second window half values calculations
                    CASE
                      WHEN visit_count_first_window_half = 0 THEN 0
                      ELSE pageview_count_first_window_half / visit_count_first_window_half
                    END AS pageviews_per_visit_first_window_half,
                    CASE
                      WHEN visit_count_last_window_half = 0 THEN 0
                      ELSE pageview_count_last_window_half / visit_count_last_window_half
                    END AS pageviews_per_visit_last_window_half,
                    CASE
                      WHEN days_active_count_first_window_half = 0 THEN 0
                      ELSE visit_count_first_window_half / days_active_count_first_window_half
                    END AS visits_per_day_active_first_window_half,
                    CASE
                      WHEN days_active_count_last_window_half = 0 THEN 0
                      ELSE visit_count_last_window_half / days_active_count_last_window_half
                    END AS visits_per_day_active_last_window_half,
                    CASE
                      WHEN visit_count_first_window_half = 0 THEN 0
                      ELSE direct_visit_count_first_window_half / visit_count_first_window_half
                    END AS direct_visits_share_first_window_half,
                    CASE
                      WHEN visit_count_last_window_half = 0 THEN 0
                      ELSE direct_visit_count_last_window_half / visit_count_last_window_half
                    END AS direct_visits_share_last_window_half,
                    CASE
                      WHEN visit_count_first_window_half = 0 THEN 0
                      ELSE timespent_sum_first_window_half / visit_count_first_window_half
                    END AS timespent_per_visit_first_window_half,
                    CASE
                      WHEN visit_count_last_window_half = 0 THEN 0
                      ELSE timespent_sum_last_window_half / visit_count_last_window_half
                    END AS timespent_per_visit_last_window_half,
                    CASE
                      WHEN pageview_count_first_window_half = 0 THEN 0
                      ELSE timespent_sum_first_window_half / pageview_count_first_window_half
                    END AS timespent_per_pageview_first_window_half,
                    CASE
                      WHEN pageview_count_last_window_half = 0 THEN 0
                      ELSE timespent_sum_last_window_half / pageview_count_last_window_half
                    END AS timespent_per_pageview_last_window_half
                  FROM (
                  SELECT
                      summable_metrics_with_outcome_by_browser_id_by_date.date AS date,
                      summable_metrics_with_outcome_by_browser_id_by_date.browser_id AS browser_id,
                      summable_metrics_with_outcome_by_browser_id_by_date.user_id AS user_id,
                      pageview_count,
                      pageview_count_last_window_half,
                      pageview_count - pageview_count_last_window_half AS pageview_count_first_window_half,
                      timespent_sum,
                      timespent_sum_last_window_half,
                      timespent_sum - timespent_sum_last_window_half AS timespent_sum_first_window_half,
                      direct_visit_count,
                      direct_visit_count_last_window_half,
                      direct_visit_count - direct_visit_count_last_window_half AS direct_visit_count_first_window_half,
                      visit_count,
                      visit_count_last_window_half,
                      visit_count - visit_count_last_window_half AS visit_count_first_window_half,
                      days_active_count,
                      days_active_count_last_window_half,
                      days_active_count - days_active_count_last_window_half AS days_active_count_first_window_half,
                      pageview_count / visit_count AS pageviews_per_visit,
                      visit_count / days_active_count AS visits_per_day_active,
                      direct_visit_count / visit_count AS direct_visits_share,
                      timespent_sum / visit_count AS timespent_per_visit,
                      timespent_sum / pageview_count AS timespent_per_pageview,
                      days_since_last_active,
                      device_brand AS device,
                      browser_family AS browser,
                      is_desktop,
                      is_mobile,
                      os_family AS os,
                      CASE
                        WHEN active_on_date THEN outcome_original
                        WHEN NOT active_on_date AND outcome_filled IS NOT NULL THEN outcome_filled
                        ELSE 'no conversion'
                      END AS outcome,
                      active_on_date AS is_active_on_date
                  FROM (
                      -- extract sums of metrics for each browser within the "last 7 days window" per each day
                      -- first day of selected date range includes only its own data (no pre :min_date data are loaded)
                      SELECT
                          date_gap_filler AS date,
                          active_browser_id AS browser_id,
                          active_user_id AS user_id,
                          -- These are calculation of base metric window aggregation for first half of the window
                          -- In case the window has an uneven number of days, first half of the window is the longer one
                          CAST(
                              CASE
                              WHEN :moving_window_length % 2 = 0 THEN SUM(pageviews) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length / 2 PRECEDING AND CURRENT ROW)
                              ELSE SUM(pageviews) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length / 2 - 1 PRECEDING AND CURRENT ROW)
                            END AS FLOAT) AS pageview_count_last_window_half,
                          CAST(SUM(pageviews) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW) AS FLOAT) AS pageview_count,
                          CAST(
                              CASE
                              WHEN :moving_window_length % 2 = 0 THEN SUM(timespent) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length / 2 PRECEDING AND CURRENT ROW)
                              ELSE SUM(timespent) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length / 2 - 1 PRECEDING AND CURRENT ROW)
                            END AS FLOAT) AS timespent_sum_last_window_half,
                          CAST(SUM(timespent) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW) AS FLOAT) AS timespent_sum,
                          CAST(SUM(sessions_without_ref) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW) AS FLOAT) AS direct_visit_count,
                          CAST(
                              CASE
                              WHEN :moving_window_length % 2 = 0 THEN SUM(sessions_without_ref) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length / 2 PRECEDING AND CURRENT ROW)
                              ELSE SUM(sessions_without_ref) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length / 2 - 1 PRECEDING AND CURRENT ROW)
                            END AS FLOAT) AS direct_visit_count_last_window_half,
                          CAST(SUM(sessions) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW) AS FLOAT) AS visit_count,
                          CAST(
                              CASE
                              WHEN :moving_window_length % 2 = 0 THEN SUM(sessions) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length / 2 PRECEDING AND CURRENT ROW)
                              ELSE SUM(sessions) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length / 2 - 1 PRECEDING AND CURRENT ROW)
                            END AS FLOAT) AS visit_count_last_window_half,
                          CAST(SUM(
                              CASE
                                  WHEN date IS NULL THEN 0
                                  ELSE 1
                              END) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW) AS FLOAT) AS days_active_count,
                          CAST(SUM(
                                CASE
                                    WHEN date IS NULL THEN 0
                                    ELSE 1
                                END) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length / 2 - 1 PRECEDING AND CURRENT ROW) AS FLOAT) AS days_active_count_last_window_half,
                          CASE
                              WHEN pageviews > 0 THEN True
                              ELSE False
                          END AS active_on_date,
                          CAST(
                            MAX(date_gap_filler) OVER (PARTITION BY active_browser_id ORDER BY date_gap_filler ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW)
                            - MAX(date) OVER (PARTITION BY active_browser_id ORDER BY active_browser_id ROWS BETWEEN :moving_window_length - 1 PRECEDING AND CURRENT ROW)
                            AS FLOAT) AS days_since_last_active,
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
                          LEFT JOIN (
                            SELECT
                                date AS presence_date,
                                browser_id
                            FROM
                                time_filtered_aggregations) presence_dates
                            ON (all_date_browsers_combinations.browser_id = presence_dates.browser_id
                                AND all_date_browsers_combinations.date = presence_dates.presence_date)
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
                       ) precalculated_window_parts
                  ) precalculated_derived_metrics_window_parts
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