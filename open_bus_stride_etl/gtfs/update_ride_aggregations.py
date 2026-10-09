import datetime
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

from sqlalchemy import text

from open_bus_stride_db import db

from .. import common, idempotent_process_gtfs_data


def _process_date_range(from_date, to_date):
    from_date = common.parse_date_str(from_date)
    to_date = common.parse_date_str(to_date, num_days=5)
    if to_date > from_date:
        dt = to_date
        min_dt = from_date
    else:
        dt = from_date
        min_dt = to_date
    print("Processing date range from {} to {}".format(dt, min_dt))
    while dt >= min_dt:
        _process_date(dt, silent=True)
        dt = dt - datetime.timedelta(days=1)


# One set-based statement per date. The previous version walked the date's rides through the
# ORM, lazy-loading each ride's stops and issuing two more lookups per ride - ~60k rides a day
# means a few hundred thousand round trips, enough to leave a day's gtfs_ride.start_time null
# well into the day it describes (2026-10-08: still null at 20:00 Israel time).
# A ride without stops gets nulls, same as before (the LEFT JOINs find nothing).
UPDATE_RIDE_AGGREGATIONS = """
    WITH rides AS (
        SELECT gtfs_ride.id
        FROM gtfs_ride
        JOIN gtfs_route ON gtfs_route.id = gtfs_ride.gtfs_route_id
        WHERE gtfs_route.date = :date
    ), first_stops AS (
        SELECT DISTINCT ON (gtfs_ride_stop.gtfs_ride_id)
            gtfs_ride_stop.gtfs_ride_id, gtfs_ride_stop.id, gtfs_ride_stop.departure_time
        FROM gtfs_ride_stop
        JOIN rides ON rides.id = gtfs_ride_stop.gtfs_ride_id
        ORDER BY gtfs_ride_stop.gtfs_ride_id, gtfs_ride_stop.stop_sequence ASC, gtfs_ride_stop.id ASC
    ), last_stops AS (
        SELECT DISTINCT ON (gtfs_ride_stop.gtfs_ride_id)
            gtfs_ride_stop.gtfs_ride_id, gtfs_ride_stop.id, gtfs_ride_stop.arrival_time
        FROM gtfs_ride_stop
        JOIN rides ON rides.id = gtfs_ride_stop.gtfs_ride_id
        ORDER BY gtfs_ride_stop.gtfs_ride_id, gtfs_ride_stop.stop_sequence DESC, gtfs_ride_stop.id DESC
    ), updated AS (
        UPDATE gtfs_ride
        SET
            first_gtfs_ride_stop_id = first_stops.id,
            last_gtfs_ride_stop_id = last_stops.id,
            start_time = first_stops.departure_time,
            end_time = last_stops.arrival_time
        FROM rides
        LEFT JOIN first_stops ON first_stops.gtfs_ride_id = rides.id
        LEFT JOIN last_stops ON last_stops.gtfs_ride_id = rides.id
        WHERE gtfs_ride.id = rides.id
        RETURNING gtfs_ride.first_gtfs_ride_stop_id, gtfs_ride.last_gtfs_ride_stop_id
    )
    SELECT
        COUNT(*) AS total_rides,
        COUNT(*) FILTER (WHERE first_gtfs_ride_stop_id = last_gtfs_ride_stop_id) AS same_first_last,
        COUNT(*) FILTER (WHERE first_gtfs_ride_stop_id <> last_gtfs_ride_stop_id) AS valid_first_last,
        COUNT(*) FILTER (WHERE first_gtfs_ride_stop_id IS NULL) AS without_first_last
    FROM updated
"""


@db.session_decorator
def _process_date(session: db.Session, date, stats=None, silent=False):
    date = common.parse_date_str(date)
    print("Updating ride aggregations for date {}".format(date))
    if stats is None:
        stats = defaultdict(int)
    row = session.execute(text(UPDATE_RIDE_AGGREGATIONS), {'date': date}).one()
    session.commit()
    stats['total rides'] += row.total_rides
    stats['rides with same first/last stop'] += row.same_first_last
    stats['rides with valid first/last stops'] += row.valid_first_last
    stats['rides without first/last stops'] += row.without_first_last
    if not silent:
        pprint(dict(stats))
    return stats


@db.session_decorator
def _is_date_missing(session, date):
    return session.execute(dedent(f"""
        select case b.cnt when 0 then 0 else a.cnt / b.cnt * 100 end as percent from (
        select count(1) as cnt
        from gtfs_ride, gtfs_route
        where gtfs_ride.gtfs_route_id = gtfs_route.id
        and gtfs_route.date = '{date.strftime("%Y-%m-%d")}'
        and gtfs_ride.start_time is not null
        and gtfs_ride.end_time is not null
    ) a,
    (
        select count(1) as cnt
        from gtfs_ride, gtfs_route
        where gtfs_ride.gtfs_route_id = gtfs_route.id
        and gtfs_route.date = '{date.strftime("%Y-%m-%d")}'
    ) b""")).one().percent < 90


def _process_idempotent(check_missing_dates):
    idempotent_process_gtfs_data.main(
        'stride-etl-gtfs-update-ride-aggregations',
        _process_date,
        _is_date_missing if check_missing_dates else None
    )


def main(date=None, date_to=None, idempotent=False, check_missing_dates=False):
    date = common.parse_None(date)
    date_to = common.parse_None(date_to)
    idempotent = common.parse_None(idempotent)
    if idempotent:
        assert not date and not date_to
        _process_idempotent(check_missing_dates)
    else:
        assert not check_missing_dates
        if date_to is None:
            _process_date(date)
        else:
            _process_date_range(date, date_to)
