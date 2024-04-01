
from datetime import date, datetime, timedelta
import pandas as pd
from pandas import Timestamp
import numpy as np


date_format = '%Y-%m-%d'
datetime_format = '%Y-%m-%d %H:%M:%S'


class waer_time_util:

    # a passthrough for all time instances
    def get(time):

        out = time

        if isinstance(out, np.int64):
            out = int(out)

        if isinstance(out, np.float64):
            out = int(out)

        if isinstance(out, float):
            out = int(out)

        if isinstance(out, date):
            out = waer_time_util.convert_date_to_nanos(out)

        if not isinstance(out, int):
            out = waer_time_util.make_nanos(out)

        if not isinstance(out, int):
            print(f"----------------- UNEXPECTED ----------------- was instance for {out} of {type(out)}", flush=True)

        return out

    def get_default_earliest():
        return waer_time_util.make_nanos('2023-01-01')

    # takes a date or datetime object
    # converts to date string
    def format_date_to_string(date_obj):
        return date_obj.strftime(date_format)

    def get_age(date, dob):
        return (datetime.combine(date, datetime.min.time()) - datetime.strptime(dob,date_format)).days/365

    def get_tomorrow():
        now = datetime.now()
        start_of_tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_of_tomorrow_ns = int(start_of_tomorrow.timestamp() * 1e9)

        return start_of_tomorrow_ns

    # if using this for nanos instead, then date_str actually is nanos int and format can be removed
    def subtract_n_days_from_datestring(date_str, n_days, format=date_format):
        date_nanos = waer_time_util.make_nanos(date_str) # make sure its nanos
        nanos_per_day = 24 * 60 * 60 * 1_000_000_000  # 24h x 60min x 60sec x 1B nanos/sec.
        days_in_nanos = n_days * nanos_per_day

        return date_nanos - days_in_nanos

    def convert_date_to_nanos(date_in):
        if not isinstance(date_in, date):
            raise TypeError(f"Not date instance: {date_in} is a {type(date_in)}")

        datestr = str(date_in)[:10]
        date_time = datetime.strptime(datestr, date_format)
        timestamp_in_seconds = date_time.timestamp()
        timestamp_in_nanoseconds = int(timestamp_in_seconds * 1_000_000_000)
        return timestamp_in_nanoseconds

    def get_date_list_as_string(start_date, end_date):
        datelist = waer_time_util.get_date_list(start_date, end_date)
        return [datetime.strftime(date, date_format) for date in datelist]

    def get_date_list(start_date, end_date, local=False):
        """
        start_date and end_date are date strings with format "%Y-%m-%d"
        returns a list of dates

        """

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        if local:
            # added day because the list will otherwise start one day too late and end one day too early.
            # assuming we have inclusive ending date. remove that part if not.
            start_dt = start_dt + pd.Timedelta(days=1)
            end_dt = end_dt + pd.Timedelta(days=1)



        L = pd.date_range(start_dt, end_dt, freq='d')
        date_list = [dt.date() for dt in L]

        return date_list

    def get_df_ts_as_date_blind(df):
        return df['timestamp'].astype(str).str[:10]


    def get_df_ts_nanos_as_date(df):
        return df['timestamp'].apply(waer_time_util.nanos_to_date)


    def get_ts_as_date(record_ts):
        return waer_time_util.nanos_to_date(record_ts)

    def nanos_to_date_year_month_string(nanoseconds):
        return str(waer_time_util.nanos_to_date(nanoseconds))[:7] # gets the YYYY-MM part.

    def nanos_to_date(nanoseconds):

        ensure_nanos = waer_time_util.make_nanos(nanoseconds)

        seconds = ensure_nanos / 1_000_000_000
        epoch = datetime(1970, 1, 1)
        date_time = epoch + timedelta(seconds=seconds)
        date_part = date_time.date()
        return date_part

    # for some reason coming in as ms somewhere. make_nanos fixes it
    def get_df_ts_as_int(df):
        return df['timestamp'].apply(waer_time_util.make_nanos)

    def is_df_ts_col_right_type(df):
        return pd.api.types.is_datetime64_any_dtype(df['timestamp'].dtype)

    def date_equal(dt1_in, dt2_in):
        dt1 = dt1_in
        dt2 = dt2_in

        if isinstance(dt1, float):
            dt1 = int(dt1)

        if isinstance(dt1, str):
            dt1 = waer_time_util.make_nanos(dt1)

        if isinstance(dt1, date):
            dt1 = waer_time_util.convert_date_to_nanos(dt1)

        if isinstance(dt2, date):
            dt2 = waer_time_util.convert_date_to_nanos(dt2)

        if isinstance(dt2, str):
            dt2 = waer_time_util.make_nanos(dt2)

        if not isinstance(dt1, int) or not isinstance(dt2, int):
            print(f"-------------------- UNEXPECTED -------------------- comparing equals: {dt1} == {dt2} ({type(dt1)} and {type(dt2)})", flush=True)


        if not len(str(dt1)) == 19 or not len(str(dt2)) == 19:
            print(f"-------------------- UNEXPECTED -------------------- comparing equals lens: {dt1} == {dt2} ({type(dt1)} and {type(dt2)})", flush=True)

        return dt1 == dt2

    def date_in(date_in, dates):
        date_compare = date_in
        dates_compare = dates
        if len(dates_compare) == 0:
            return False

        if isinstance(dates_compare, set):
            dates_compare = list(dates_compare)

        if isinstance(dates_compare[0], str) and isinstance(date_compare, date):
            date_compare = str(date_compare)

        if isinstance(dates_compare[0], str) and isinstance(date_compare, int):
            date_compare = waer_time_util.nanos_to_date(date_compare)
            date_compare = waer_time_util.date_to_string(date_compare)

        if isinstance(dates_compare[0], date) and not isinstance(date_compare, date):
            date_compare = waer_time_util.make_nanos(date_compare)
            date_compare = waer_time_util.nanos_to_date(date_compare)


        return date_compare in dates_compare

    def date_to_string(date_in):
        return date_in.strftime(date_format)

    def get_today_in_hours(df_temp, date):
        df_temp['timestamp_as_datetime'] = pd.to_datetime(df_temp['timestamp'] / 1_000, unit='us')
        df_temp['at_start_of_day'] = df_temp['timestamp_as_datetime'].dt.floor('D')
        df_temp['seconds_in_day'] = (df_temp['timestamp_as_datetime'] - df_temp['at_start_of_day']).dt.total_seconds()

        return df_temp['seconds_in_day'] / 3600


    # SUPER MASTER STANDARDIZER. a master converter and sanity check essentially.
    def make_nanos(n):

        out = n

        try:
            out = int(out)
        except:
            pass

        if isinstance(out, str):
            # if required, converts str to datetime first
            out = waer_time_util._parse_string_dt_to_datetime(out)

        if isinstance(out, Timestamp):
            # if required, converts Timestamp to datetime first.
            out = waer_time_util._get_ts_as_datetime(out)

        if isinstance(out, datetime):
            # if required, converts datetime to nanos
            out = waer_time_util._get_datetime_as_nanos(out)

        if isinstance(out, date):
            out = waer_time_util.convert_date_to_nanos(out)

        # ensure it is actually nanos. length must be 19.
        if len(str(out)) > 19:
            raise TypeError(f"make_nanos - longer than nanos length for {out}, type {type(out)}")
        while len(str(out)) < 19:
            out *= 10
        return out

    def enforce_nanos(timestamp_in):
        if not isinstance(timestamp_in, int):
            raise TypeError(f"\n\n\n--------------------- enforce_nanos - FAILED. {timestamp_in} is not an int, but is {type(timestamp_in)}")
        len_in = len(str(timestamp_in))
        if len_in != 19:
            raise TypeError(f"\n\n\n--------------------- enforce_nanos - FAILED. {timestamp_in} is not in nanos (19), but is {len_in}")

    def _get_ts_as_datetime(record_ts):
        if isinstance(record_ts, Timestamp): # pandas Timestamp
            datetime_out = waer_time_util._format_to_datetime(datetime.utcfromtimestamp(record_ts.timestamp()))
        else:
            datetime_out = waer_time_util._format_to_datetime(datetime.utcfromtimestamp((int(record_ts) / 1_000_000)))
        return datetime_out

    def _get_datetime_as_nanos(datetime_in):
        timestamp_in_seconds = datetime_in.timestamp()
        timestamp_in_nanoseconds = int(timestamp_in_seconds * 1_000_000_000)
        return timestamp_in_nanoseconds

    def _format_to_datetime(dt_utcfromtimestamp):
        return dt_utcfromtimestamp.strftime(datetime_format)

    def _parse_string_dt_to_datetime(dt_string):
        try:
            return datetime.fromisoformat(dt_string)
        except ValueError:
            pass

        formats = [
            datetime_format,
        ]

        for fmt in formats:
            try:
                return datetime.strptime(dt_string, fmt)
            except ValueError:
                pass

        raise ValueError(f'Unknown datetime format: {dt_string}')
