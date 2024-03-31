import json
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
from .waer_time_util import waer_time_util

from .repos_core import coredb_outputs as outputs_repo
from .repos_core import coredb_parsed as parsed_repo
from .repos_core import coredb_profiles as profiles_repo
from .repos_core import coredb_samples as samples_repo



class waer_coredb_util:
    """
        postgres core db utilities.

        also includes the user id wrappers. these also used in model. maybe not necessary, if wrapping handled here. need to test.

        remove the _postgres maybe from the function calls.
    """

    # makes it easy to get out a specific test run results, even for same user, when testing locally
    def _make_unique_version(vers):
        vers = "dev-2024MAR30_6"

        return vers


    def _to_coll_if_not(key_or_keys):
        if not isinstance(key_or_keys, list):
            return [key_or_keys]
        else:
            return key_or_keys


    def _ensure_nanos_ts(df):
        return df['timestamp'].apply(waer_time_util.make_nanos) # for some reason, some seem nanos, others micros and that causes instability in getting the data. too lazy to investigate one by one.


    def _convert_to_json(record_maybe_jsonable):
        # convert python object to json string
        record_native_python = waer_coredb_util.convert_numpy(record_maybe_jsonable)

        return json.dumps(record_native_python)


    def _convert_from_json(record_maybe_jsoned):
        # convert json string to python object
        # we should probably make all val also json, so all vals are just jsons. but not currently.
        # we still run all through here for consistency and to keep this in mind.

        try:
            return json.loads(record_maybe_jsoned)
        except:
            return record_maybe_jsoned


    def convert_numpy(obj):
        # ensure python dicts that have numpy data types are converted to jsonable formats.
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {key: waer_coredb_util.convert_numpy(value) for key, value in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [waer_coredb_util.convert_numpy(item) for item in obj]
        return obj

    # from outputs - seems we use lists everywhere internally.
    def write_outputs_postgres(outputs):
        print(f"------------------ write_outputs_postgres - writing outputs ------------------ ")
        df = waer_coredb_util._ensure_dataframe(outputs)

        print("... write_outputs_postgres. columns")
        print(df.columns)
        print("... write_outputs_postgres. unique keys to be written")
        print(df['key'].unique())

        df['timestamp'] = waer_coredb_util._ensure_nanos_ts(df)
        df['val'] = df['val'].apply(waer_coredb_util._convert_to_json)
        df['version'] = df['version'].apply(waer_coredb_util._make_unique_version)
        df['user_id'] = df['user_id'].apply(waer_coredb_util.wrap_user_id_prefix_if_not)


        if 'id' in df.columns:
            df = df.drop('id', axis=1)

        print(df.head())
#         for value in df['val'].head():
#             print(value)

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        outputs_repo.insertBatched(df)



    # to outputs - seems we use lists everywhere internally.
    def query_outputs_postgres(user_id, key_or_keys, start_datetime, end_datetime):
        start_timestamp = waer_time_util.make_nanos(start_datetime)
        end_timestamp = waer_time_util.make_nanos(end_datetime)
        keys = waer_coredb_util._to_coll_if_not(key_or_keys)
        user_id_wrapped = waer_coredb_util.wrap_user_id_prefix_if_not(user_id)

        print(f"query_outputs_postgres - {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")

        df = outputs_repo.getAll(user_id_wrapped, keys, start_timestamp, end_timestamp)

        if df.empty:
            print(f"query_outputs_postgres - No results for {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")
            return []
        print(df.head())

        df['val'] = df['val'].apply(waer_coredb_util._convert_from_json)
        df['timestamp'] = waer_coredb_util._ensure_nanos_ts(df)

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        return df.to_dict(orient='records')



    def write_parsed_postgres(parsed):
        # THIS IS NEVER USED IN MODEL!
        # this is used in ingest
        print(f"------------------ write_parsed_postgres - writing parsed ------------------ ")
        df = waer_coredb_util._ensure_dataframe(parsed)

        print("... write_parsed_postgres. columns")
        print(df.columns)
        print("... write_parsed_postgres. unique keys to be written")
        print(df['key'].unique())

        df['timestamp'] = waer_coredb_util._ensure_nanos_ts(df)
        df['val'] = df['val'].apply(waer_coredb_util._convert_to_json)
        df['version'] = df['version'].apply(waer_coredb_util._make_unique_version)
        df['user_id'] = df['user_id'].apply(waer_coredb_util.wrap_user_id_prefix_if_not)

        if 'id' in df.columns:
            df = df.drop('id', axis=1)

        print(df.head())

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        parsed_repo.insertBatched(df)



    def query_parsed_postgres(user_id, key_or_keys, start_datetime, end_datetime):
        start_timestamp = waer_time_util.make_nanos(start_datetime)
        end_timestamp = waer_time_util.make_nanos(end_datetime)
        keys = waer_coredb_util._to_coll_if_not(key_or_keys)
        user_id_wrapped = waer_coredb_util.wrap_user_id_prefix_if_not(user_id)

        print(f"query_parsed_postgres - {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")

        df = parsed_repo.getAll(user_id_wrapped, keys, start_timestamp, end_timestamp)

        if df.empty:
            print(f"query_parsed_postgres - No results for {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")
            return []
        print(df.head())

        df['val'] = df['val'].apply(waer_coredb_util._convert_from_json)
        df['timestamp'] = waer_coredb_util._ensure_nanos_ts(df)

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        return df.to_dict(orient='records')



    def write_samples_postgres(samples):
        # THIS IS NEVER USED IN MODEL!
        # this is used in ingest
        print(f"------------------ write_samples_postgres - writing samples ------------------ ")
        df = waer_coredb_util._ensure_dataframe(samples)


        print("... write_samples_postgres. columns")
        print(df.columns)
        print("... write_samples_postgres. unique keys to be written")
        print(df['key'].unique())

        df['timestamp'] = waer_coredb_util._ensure_nanos_ts(df)
        df['val'] = df['val'].apply(waer_coredb_util._convert_to_json)
        df['version'] = df['version'].apply(waer_coredb_util._make_unique_version)
        df['user_id'] = df['user_id'].apply(waer_coredb_util.wrap_user_id_prefix_if_not)

        if 'id' in df.columns:
            df = df.drop('id', axis=1)

        print(df.head())

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        samples_repo.insertBatched(df)



    def query_samples_postgres(user_id, key_or_keys, start_datetime, end_datetime):
        start_timestamp = waer_time_util.make_nanos(start_datetime)
        end_timestamp = waer_time_util.make_nanos(end_datetime)
        keys = waer_coredb_util._to_coll_if_not(key_or_keys)
        user_id_wrapped = waer_coredb_util.wrap_user_id_prefix_if_not(user_id)

        print(f"query_samples_postgres - {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")

        df = samples_repo.getAll(user_id_wrapped, keys, start_timestamp, end_timestamp)

        if df.empty:
            print(f"query_samples_postgres - No results for {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")
            return []
        print(df.head())

        df['val'] = df['val'].apply(waer_coredb_util._convert_from_json)
        df['timestamp'] = waer_coredb_util._ensure_nanos_ts(df)

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        return df.to_dict(orient='records')



    def write_profiles_postgres(profiles):
        # THIS IS NEVER USED IN MODEL!
        # this is used in ingest
        print(f"------------------ write_profiles_postgres - writing profiles ------------------ ")
        df = waer_coredb_util._ensure_dataframe(profiles)

        print("... write_profiles_postgres. columns")
        print(df.columns)
        print("... write_profiles_postgres. unique keys to be written")
        print(df['key'].unique())

        df['timestamp'] = waer_coredb_util._ensure_nanos_ts(df)
        df['val'] = df['val'].apply(waer_coredb_util._convert_to_json)
        df['version'] = df['version'].apply(waer_coredb_util._make_unique_version)
        df['user_id'] = df['user_id'].apply(waer_coredb_util.wrap_user_id_prefix_if_not)

        if 'id' in df.columns:
            df = df.drop('id', axis=1)

        print(df.head())

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        profiles_repo.insertBatched(df)



    def query_profiles_postgres(user_id, key_or_keys, start_datetime, end_datetime):
        start_timestamp = waer_time_util.make_nanos(start_datetime)
        end_timestamp = waer_time_util.make_nanos(end_datetime)
        keys = waer_coredb_util._to_coll_if_not(key_or_keys)
        user_id_wrapped = waer_coredb_util.wrap_user_id_prefix_if_not(user_id)

        print(f"query_profiles_postgres - {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")

        df = profiles_repo.getAll(user_id_wrapped, keys, start_timestamp, end_timestamp)

        if df.empty:
            print(f"query_profiles_postgres - No results for {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")
            return []
        print(df.head())

        df['val'] = df['val'].apply(waer_coredb_util._convert_from_json)
        df['timestamp'] = waer_coredb_util._ensure_nanos_ts(df)

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        return df.to_dict(orient='records')


    def wrap_user_id_prefix_if_not(user_id):
        if user_id.startswith("user_id="):
            return user_id
        else:
            return "user_id=" + user_id


    def wrap_user_id_prefix_guided_if_not(user_id, guiding_user_id):
        if user_id.startswith("user_id=") and guiding_user_id.startswith('user_id='):
            return user_id
        else:
            return "user_id=" + user_id


    def _ensure_dataframe(input_collection):
        if isinstance(input_collection, pd.DataFrame):
            return input_collection
        elif isinstance(input_collection, list):
            return pd.DataFrame(input_collection)
        else:
            print(f"----------- UNEXPECTED ---------- {type(input_collection)} is not a list or a df. Trying to wrap in df anyway.")
            return pd.DataFrame(input_collection)