import json
from datetime import datetime, timedelta, date
import pandas as pd
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


    # from outputs - seems we use lists everywhere internally.
    # path_to_data - irrelevant, but here to follow the same signature as _sqlite, _json, _waerlib
    def write_outputs_postgres(outputs, path_to_data):
        print(f"------------------ write_outputs_postgres - writing outputs ------------------ ")
        df = outputs
        if not isinstance(outputs, pd.DataFrame):
            df = pd.DataFrame(outputs)

        print("... write_outputs_postgres. columns")
        print(df.columns)
        print("... write_outputs_postgres. unique keys to be written")
        print(df['key'].unique())

        df['timestamp'] = waer_utility._ensure_nanos_ts(df)
        df['val'] = df['val'].apply(json.dumps)
        df['version'] = df['version'].apply(waer_utility._make_unique_version)
        df['user_id'] = df['user_id'].apply(waer_utility.wrap_user_id_prefix_if_not)


        print(df.head())

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        outputs_repo.insertBatched(df)



    # to outputs - seems we use lists everywhere internally.
    # path_to_data - irrelevant, but here to follow the same signature as _sqlite, _json, _waerlib
    def query_outputs_postgres(user_id, key_or_keys, start_datetime, end_datetime, path_to_data):
        start_timestamp = waer_utility.make_nanos(start_datetime)
        end_timestamp = waer_utility.make_nanos(end_datetime)
        keys = waer_utility._to_coll_if_not(key_or_keys)
        user_id_wrapped = waer_utility.wrap_user_id_prefix_if_not(user_id)

        print(f"query_outputs_postgres - {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")

        df = outputs_repo.getAll(user_id_wrapped, keys, start_timestamp, end_timestamp)

        if df.empty:
            print(f"query_outputs_postgres - No results for {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")
            return []
        print(df.head())

        df['timestamp'] = waer_utility._ensure_nanos_ts(df)

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        return df.to_dict(orient='records')



    # path_to_data - irrelevant, but here to follow the same signature as _sqlite, _json, _waerlib
    def write_parsed_postgres(parsed, path_to_data):
        # THIS IS NEVER USED IN MODEL!
        # this is used in ingest
        print(f"------------------ write_parsed_postgres - writing parsed ------------------ ")
        df = parsed
        if not isinstance(parsed, pd.DataFrame):
            df = pd.DataFrame(parsed)

        print("... write_parsed_postgres. columns")
        print(df.columns)
        print("... write_parsed_postgres. unique keys to be written")
        print(df['key'].unique())

        df['timestamp'] = waer_utility._ensure_nanos_ts(df)
        df['val'] = df['val'].apply(json.dumps)
        df['version'] = df['version'].apply(waer_utility._make_unique_version)
        df['user_id'] = df['user_id'].apply(waer_utility.wrap_user_id_prefix_if_not)

        print(df.head())

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        parsed_repo.insertBatched(df)



    # path_to_data - irrelevant, but here to follow the same signature as _sqlite, _json, _waerlib
    def query_parsed_postgres(user_id, key_or_keys, start_datetime, end_datetime, path_to_data):
        start_timestamp = waer_utility.make_nanos(start_datetime)
        end_timestamp = waer_utility.make_nanos(end_datetime)
        keys = waer_utility._to_coll_if_not(key_or_keys)
        user_id_wrapped = waer_utility.wrap_user_id_prefix_if_not(user_id)

        print(f"query_parsed_postgres - {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")

        df = parsed_repo.getAll(user_id_wrapped, keys, start_timestamp, end_timestamp)

        if df.empty:
            print(f"query_parsed_postgres - No results for {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")
            return []
        print(df.head())

        df['timestamp'] = waer_utility._ensure_nanos_ts(df)

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        return df.to_dict(orient='records')



    # path_to_data - irrelevant, but here to follow the same signature as _sqlite, _json, _waerlib
    def write_samples_postgres(samples, path_to_data):
        # THIS IS NEVER USED IN MODEL!
        # this is used in ingest
        print(f"------------------ write_samples_postgres - writing samples ------------------ ")
        df = samples
        if not isinstance(samples, pd.DataFrame):
            df = pd.DataFrame(samples)

        print("... write_samples_postgres. columns")
        print(df.columns)
        print("... write_samples_postgres. unique keys to be written")
        print(df['key'].unique())

        df['timestamp'] = waer_utility._ensure_nanos_ts(df)
        df['val'] = df['val'].apply(json.dumps)
        df['version'] = df['version'].apply(waer_utility._make_unique_version)
        df['user_id'] = df['user_id'].apply(waer_utility.wrap_user_id_prefix_if_not)

        print(df.head())

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        samples_repo.insertBatched(df)



    # path_to_data - irrelevant, but here to follow the same signature as _sqlite, _json, _waerlib
    def query_samples_postgres(user_id, key_or_keys, start_datetime, end_datetime, path_to_data):
        start_timestamp = waer_utility.make_nanos(start_datetime)
        end_timestamp = waer_utility.make_nanos(end_datetime)
        keys = waer_utility._to_coll_if_not(key_or_keys)
        user_id_wrapped = waer_utility.wrap_user_id_prefix_if_not(user_id)

        print(f"query_samples_postgres - {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")

        df = samples_repo.getAll(user_id_wrapped, keys, start_timestamp, end_timestamp)

        if df.empty:
            print(f"query_samples_postgres - No results for {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")
            return []
        print(df.head())

        df['timestamp'] = waer_utility._ensure_nanos_ts(df)

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        return df.to_dict(orient='records')




    # path_to_data - irrelevant, but here to follow the same signature as _sqlite, _json, _waerlib
    def write_profiles_postgres(profiles, path_to_data):
        # THIS IS NEVER USED IN MODEL!
        # this is used in ingest
        print(f"------------------ write_profiles_postgres - writing profiles ------------------ ")
        df = profiles
        if not isinstance(profiles, pd.DataFrame):
            df = pd.DataFrame(profiles)

        print("... write_profiles_postgres. columns")
        print(df.columns)
        print("... write_profiles_postgres. unique keys to be written")
        print(df['key'].unique())

        df['timestamp'] = waer_utility._ensure_nanos_ts(df)
        df['val'] = df['val'].apply(json.dumps)
        df['version'] = df['version'].apply(waer_utility._make_unique_version)
        df['user_id'] = df['user_id'].apply(waer_utility.wrap_user_id_prefix_if_not)

        print(df.head())

        df['timestamp'].apply(waer_time_util.enforce_nanos) # one more sanity check that we really are storing-querying nanos before providing it through this layer
        profiles_repo.insertBatched(df)

    # path_to_data - irrelevant, but here to follow the same signature as _sqlite, _json, _waerlib
    def query_profiles_postgres(user_id, key_or_keys, start_datetime, end_datetime, path_to_data):
        start_timestamp = waer_utility.make_nanos(start_datetime)
        end_timestamp = waer_utility.make_nanos(end_datetime)
        keys = waer_utility._to_coll_if_not(key_or_keys)
        user_id_wrapped = waer_utility.wrap_user_id_prefix_if_not(user_id)

        print(f"query_profiles_postgres - {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")

        df = profiles_repo.getAll(user_id_wrapped, keys, start_timestamp, end_timestamp)

        if df.empty:
            print(f"query_profiles_postgres - No results for {user_id_wrapped}, {keys}, {start_timestamp}..{end_timestamp} ({start_datetime}..{end_datetime})")
            return []
        print(df.head())

        df['timestamp'] = waer_utility._ensure_nanos_ts(df)

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
