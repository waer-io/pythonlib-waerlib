import pandas as pd
import json
from .read import read
from .repos_core import coredb_outputs as outputs_repo

def query_data_json(user_id, key, start_datetime, end_datetime, path_to_data):
    """
    the main query function will connect to the database
    this is a local version of the query function which reads from a json file (list of dicts) in the 4-column format


    """

    with open(path_to_data,'r') as f:
        data = json.load(f)

        selection = [record for record in data if \
                     record['user_id']==user_id \
                     and record['key']==key \
                     and (pd.to_datetime(record['timestamp']) >= pd.to_datetime(start_datetime)) \
                     and (pd.to_datetime(record['timestamp']) <= pd.to_datetime(end_datetime))
                    ]

    return selection

def get_data(user_id, start_ts, end_ts, tags, use_gcp = True):
    """
    generalised function to get data for the display
    allows option of using GCP storage (use_gcp = True) or reading from json
    which is easier for testing


    """
    print(f"Getting data for {user_id}, {start_ts}..{end_ts}, {use_gcp}, {tags}", flush=True)

    if use_gcp:
        df = outputs_repo.getAll(user_id, tags, start_ts, end_ts)
        df['timestamp'] = df['timestamp'].astype(str)
        df['val'] = df['val'].apply(lambda x: json.loads(x))
    else:
        path_to_data = './outputs/test_data.json'
        data = query_data_json(user_id, tags, start_ts, end_ts, path_to_data)
        df = pd.DataFrame(data)
    return df




def get_latest_sleep_composite_value(existing_df):
    return _get_latest_composite_value(existing_df[existing_df['key'] == 'sleep_composite'])

def get_latest_activity_composite_value(existing_df):
    return _get_latest_composite_value(existing_df[existing_df['key'] == 'activity_composite'])

def get_latest_fitness_composite_value(existing_df):
    return _get_latest_composite_value(existing_df[existing_df['key'] == 'fitness_composite'])


def _get_latest_composite_value(existing_df):
    df = existing_df

    if len(df) == 0:
        print('_get_latest_composite_value - No data')
        return None

    df = df.sort_values(by = 'timestamp')
    latest_val = df.iloc[-1]['val']
    if 'scaled_outputs' in latest_val and 'scaled_estimate' in latest_val['scaled_outputs'] and 'mu' in latest_val['scaled_outputs']['scaled_estimate']:
        latest_value = latest_val['scaled_outputs']['scaled_estimate']['mu']
        return round(latest_value, 1)
    else:
        print('Required data not found')
        return None


def get_latest_waer_index_value(existing_df):
    df = existing_df[existing_df['key'] == 'waer_index']

    if len(df) == 0:
        print('get_latest_waer_index_value - No data')
        return None

    df = df.sort_values(by = 'timestamp')
    latest_value = df.iloc[-1]['val']['waer_index_5']

    return round(latest_value, 1)


def get_latest_waer_index_tuple(user_id, start_date, end_date):
    tags = ['waer_index']
    df = get_data(user_id, start_date, end_date, tags, use_gcp = True)

    if len(df) == 0:
        print ('get_latest_waer_index_tuple - No data')
        return None
    df = df.sort_values(by = 'timestamp')

    waer_index_5 = df.iloc[-1]['val']['waer_index_5']
    if len(df) > 31:
        waer_index_5_prev = df.iloc[-31]['val']['waer_index_5']
    else:
        waer_index_5_prev = df.iloc[0]['val']['waer_index_5']

    return waer_index_5, waer_index_5_prev


def get_composites_and_filled_data(user_id, start_date, end_date):
    tags = ['sleep_composite','activity_composite','fitness_composite',
           'asleep_minutes_filled','moderate_minutes_filled','resting_heartrate_bpm_filled']
    df = get_data(user_id, start_date, end_date, tags, use_gcp = True)
    data = df.to_dict(orient = 'records')

    return data


def get_composites_data(user_id, start_date, end_date):
    tags = ['sleep_composite','activity_composite','fitness_composite']
    df = get_data(user_id, start_date, end_date, tags, use_gcp = True)
    data = df.to_dict(orient = 'records')

    return data
