import pandas as pd
import json
from .read import read

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

def get_data(user_id, start_date, end_date, tags, use_gcp = True):
    """
    generalised function to get data for the display
    allows option of using GCP storage (use_gcp = True) or reading from json
    which is easier for testing


    """
    if use_gcp:
        df = read(user_id, start_date, end_date, tags, 'outputs')
        df['timestamp'] = df['timestamp'].astype(str)
        df['val'] = df['val'].apply(lambda x: json.loads(x))
    else:
        path_to_data = './outputs/test_data.json'
        data = query_data_json(user_id, tags, start_date, end_date, path_to_data)
        df = pd.DataFrame(data)
    return df




def get_latest_sleep_composite_value(user_id, start_date, end_date):
    return get_latest_composite_value(user_id, start_date, end_date, 'sleep_composite')

def get_latest_activity_composite_value(user_id, start_date, end_date):
    return get_latest_composite_value(user_id, start_date, end_date, 'activity_composite')

def get_latest_fitness_composite_value(user_id, start_date, end_date):
    return get_latest_composite_value(user_id, start_date, end_date, 'fitness_composite')

def get_latest_composite_value(user_id, start_date, end_date, tag):
    tags = [tag]
    df = get_data(user_id, start_date, end_date, tags, use_gcp = True)

    if len(df) == 0:
        print ('No data')
        return None

    df = df.sort_values(by = 'timestamp')
    latest_value = df.iloc[-1]['val'].apply(lambda x: x['scaled_outputs']['scaled_estimate']['mu'])

    return round(latest_value, 1)


def get_latest_waer_index_value(user_id, start_date, end_date):
    tags = ['waer_index']
    df = get_data(user_id, start_date, end_date, tags, use_gcp = True)

    if len(df) == 0:
        print ('No data')
        return None
    df = df.sort_values(by = 'timestamp')

    return df.iloc[-1]['val']['waer_index_5']


def get_latest_waer_index_tuple(user_id, start_date, end_date):
    tags = ['waer_index']
    df = get_data(user_id, start_date, end_date, tags, use_gcp = True)

    if len(df) == 0:
        print ('No data')
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
