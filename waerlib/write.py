import os
import uuid
import json
import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
from google.cloud import storage
from datetime import datetime
import pyarrow.flight as flight


tag_write = "[WAERLIB: write]"

def validate_df(df):
    assert 'timestamp' in df.columns, 'Column "timestamp" not in df'
    assert 'key' in df.columns, 'Column "key" not in df'
    assert 'val' in df.columns, 'Column "val" not in df'
    assert 'version' in df.columns, 'Column "version" not in df'
    assert len(df.columns) == 4, 'Invalid columns in df'

    df.loc[:,'timestamp'] = pd.to_datetime(df.loc[:,'timestamp'])
    df.loc[:,'key'] = df.loc[:,'key'].astype(str)
    df.loc[:,'val'] = df.loc[:,'val'].astype(str)
    df.loc[:,'version'] = df.loc[:,'version'].astype(str)
    df.loc[:,'month'] = df.loc[:,'timestamp'].astype(str).astype(str).str.slice(0,7)
    df.loc[:,'timestamp'] = df.loc[:,'timestamp'].astype('datetime64[us]').astype(int)
    
    return df

def write(user_id, df, folder):
    df = validate_df(df)
    df.loc[:,'user_id'] = user_id
    df = df.set_index('timestamp')
    pq.write_to_dataset(
        pa.Table.from_pandas(df),
        root_path=f"{os.environ['GCP_BUCKET_NAME']}/{folder}",
        partition_cols=['user_id', 'month'],
        filesystem=fs.GcsFileSystem()
    )

    # in testing. unable to move it here from before read. doesn't complete as request timeouts i guess, at the authenticate token. maybe same ssl issue. it can take like 8 minutes for samples to be refreshed
    # refresh_collections(collections = [folder])

def store_raw(data):
    storage_client = storage.Client(project=os.environ['GCP_PROJECT_ID'])
    bucket = storage_client.get_bucket(os.environ['GCP_BUCKET_NAME'])

    fname = str(uuid.uuid4())
    if data.get('user') and data['user'].get('user_id'):
        fname = 'terraUID-' + data['user']['user_id'] + '~' + fname
    fname = 'raw/' + fname
    
    blob = bucket.blob(fname)
    with blob.open(mode='w') as f:
        f.write(json.dumps(data))


gcs_filesystem_reused = None
def write_with_reuse_client(user_id, df, folder):
    global gcs_filesystem_reused
    if gcs_filesystem_reused is None:
        gcs_filesystem_reused = fs.GcsFileSystem()
    df = validate_df(df)
    df.loc[:,'user_id'] = user_id
    df = df.set_index('timestamp')
    pq.write_to_dataset(
        pa.Table.from_pandas(df),
        root_path=f"{os.environ['GCP_BUCKET_NAME']}/{folder}",
        partition_cols=['user_id', 'month'],
        filesystem=gcs_filesystem_reused
    )

    # in testing. unable to move it here from before read. doesn't complete as request timeouts i guess, at the authenticate token. maybe same ssl issue. it can take like 8 minutes for samples to be refreshed
    #refresh_collections(collections = [folder])

storage_client_reused = None
def store_raw_with_reuse_client(data, user_id, msg_type = ""):
    global storage_client_reused
    if storage_client_reused is None:
        storage_client_reused = storage.Client(project=os.environ['GCP_PROJECT_ID'])

    bucket = storage_client_reused.get_bucket(os.environ['GCP_BUCKET_NAME'])

    fname = str(uuid.uuid4())
    if user_id:
        fname = 'terraUID-' + user_id + "-"+ msg_type + '~' + fname
    fname = 'raw/' + fname

    blob = bucket.blob(fname)
    with blob.open(mode='w') as f:
        f.write(json.dumps(data))

    return fname # return the final filename out

