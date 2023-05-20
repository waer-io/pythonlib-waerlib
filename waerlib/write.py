import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

def validate_df(df):
    assert 'user_id' in df.columns, 'Column "user_id" not in df'
    assert 'timestamp' in df.columns, 'Column "timestamp" not in df'
    assert 'key' in df.columns, 'Column "key" not in df'
    assert 'value' in df.columns, 'Column "value" not in df'
    assert len(df.columns) == 4, 'Invalid columns in df'

    df.loc[:,'timestamp'] = pd.to_datetime(df.loc[:,'timestamp'])
    df.loc[:,'user_id'] = df.loc[:,'user_id'].astype(str)
    df.loc[:,'key'] = df.loc[:,'key'].astype(str)
    df.loc[:,'value'] = df.loc[:,'value'].astype(str)
    df.loc[:,'month'] = df.loc[:,'timestamp'].dt.date.astype(str).str.slice(0,7)
    
    return df

def write(user_id, df, bucket, folder):
    df.loc[:,'user_id'] = user_id
    df = validate_df(df)
    pq.write_to_dataset(
        pa.Table.from_pandas(df),
        root_path=f'{bucket}/{folder}',
        partition_cols=['user_id', 'month'],
        filesystem=fs.GcsFileSystem()
    )
