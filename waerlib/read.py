import os
import pandas as pd
import pyarrow.flight as flight


def read(user_id, beg_time, end_time, tags, collection, dedup=False):
    host = os.environ['DREMIO_HOST']
    username = os.environ['DREMIO_USERNAME']
    password = os.environ['DREMIO_PASSWORD']


    client = flight.FlightClient(f'grpc+tcp://{host}:32010/grpc')
    token = client.authenticate_basic_token(username, password)
    options = flight.FlightCallOptions(headers=[token])

    # Update metadata ...
    # attempting to move this to after write instead, see write.refresh_collections.
    # re coordinator: before updating waerlib version, need to add env vars to coordinator
#     query = f'''ALTER TABLE datalake.{collection} REFRESH METADATA;'''
#     flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
#     reader = client.do_get(flight_info.endpoints[0].ticket, options)
#     df = reader.read_pandas()

    # Query data
    if dedup==False:
        query = f'''
        SELECT * FROM datalake.{collection}
        WHERE "dir0"='user_id={user_id}'
        AND RIGHT("dir1",7)>='{beg_time[:7]}'
        AND RIGHT("dir1",7)<='{end_time[:7]}'
        AND "key" IN ({','.join(["'" + i + "'" for i in tags])})
        AND "timestamp">='{pd.to_datetime([beg_time]).astype('datetime64[us]').astype(int)[0]}'
        AND "timestamp"<='{pd.to_datetime([end_time]).astype('datetime64[us]').astype(int)[0]}'
        '''
    else:
        query = f'''
        SELECT * FROM (
        SELECT "dir0", "timestamp", "key", "val", 
        ROW_NUMBER() OVER (PARTITION BY "dir0", "timestamp", "key" ORDER BY "timestamp" DESC)
        FROM (
        SELECT * FROM datalake.{collection}
        WHERE "dir0"='user_id={user_id}'
        AND RIGHT("dir1",7)>='{beg_time[:7]}'
        AND RIGHT("dir1",7)<='{end_time[:7]}'
        AND "key" IN ({','.join(["'" + i + "'" for i in tags])})
        AND "timestamp">='{pd.to_datetime([beg_time]).astype('datetime64[us]').astype(int)[0]}'
        AND "timestamp"<='{pd.to_datetime([end_time]).astype('datetime64[us]').astype(int)[0]}'
        )
        ) WHERE "EXPR$4"=1
        '''
        
    flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
    reader = client.do_get(flight_info.endpoints[0].ticket, options)

    df = reader.read_pandas()
    df = df.rename(columns={'dir0':'user_id'})
    df = df.drop(['dir1'], axis=1)
    df.user_id = df.user_id.map(lambda x: x.split('user_id=')[1])
    df.timestamp = pd.to_datetime(df.timestamp*1000)
    return df

