import os
import pandas as pd
import pyarrow.flight as flight


# don't use this. use the read_v2 below. We need to get rid of extra work here and
# later handle any time and user_id parsing elsewhere.
def read(user_id, beg_time, end_time, tags, collection, dedup=False):
    host = os.environ['DREMIO_HOST']
    username = os.environ['DREMIO_USERNAME']
    password = os.environ['DREMIO_PASSWORD']


    client = flight.FlightClient(f'grpc+tcp://{host}:32010/grpc')
    token = client.authenticate_basic_token(username, password)
    options = flight.FlightCallOptions(headers=[token])

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



# this function should only read, not do other things.
# start_time_nanos and end_time_nanos   - we'll aim to just work with nanos for now. way too much time is spent on debugging time issues.
# start_year_month and end_year_month   - the folder date, the 'month=YYYY-MM' folder (aka dir1)
# dedup                                 - was always false, so just removed it.
def read_v2(user_id, start_time_nanos, end_time_nanos, start_year_month, end_year_month, tags, collection, dedup=False):
    host = os.environ['DREMIO_HOST']
    username = os.environ['DREMIO_USERNAME']
    password = os.environ['DREMIO_PASSWORD']


    client = flight.FlightClient(f'grpc+tcp://{host}:32010/grpc')
    token = client.authenticate_basic_token(username, password)
    options = flight.FlightCallOptions(headers=[token])


    # the old records seem to be microseconds, so /1000 as we use nanos everywhere for post 2024MAR29 logic.
    query = f'''
    SELECT * FROM datalake.{collection}
    WHERE "dir0"='user_id={user_id}'
    AND RIGHT("dir1",7)>='{start_year_month}'
    AND RIGHT("dir1",7)<='{end_year_month}'
    AND "key" IN ({','.join(["'" + i + "'" for i in tags])})
    AND "timestamp">={start_time_nanos/1000}
    AND "timestamp"<={end_time_nanos/1000}
    '''

    flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
    reader = client.do_get(flight_info.endpoints[0].ticket, options)

    print(f"WAERLIB OUTGOING QUERY", flush=True)
    print(f"{query}", flush=True)

    df = reader.read_pandas()
    df = df.rename(columns={'dir0':'user_id'})
    df = df.drop(['dir1'], axis=1) # dir1 is the 'month=YYYY-MM' folder

    # we'll just assume its with this prefix everywhere. (will remove this line and comment later, here for documentation purposes)
    #df.user_id = df.user_id.map(lambda x: x.split('user_id=')[1])

    # we'll assume we always just store nanos. (will remove this line and comment later, here for documentation purposes)
    # df.timestamp = pd.to_datetime(df.timestamp*1000)

    return df
