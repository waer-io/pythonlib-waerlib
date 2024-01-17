import os
import pandas as pd
import pyarrow.flight as flight

def refresh_collections():
    """
    Refreshes metadata for specified collections in a Dremio datalake using Apache Arrow Flight.

    Ensures we get latest data.
    """

    collections = ['profiles', 'outputs', 'samples', 'parsed']
    host = os.environ['DREMIO_HOST']
    username = os.environ['DREMIO_USERNAME']
    password = os.environ['DREMIO_PASSWORD']


    try:
        client = flight.FlightClient(f'grpc+tcp://{host}:32010/grpc')
        token = client.authenticate_basic_token(username, password)
        options = flight.FlightCallOptions(headers=[token])

        for collection in collections:
            query = f'ALTER TABLE datalake.{collection} REFRESH METADATA;'
            print(f"Refreshing metadata for {collection}")
            flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
            with client.do_get(flight_info.endpoints[0].ticket, options) as reader:
                print(f"Refreshed metadata successfully for {collection}")
                df = reader.read_pandas()

        print("Metadata refresh completed.")
        return True

    except Exception as e:
        print(f"An error occurred refreshing metadata: {e}")
        return False


def read(user_id, beg_time, end_time, tags, collection, dedup=False):
    host = os.environ['DREMIO_HOST']
    username = os.environ['DREMIO_USERNAME']
    password = os.environ['DREMIO_PASSWORD']


    client = flight.FlightClient(f'grpc+tcp://{host}:32010/grpc')
    token = client.authenticate_basic_token(username, password)
    options = flight.FlightCallOptions(headers=[token])

    # Update metadata
    # Commented as this makes every query 30s+. We currently are trying to
    # get quick responses over getting the responses updated quick.
    # We'll test out if this works, and if yes, we might have a separate cronjob
    # for refreshing more often.
    #query = f'''ALTER TABLE datalake.{collection} REFRESH METADATA;'''
    #flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
    #reader = client.do_get(flight_info.endpoints[0].ticket, options)
    #df = reader.read_pandas()

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
        dedup_query = f'''
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

