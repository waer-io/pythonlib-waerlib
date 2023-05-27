import os
import pyarrow.flight as flight

#ALTER TABLE test REFRESH METADATA;

def read(user_id, beg_time, end_time, tags, collection):
    host = os.environ['DREMIO_HOST']
    username = os.environ['DREMIO_USERNAME']
    password = os.environ['DREMIO_PASSWORD']

    client = flight.FlightClient(f'grpc+tcp://{host}:32010/grpc')
    token = client.authenticate_basic_token(username, password)

    query = f'''
    SELECT * FROM datalake.{collection}
    WHERE "dir0"='user_id={user_id}'
    AND RIGHT("dir1",7)>='{beg_time[:7]}'
    AND RIGHT("dir1",7)<='{end_time[:7]}'
    AND "key" IN ({','.join(["'" + i + "'" for i in tags])})
    AND "timestamp">='{beg_time}'
    AND "timestamp"<='{end_time}'
    '''
    options = flight.FlightCallOptions(headers=[token])
    flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
    reader = client.do_get(flight_info.endpoints[0].ticket, options)

    df = reader.read_pandas()
    df = df.rename(columns={'dir0':'user_id'})
    df = df.drop(['dir1'], axis=1)
    df.user_id = df.user_id.map(lambda x: x.split('user_id=')[1])
    
    return df


