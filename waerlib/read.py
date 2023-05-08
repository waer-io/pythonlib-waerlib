import pyarrow.flight as flight

def read(user_id, beg_time, end_time, tags, host, port, username, password):
    client = flight.FlightClient(f'grpc+tcp://{host}:32010/grpc')
    token = client.authenticate_basic_token(username, password)

    query = '''
    SELECT * FROM datalake.test limit 10
    '''
    options = flight.FlightCallOptions(headers=[token])
    flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
    reader = client.do_get(flight_info.endpoints[0].ticket, options)

    df = reader.read_pandas()
    #df = df.rename(columns={'dir0':'a'})
    #df.loc[:,'a'] = [entry.split('=')[1] for entry in df.loc[:,'a']]
    
    return df

