
import os
from datetime import datetime, timedelta
import pyarrow.flight as flight

tag_refresh = "[WAERLIB: refresh]"


def refresh_collections(collections = ['profiles', 'outputs', 'samples', 'parsed']):
    """
    Refreshes metadata for specified collections in a Dremio datalake using Apache Arrow Flight.

    Ensures we get latest data.
    """

    print(f"[{datetime.now()}] {tag_refresh} Starting refresh collections...", flush=True)

    host = os.environ['DREMIO_HOST']
    username = os.environ['DREMIO_USERNAME']
    password = os.environ['DREMIO_PASSWORD']

    try:
        flight_client = flight.FlightClient(f'grpc+tcp://{host}:32010/grpc')

        for collection in collections:

            # try auth each time, so as to not expire token
            print(f"[{datetime.now()}] {tag_refresh} Authenticating token..", flush=True)
            token = flight_client.authenticate_basic_token(username, password)
            print(f"[{datetime.now()}] {tag_refresh} Authenticated token", flush=True)
            options = flight.FlightCallOptions(headers=[token], timeout=timedelta(seconds=60 * 15)) # 15min
            # /

            query = f'''ALTER TABLE datalake.{collection} REFRESH METADATA;'''
            print(f"[{datetime.now()}] {tag_refresh} Refreshing metadata for {collection}", flush=True)
            flight_info = flight_client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
            print(f"[{datetime.now()}] {tag_refresh} Performing get for {collection}", flush=True)
            reader = flight_client.do_get(flight_info.endpoints[0].ticket, options)
            # Process reader if needed here

        print(f"[{datetime.now()}] {tag_refresh} Metadata refresh completed.", flush=True)
        return True

    except Exception as e:
        print(f"[{datetime.now()}] {tag_refresh} An error occurred refreshing metadata: {e}", flush=True)
        return False
