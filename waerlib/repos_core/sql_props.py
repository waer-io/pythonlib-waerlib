import os

def get_sql_url(local=False):

    if local:
        return get_local_sql_url()

    sql_url = 'postgresql://{username}:{password}@{hostname}/{database}'.format(
        hostname=os.environ['CORE_POSTGRES_HOSTNAME'],
        database=os.environ['CORE_POSTGRES_DATABASE'],
        username=os.environ['CORE_POSTGRES_USERNAME'],
        password=os.environ['CORE_POSTGRES_PASSWORD'],
    )
    return sql_url

def get_local_sql_url():
    """
    For use with local cloud-sql-proxy.
    https://cloud.google.com/sql/docs/postgres/connect-auth-proxy#connect-tcp

    * run sql auth proxy:  ./cloud-sql-proxy --address 0.0.0.0 --port 1234 waer-prd-infras-ed0e:europe-west1:waer-prd-database-core-a3c252f5f096
    * log in (e.g. to check it works): psql "host=127.0.0.1 sslmode=disable dbname=core user=admin port=1234"
    #
    #export CORE_POSTGRES_HOSTNAME=127.0.0.1 (your local)
    #export CORE_POSTGRES_DATABASE=core (actual remote db name)
    #export CORE_POSTGRES_USERNAME=admin (actual remote user)
    #export CORE_POSTGRES_PASSWORD=<actual remote password>

    """
    return 'postgresql://{username}:{password}@{hostname}:{port}/{database}'.format(
                   hostname=os.environ['CORE_POSTGRES_HOSTNAME'],
                   database=os.environ['CORE_POSTGRES_DATABASE'],
                   username=os.environ['CORE_POSTGRES_USERNAME'],
                   port=1234, # some port as set with cloud sql proxy, don't set 5432, likely already in use if running postgres locally
                   password=os.environ['CORE_POSTGRES_PASSWORD'],
               )