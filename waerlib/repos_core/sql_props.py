import os

def get_sql_url():

    local = os.environ.get("IS_LOCAL")

    if local is not None and local.lower() == "true":
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


def get_pool_settings():
    """
        * pool_size: This is the number of connections to keep in the pool. The default is usually 5. Increasing the pool size can improve performance under high load by reducing the number of times new connections need to be created.
        * max_overflow: This specifies the number of connections that can be created above pool_size if the pool is exhausted. By default, it’s often set to 10. This allows the pool to exceed its size temporarily under heavy load, at the cost of additional resources.
        * pool_timeout: The number of seconds to wait before giving up on returning a connection from the pool. If set to None, it will wait indefinitely. This can be adjusted to ensure that your application behaves correctly under high load, either by waiting longer for a connection or failing quickly to try alternative logic.
        * pool_recycle: This is the maximum number of seconds a connection can persist in the pool. After this time, the connection is automatically replaced with a new one. This is useful to avoid database server timeouts or to deal with database server settings that close idle connections.
        * echo_pool: Setting this to True enables logging for pool checkouts and check-ins, which can be useful for debugging and performance tuning.
        * pool_pre_ping: If set to True, SQLAlchemy will test each connection for liveness before using it, automatically discarding and replacing stale or broken connections. This can prevent errors in applications that experience infrequent use of database connections or operate in environments where network issues are common.
    """
    ten_minutes_in_seconds = 10*60
    return {
               'pool_size': 10,
               'max_overflow': 20,
               'pool_timeout': 30,
               'pool_recycle': ten_minutes_in_seconds,
               'echo_pool': True,
               'pool_pre_ping': True
           }