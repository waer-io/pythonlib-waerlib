import datetime
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, sessionmaker

from .sql_props import get_sql_url, get_pool_settings

engine = sa.create_engine(get_sql_url(), **get_pool_settings)
Session = sessionmaker(bind=engine)
Base = declarative_base()

"""

    The 'outputs' table in Postgres

    model - writes to. insertBatched.
    model - reads from. getAll.
    coordinator - for reports. The query is made right in this lib in `report.py` in function `get_data`.

"""

class Outputs(Base):
    __tablename__ = "outputs"
    id = sa.Column('id', sa.BigInteger, primary_key=True)
    timestamp = sa.Column('timestamp', sa.BigInteger, nullable=False)
    key = sa.Column('key', sa.String(255), nullable=False)
    val = sa.Column('val', sa.Text, nullable=False) # might want it as bytea in the future.
    version = sa.Column('version', sa.String(255), nullable=False)
    user_id = sa.Column('user_id', sa.String(255), nullable=False)


def insertBatched(df):
    with Session() as session:
        with session.begin():
            df.to_sql(Outputs.__tablename__, con=session.bind, if_exists='append', index=False, chunksize=1000)

# todo. might want an index on those for all tables?
def getAll(user_id, keys, start_timestamp, end_timestamp):
    with Session() as session:
        query = session.query(Outputs).filter(
            Outputs.user_id == user_id,
            Outputs.key.in_(keys),
            Outputs.timestamp >= start_timestamp,
            Outputs.timestamp <= end_timestamp,
        )
        return pd.read_sql(query.statement, session.bind)