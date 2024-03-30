import datetime
import pandas as pd

import sqlalchemy as sa
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base
import json
from sqlalchemy.dialects.postgresql import JSONB
from .sql_props import get_sql_url

engine = sa.create_engine(get_sql_url())

Base = declarative_base()

"""

    The 'samples' table in Postgres

    ingest - writes to. insertBatched.
    model - reads from. getAll.

"""

class Samples(Base):
    __tablename__ = "samples"
    id = sa.Column('id', sa.BigInteger, primary_key=True)
    timestamp = sa.Column('timestamp', sa.BigInteger, nullable=False)
    key = sa.Column('key', sa.String(255), nullable=False)
    val = sa.Column('val', JSONB, nullable=False)
    version = sa.Column('version', sa.String(255), nullable=False)
    user_id = sa.Column('user_id', sa.String(255), nullable=False)


def insertBatched(df):
    df.to_sql(Samples.__tablename__, con=engine, if_exists='append', index=False, chunksize=1000)

# todo. might want an index on those for all tables?
def getAll(user_id, keys, start_timestamp, end_timestamp):
    with sa.orm.Session(engine) as session:
        query = session.query(Samples).filter(
            Samples.user_id == user_id,
            Samples.key.in_(keys),
            Samples.timestamp >= start_timestamp,
            Samples.timestamp <= end_timestamp,
        )

        return pd.read_sql(query.statement, session.bind)