import datetime
import pandas as pd

import sqlalchemy as sa
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base
import json
from .sql_props import get_sql_url

engine = sa.create_engine(get_sql_url())

Base = declarative_base()

"""

    The 'parsed' table in Postgres

    ingest - writes to. insertBatched.
    model - reads from. getAll.

"""

class Parsed(Base):
    __tablename__ = "parsed"
    id = sa.Column('id', sa.BigInteger, primary_key=True)
    timestamp = sa.Column('timestamp', sa.BigInteger, nullable=False)
    key = sa.Column('key', sa.String(255), nullable=False)
    val = sa.Column('val', sa.Text, nullable=False) # might want it as bytea in the future.
    version = sa.Column('version', sa.String(255), nullable=False)
    user_id = sa.Column('user_id', sa.String(255), nullable=False)


def insertBatched(df):
    df.to_sql(Parsed.__tablename__, con=engine, if_exists='append', index=False, chunksize=1000)

# todo. might want an index on those for all tables?
def getAll(user_id, keys, start_timestamp, end_timestamp):
    with sa.orm.Session(engine) as session:
        query = session.query(Parsed).filter(
            Parsed.user_id == user_id,
            Parsed.key.in_(keys),
            Parsed.timestamp >= start_timestamp,
            Parsed.timestamp <= end_timestamp,
        )

        return pd.read_sql(query.statement, session.bind)