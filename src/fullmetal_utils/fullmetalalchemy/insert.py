from typing import Optional, Sequence

import sqlalchemy as sa
from sqlalchemy.orm import Session

from .constraints import missing_primary_key_with_table
from .exeptions import MissingPrimaryKey
from .sa_orm import get_class_with_session, get_table_from_engine


def insert_records_with_engine(
    table_name: str,
    records: Sequence[dict],
    engine: sa.engine.Engine,
    schema: Optional[str] = None
) -> None:
    table = get_table_from_engine(table_name, engine, schema)
    with Session(engine) as session:
        insert_records_with_session(table, records, session)
        session.commit()


def insert_records_with_session(
    table: sa.Table,
    records: Sequence[dict],
    session: Session
) -> None:
    """
    Insert records into a given table using a provided session.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The table to insert records into. Can be a string name of the table or a SQLAlchemy
        Table object.
    records : Sequence[Dict[str, Any]]
        A sequence of dictionaries representing the records to insert into the table.
    session : sqlalchemy.orm.Session
        A SQLAlchemy session to use for the insertion.

    Returns
    -------
    None
    """
    if missing_primary_key_with_table(table):
        insert_records_slow_with_session(table, records, session)
    else:
        insert_records_fast_with_session(table, records, session)


def insert_records_fast_with_session(
    table: sa.Table,
    records: Sequence[dict],
    session: Session
) -> None:
    """
    Insert a sequence of new records into a SQLAlchemy Table using bulk insert.

    Parameters
    ----------
    table : sqlalchemy.Table
        The SQLAlchemy Table to insert the records into.
    records : Sequence[fullmetalalchemy.types.Record]
        The sequence of new records to insert into the table.
    session : sqlalchemy.orm.Session
        The SQLAlchemy Session to use for the transaction.

    Raises
    ------
    fullmetalalchemy.exceptions.MissingPrimaryKey
        If the table does not have a primary key.

    Returns
    -------
    None
    """
    if missing_primary_key_with_table(table):
        raise MissingPrimaryKey()
    table_class = get_class_with_session(table.name, session, schema=table.schema)
    mapper = sa.inspect(table_class)
    session.bulk_insert_mappings(mapper, records)


def insert_records_slow_with_session(
    table: sa.Table,
    records: Sequence[dict],
    session: Session
) -> None:
    """
    Inserts records into the given table using the provided session and
    the slow method of SQLAlchemy.

    Parameters
    ----------
    table : sqlalchemy.Table
        The table into which the records are being inserted.
    records : Sequence[fullmetalalchemy.types.Record]
        The records to be inserted.
    session : sqlalchemy.orm.session.Session
        The session to use for the insertion.

    Returns
    -------
    None

    """
    session.execute(table.insert(), records)