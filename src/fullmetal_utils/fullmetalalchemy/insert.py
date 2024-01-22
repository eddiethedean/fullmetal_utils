from typing import Optional, Sequence
import sqlalchemy as sa
from sqlalchemy.orm import Session

from .constraints import missing_primary_key
from .exeptions import MissingPrimaryKey
from .sa_orm import connection_from_session, get_class, get_table


def insert_records(
    table_name: str,
    records: Sequence[dict],
    engine: sa.engine.Engine,
    schema: Optional[str] = None
) -> None:
    with Session(engine) as session:
        connection = connection_from_session(session)
        table = get_table(table_name, connection, schema)
        insert_records_session(table, records, session)
        session.commit()


def insert_records_session(
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
    if missing_primary_key(table):
        insert_records_slow_session(table, records, session)
    else:
        insert_records_fast_session(table, records, session)


def insert_records_fast_session(
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
    if missing_primary_key(table):
        raise MissingPrimaryKey()
    connection = session.connection()
    table_class = get_class(table.name, connection, schema=table.schema)
    mapper = sa.inspect(table_class)
    session.bulk_insert_mappings(mapper, records)


def insert_records_slow_session(
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