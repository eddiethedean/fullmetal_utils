from typing import List, Optional

import sqlalchemy as sa
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.session import Session

from .exeptions import MissingPrimaryKey


def get_metadata_from_engine(
    engine: sa.Engine,
    schema: Optional[str] = None
) -> sa.MetaData:
    """
    Get a SQLAlchemy MetaData object associated with a given database connection and schema.

    Parameters
    ----------
    engine : fullmetalalchemy.engine.Engine
        The database connection to associate with the MetaData object.
    schema : Optional[str], default None
        The name of the schema to use with the MetaData object. If None, the default schema is used.

    Returns
    -------
    sqlalchemy.MetaData
        The MetaData object associated with the input connection and schema.
    """
    # 2.X version
    meta = sa.MetaData(schema=schema)
    meta.reflect(bind=engine)
    return meta
    # 1.X version
    # return sa.MetaData(bind=engine, schema=schema)


def get_metadata_from_session(
    session: Session,
    schema: Optional[str] = None
) -> sa.MetaData:
    """
    Get a SQLAlchemy MetaData object associated with a given database connection and schema.
    """
    con = session.connection()
    meta = sa.MetaData(schema=schema)
    meta.reflect(bind=con)
    return meta


def get_table_from_engine(
    table_name: str,
    engine: sa.Engine,
    schema: Optional[str] = None
) -> sa.Table:
    """
    Get a SQLAlchemy Table object associated with a given table name, database connection, and schema.

    Parameters
    ----------
    table_name : str
        The name of the table to retrieve.
    connection : fullmetalalchemy.types.SqlConnection
        The database connection to use to retrieve the table.
    schema : Optional[str], default None
        The name of the schema to use when retrieving the table. If None, the default schema is used.

    Returns
    -------
    sqlalchemy.Table
        The Table object associated with the input table name, database connection, and schema.
    """
    metadata = get_metadata_from_engine(engine, schema)
    return sa.Table(table_name,
                    metadata,
                    autoload_with=engine,
                    extend_existing=True,
                    schema=schema)


def get_class_from_engine(
    table_name: str,
    engine: sa.Engine,
    schema: Optional[str] = None
) -> DeclarativeMeta:
    """
    Reflects the specified table and returns a declarative class that corresponds to it.

    Parameters
    ----------
    table_name : str
        The name of the table to reflect.
    connection : Union[SqlConnection, Session]
        The connection to use to reflect the table. This can be either an `SqlConnection`
        or an active `Session` object.
    schema : Optional[str], optional
        The name of the schema to which the table belongs, by default None.

    Returns
    -------
    DeclarativeMeta
        The declarative class that corresponds to the specified table.

    Raises
    ------
    MissingPrimaryKey
        If the specified table does not have a primary key.
    """
    metadata = get_metadata_from_engine(engine, schema)
    metadata.reflect(engine, only=[table_name], schema=schema)
    Base = automap_base(metadata=metadata)
    Base.prepare()
    if table_name not in Base.classes:
        raise MissingPrimaryKey()
    return Base.classes[table_name]


def get_class_from_session(
    table_name: str,
    session: Session,
    schema: Optional[str] = None
) -> DeclarativeMeta:
    """
    Reflects the specified table and returns a declarative class that corresponds to it.

    Parameters
    ----------
    table_name : str
        The name of the table to reflect.
    connection : Union[SqlConnection, Session]
        The connection to use to reflect the table. This can be either an `SqlConnection`
        or an active `Session` object.
    schema : Optional[str], optional
        The name of the schema to which the table belongs, by default None.

    Returns
    -------
    DeclarativeMeta
        The declarative class that corresponds to the specified table.

    Raises
    ------
    MissingPrimaryKey
        If the specified table does not have a primary key.
    """
    connection = session.connection()
    metadata = get_metadata_from_session(session, schema)
    metadata.reflect(connection, only=[table_name], schema=schema)
    Base = automap_base(metadata=metadata)
    Base.prepare()
    if table_name not in Base.classes:
        raise MissingPrimaryKey()
    return Base.classes[table_name]


def get_column_from_table(
    table: sa.Table,
    column_name: str
) -> sa.Column:
    """
    Retrieve a SQLAlchemy column object from a SQLAlchemy table.

    Parameters
    ----------
    table : sqlalchemy.Table
        The SQLAlchemy table to retrieve the column from.
    column_name : str
        The name of the column to retrieve.

    Returns
    -------
    sqlalchemy.Column
        The SQLAlchemy column object corresponding to the given column name.
    """
    return table.c[column_name]


def primary_key_columns_from_table(
    table: sa.Table
) ->  List[sa.Column]:
    """
    Return the primary key columns of a SQLAlchemy Table.

    Parameters
    ----------
    table : sqlalchemy.Table
        The table whose primary key columns will be returned.

    Returns
    -------
    List of sqlalchemy.Column
        The list of primary key columns for the input table.
    """
    return list(table.primary_key.columns)