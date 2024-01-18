from typing import Optional

import sqlalchemy as sa
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.ext.automap import automap_base

from fullmetal_utils.exeptions import MissingPrimaryKey


def connection_from_session(session: sa.orm.Session) -> sa.Connection:
    return session.connection()


def get_metadata(
    connection: sa.Connection,
    schema: Optional[str] = None
) -> sa.MetaData:
    """
    Get a SQLAlchemy MetaData object associated with a given database connection and schema.

    Parameters
    ----------
    connection : fullmetalalchemy.types.SqlConnection
        The database connection to associate with the MetaData object.
    schema : Optional[str], default None
        The name of the schema to use with the MetaData object. If None, the default schema is used.

    Returns
    -------
    sqlalchemy.MetaData
        The MetaData object associated with the input connection and schema.

    Examples
    --------
    >>> import fullmetalalchemy as fa
    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> fa.features.get_metadata(engine)
    MetaData(bind=Engine(sqlite:///data/test.db))
    """
    # 2.X version
    meta = sa.MetaData(schema=schema)
    meta.reflect(bind=connection)
    return meta
    # 1.X version
    # return sa.MetaData(bind=connection, schema=schema)


def get_table(
    table_name: str,
    connection: sa.Connection,
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
    metadata = get_metadata(connection, schema)
    return sa.Table(table_name,
                    metadata,
                    autoload_with=connection,
                    extend_existing=True,
                    schema=schema)


def get_class(
    table_name: str,
    connection: sa.Connection,
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

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine = fa.create_engine('sqlite:///data/test.db')
    >>> fa.features.get_class('xy', engine)
    sqlalchemy.ext.automap.xy
    """
    metadata = get_metadata(connection, schema)

    metadata.reflect(connection, only=[table_name], schema=schema)
    Base = automap_base(metadata=metadata)
    Base.prepare()
    if table_name not in Base.classes:
        raise MissingPrimaryKey()
    return Base.classes[table_name]