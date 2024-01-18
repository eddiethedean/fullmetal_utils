from typing import List, Optional

import sqlalchemy as sa

from .sa_orm import get_table


def get_column_names_from_table(
    table: sa.Table
) ->  List[str]:
    """
    Returns a list of the column names for the given SQLAlchemy table object.

    Parameters
    ----------
    table : sqlalchemy.Table
        The SQLAlchemy table object to get column names for.

    Returns
    -------
    List[str]
        A list of the column names for the given table.
    """
    return [c.name for c in table.columns]


def get_column_names(
    table_name: str,
    engine: sa.Engine,
    schema: Optional[str] = None
) -> List[str]:
    """
    Returns a list of the column names for the given table name.

    Parameters
    ----------
    table_name : str
        The table name to get column names for.
    engine: sqlalchemy.Engine
        The engine to connect to the database.
    schema: Optional[str]
        The database schema name.

    Returns
    -------
    List[str]
        A list of the column names for the given table name.
    """
    with engine.connect() as connection:
        table = get_table(table_name, engine, schema)
        return get_column_names_from_table(table)