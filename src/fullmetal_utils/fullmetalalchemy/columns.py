from typing import Any, Dict, List, Optional

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
    engine: sa.engine.Engine,
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
        table = get_table(table_name, connection, schema)
        return get_column_names_from_table(table)
    

def get_column_types_from_table(
    table: sa.Table
) -> Dict[str, Any]:
    """
    Get the types of columns in a SQLAlchemy table.

    Parameters
    ----------
    table : sqlalchemy.Table
        SQLAlchemy table to get column types from.

    Returns
    -------
    dict
        A dictionary with the names of columns as keys and the SQLAlchemy
        types of the columns as values.
    """
    return {c.name: c.type for c in table.c}


def get_column_types(
    table_name: str,
    engine: sa.engine.Engine,
    schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get the types of columns in a SQLAlchemy table.

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
    dict
        A dictionary with the names of columns as keys and the SQLAlchemy
        types of the columns as values.
    """
    with engine.connect() as connection:
        table = get_table(table_name, connection, schema)
        return get_column_types_from_table(table)