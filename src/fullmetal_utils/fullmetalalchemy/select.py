from typing import Any, Dict, Generator, List, Optional, Sequence, Union

import sqlalchemy as sa

from fullmetal_utils.fullmetalalchemy.rows import row_to_dict
from fullmetal_utils.fullmetalalchemy.sa_orm import get_column, get_table, primary_key_columns


def select_records_all_from_table(
    table: Union[sa.Table, str],
    connection: Optional[sa.Connection] = None,
    sorted: bool = False,
    include_columns: Optional[Sequence[str]] = None
) ->  Generator[Dict[str, Any], None, None]:
    """
    Select all records from the specified table.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The table to select records from.
    connection : Optional[fullmetalalchemy.types.SqlConnection]
        The database connection to use. If not provided, the default connection
        will be used.
    sorted : bool, optional
        If True, the records will be sorted by their primary key(s), by default False.
    include_columns : Optional[Sequence[str]], optional
        A sequence of column names to include in the query, by default None.

    Returns
    -------
    Generator[dict]
        A generator of dictionaries representing the selected records.
    """
    if connection is None:
        connection = table.bind.connect()

    if include_columns is not None:
        columns = [get_column(table, column_name) for column_name in include_columns]
        query = sa.select(*columns)
    else:
        query = sa.select(table)

    if sorted:
        query = query.order_by(*primary_key_columns(table))
    results = connection.execute(query)
    for row in results:
        yield row_to_dict(row)


def select_records_all(
    table_name: str,
    connection: sa.Connection,
    sorted: bool = False,
    include_columns: Optional[Sequence[str]] = None
) ->  Generator[Dict[str, Any], None, None]:
    """
    Select all records from the specified table.

    Parameters
    ----------
    table : Union[sqlalchemy.Table, str]
        The table to select records from.
    connection : Optional[fullmetalalchemy.types.SqlConnection]
        The database connection to use. If not provided, the default connection
        will be used.
    sorted : bool, optional
        If True, the records will be sorted by their primary key(s), by default False.
    include_columns : Optional[Sequence[str]], optional
        A sequence of column names to include in the query, by default None.

    Returns
    -------
    Generator[dict]
        A generator of dictionaries representing the selected records.
    """
    table = get_table(table_name, connection)
    return select_records_all_from_table(table, connection, sorted, include_columns)