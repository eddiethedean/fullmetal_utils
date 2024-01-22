from typing import Any, Dict, Generator, Optional, Sequence

import sqlalchemy as sa
from sqlalchemy.orm import Session

from fullmetal_utils.fullmetalalchemy.rows import row_to_dict
from fullmetal_utils.fullmetalalchemy.sa_orm import get_column_with_table, get_table_from_engine, get_table_from_session, primary_key_columns_with_table


def select_records_all_query_with_table(
    table: sa.Table,
    sorted: bool = False,
    include_columns: Optional[Sequence[str]] = None
) -> sa.Select:
    if include_columns is not None:
        columns = [get_column_with_table(table, column_name) for column_name in include_columns]
        query = sa.select(*columns)
    else:
        query = sa.select(table)
    if sorted:
        query = query.order_by(*primary_key_columns_with_table(table))
    return query


def rows_from_results(results: sa.CursorResult) -> Generator[Dict[str, Any], None, None]:
    for row in results:
        yield row_to_dict(row)


def select_records_all_with_session(
    table_name: str,
    session: Session,
    sorted: bool = False,
    include_columns: Optional[Sequence[str]] = None
) ->  Generator[Dict[str, Any], None, None]:
    """
    Select all records from the specified table.
    """
    table = get_table_from_session(table_name, session)
    query = select_records_all_query_with_table(table, sorted, include_columns)
    connection = session.connection()
    results = connection.execute(query)
    return rows_from_results(results)


def select_records_all_with_engine(
    table_name: str,
    engine: sa.Engine,
    sorted: bool = False,
    include_columns: Optional[Sequence[str]] = None
) ->  Generator[Dict[str, Any], None, None]:
    """
    Select all records from the specified table.
    """
    table = get_table_from_engine(table_name, engine)
    query = select_records_all_query_with_table(table, sorted, include_columns)
    with engine.connect() as connection:
        results = connection.execute(query)
    return rows_from_results(results)