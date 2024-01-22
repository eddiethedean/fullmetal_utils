from typing import Any, Dict, Generator, Optional, Sequence

import sqlalchemy as sa

from fullmetal_utils.fullmetalalchemy.connections import HasConnection, get_connection_from_hc
from fullmetal_utils.fullmetalalchemy.rows import row_to_dict
from fullmetal_utils.fullmetalalchemy.sa_orm import get_column, get_table, primary_key_columns


def select_records_all(
    table_name: str,
    connection: HasConnection,
    sorted: bool = False,
    include_columns: Optional[Sequence[str]] = None
) ->  Generator[Dict[str, Any], None, None]:
    """
    Select all records from the specified table.
    """
    connection = get_connection_from_hc(connection)
    table = get_table(table_name, connection)
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