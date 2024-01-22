from typing import Any, Dict, Generator, Optional, Sequence

import sqlalchemy as sa

from fullmetal_utils.fullmetalalchemy.connections import HasConnection, get_connectable_from_hc
from fullmetal_utils.fullmetalalchemy.rows import row_to_dict
from fullmetal_utils.fullmetalalchemy.sa_orm import get_column_from_table, get_table_from_engine, primary_key_columns_from_table


def select_records_all(
    table_name: str,
    engine: sa.Engine,
    sorted: bool = False,
    include_columns: Optional[Sequence[str]] = None
) ->  Generator[Dict[str, Any], None, None]:
    """
    Select all records from the specified table.
    """
    table = get_table_from_engine(table_name, engine)
    if include_columns is not None:
        columns = [get_column_from_table(table, column_name) for column_name in include_columns]
        query = sa.select(*columns)
    else:
        query = sa.select(table)
    if sorted:
        query = query.order_by(*primary_key_columns_from_table(table))

    with engine.connect() as con:
        results = con.execute(query)
    for row in results:
        yield row_to_dict(row)