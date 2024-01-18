from typing import Any, Iterable, Optional, Sequence, Union
import decimal
import datetime

import sqlalchemy as sa
from sqlalchemy import Engine
from tinytim.rows import row_dicts_to_data
from tinytim.data import column_names

from . import sa_orm
from . import type_convert


def create_table(
    table_name: str,
    column_names:  Sequence[str],
    column_types:  Sequence,
    primary_key: Sequence[str],
    engine: Engine,
    schema: Optional[str] = None,
    autoincrement: Optional[bool] = False,
    if_exists: Optional[str] = 'error'
) -> sa.Table:
    """
    Create a sql table from specifications.

    Parameters
    ----------
    table_name : str
    column_names : Sequence[str]
    column_types : Sequence
    primary_key : Sequence[str]
    engine : SqlAlchemy Engine
    schema : Optional[str]
    autoincrement : Optional[bool] default, None
    if_exists : Optional[str] default, 'error'

    Returns
    -------
    sqlalchemy.Table
    """
    cols = []
    
    for name, python_type in zip(column_names, column_types):
        sa_type = type_convert._type_convert[python_type]
        if type(primary_key) is str:
            primary_key = [primary_key]
        if name in primary_key:
            col = sa.Column(name, sa_type,
                            primary_key=True,
                            autoincrement=autoincrement)
        else:
            col = sa.Column(name, sa_type)
        cols.append(col)

    metadata = sa_orm.get_metadata(engine, schema)
    table = sa.Table(table_name, metadata, *cols, schema=schema)
    if if_exists == 'replace':
        drop_table_sql = sa.schema.DropTable(table, if_exists=True)
        with engine.connect() as con:
            con.execute(drop_table_sql)
    table_creation_sql = sa.schema.CreateTable(table)
    with engine.connect() as con:
        con.execute(table_creation_sql)
    return sa_orm.get_table(table_name, engine, schema=schema)


def column_datatype(values: Iterable) -> type:
    dtypes = [
        int, str, (int, float), decimal.Decimal, datetime.datetime,
        bytes, bool, datetime.date, datetime.time, 
        datetime.timedelta, list, dict
    ]
    for value in values:
        for dtype in list(dtypes):
            if not isinstance(value, dtype):
                dtypes.pop(dtypes.index(dtype))
    if len(dtypes) == 2:
        if set([int, Union[float, int]]) == {int, Union[float, int]}:
            return int
    if len(dtypes) == 1:
        if dtypes[0] == Union[float, int]:
            return float
        return dtypes[0]
    return str


def create_table_from_rows(
    table_name: str,
    rows:  Sequence[dict],
    primary_key: Sequence[str],
    engine: Engine,
    column_types: Optional[Sequence] = None,
    schema: Optional[str] = None,
    autoincrement: Optional[bool] = False,
    if_exists: Optional[str] = 'error',
    columns: Optional[Sequence[str]] = None,
    missing_value: Optional[Any] = None
) -> sa.Table:
    """
    Create a sql table from specs.
    
    Returns
    -------
    sqlalchemy.Table
    """
    data = row_dicts_to_data(rows, columns, missing_value)
    if column_types is None:
        column_types = [column_datatype(values) for values in data.values()]
    col_names = column_names(data)
    return create_table(table_name, col_names, column_types, primary_key, engine, schema, autoincrement, if_exists)