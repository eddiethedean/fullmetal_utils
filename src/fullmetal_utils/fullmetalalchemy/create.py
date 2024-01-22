from typing import Any, Dict, Iterable, Literal, Optional, Sequence, Union
import decimal
import datetime

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from tinytim.rows import row_dicts_to_data
from tinytim.data import column_names

from . import sa_orm
from . import type_convert


def create_table_with_engine(
    name: str,
    columns: Dict[str, Any],
    primary_key: str | Sequence[str],
    engine: Engine,
    schema: Optional[str] = None,
    autoincrement: Union[bool, Literal['auto', 'ignore_fk']] = False,
    if_exists: Optional[str] = 'error'
) -> sa.Table:
    """
    Create a sql table from specifications.

    Parameters
    ----------
    name : str
        Name of table
    columns : Dict[str, Any]
        Dictionary mapping column names to their types, for example {"name": str, "age": int}
    primary_key : str | Sequence[str]
        String name of column to use as a primary key, or a sequence of strings for a compound primary key covering multiple columns
    engine : SqlAlchemy Engine
    schema : Optional[str]
    autoincrement : Optional[bool] default, None
        Autoincrement primary key
    if_exists : Optional[str] default, 'error'

    Returns
    -------
    sqlalchemy.Table
    """
    cols = []
    
    for col_name, python_type in columns.items():
        sa_type = type_convert._type_convert[python_type]
        if type(primary_key) is str:
            primary_key = [primary_key]
        if col_name in primary_key:
            col = sa.Column(col_name, sa_type,
                            primary_key=True,
                            autoincrement=autoincrement)
        else:
            col = sa.Column(col_name, sa_type)
        cols.append(col)

    metadata = sa_orm.get_metadata_with_engine(engine, schema)
    table = sa.Table(name, metadata, *cols, schema=schema)
    if if_exists == 'replace':
        drop_table_sql = sa.schema.DropTable(table, if_exists=True)
        with engine.connect() as con:
            con.execute(drop_table_sql)
    table_creation_sql = sa.schema.CreateTable(table)
    with engine.connect() as con:
        con.execute(table_creation_sql)
    return sa_orm.get_table_from_engine(name, engine, schema=schema)


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


def create_table_from_rows_with_engine(
    table_name: str,
    rows:  Sequence[Dict[str, Any]],
    primary_key: Sequence[str],
    engine: Engine,
    column_types: Optional[Sequence] = None,
    schema: Optional[str] = None,
    autoincrement: Union[bool, Literal['auto', 'ignore_fk']] = False,
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
    cols = dict(zip(col_names, column_types))
    return create_table_with_engine(table_name, cols, primary_key, engine, schema, autoincrement, if_exists)