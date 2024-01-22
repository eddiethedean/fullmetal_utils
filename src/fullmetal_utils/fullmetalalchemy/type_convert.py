"""
Functions for converting between Python and SQL data types.
"""

import decimal as _decimal
import datetime as _datetime
import typing as _t

from sqlalchemy import types as _sqltypes


def sql_type(t):
    return _type_convert[t]


def python_type(t):
    return _sql_to_python[t]


_type_convert = {
    int: _sqltypes.Integer,
    str: _sqltypes.Unicode,
    float: _sqltypes.Float,
    _decimal.Decimal: _sqltypes.Numeric,
    _datetime.datetime: _sqltypes.DateTime,
    bytes: _sqltypes.LargeBinary,
    bool: _sqltypes.Boolean,
    _datetime.date: _sqltypes.Date,
    _datetime.time: _sqltypes.Time,
    _datetime.timedelta: _sqltypes.Interval,
    list: _sqltypes.ARRAY,
    dict: _sqltypes.JSON
}

_sql_to_python = {
    _sqltypes.Integer: int,
    _sqltypes.SmallInteger: int,
    _sqltypes.SMALLINT: int,
    _sqltypes.BigInteger: int,
    _sqltypes.BIGINT: int,
    _sqltypes.INTEGER: int,
    _sqltypes.Unicode: str,
    _sqltypes.NVARCHAR: str,
    _sqltypes.NCHAR: str,
    _sqltypes.Float: _decimal.Decimal,
    _sqltypes.REAL: _decimal.Decimal,
    _sqltypes.FLOAT: _decimal.Decimal,
    _sqltypes.Numeric: _decimal.Decimal,
    _sqltypes.NUMERIC: _decimal.Decimal,
    _sqltypes.DECIMAL: _decimal.Decimal,
    _sqltypes.DateTime: _datetime.datetime,
    _sqltypes.TIMESTAMP: _datetime.datetime,
    _sqltypes.DATETIME: _datetime.datetime,
    _sqltypes.LargeBinary: bytes,
    _sqltypes.BLOB: bytes,
    _sqltypes.Boolean: bool,
    _sqltypes.BOOLEAN: bool,
    _sqltypes.MatchType: bool,
    _sqltypes.Date: _datetime.date,
    _sqltypes.DATE: _datetime.date,
    _sqltypes.Time: _datetime.time,
    _sqltypes.TIME: _datetime.time,
    _sqltypes.Interval: _datetime.timedelta,
    _sqltypes.ARRAY: list,
    _sqltypes.JSON: dict
}


def get_sql_types(data: _t.Mapping[str, _t.Sequence]) -> list:
    return [get_sql_type(values) for values in data.values()]


def get_sql_type(values: _t.Sequence) -> _t.Any:
    for python_type in _type_convert:
        if all(type(val) == python_type for val in values):
            return _type_convert[python_type]
    return _type_convert[str]