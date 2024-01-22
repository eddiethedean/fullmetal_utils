from typing import Union

from sqlalchemy import Engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.orm import Session

HasConnection = Union[Engine, Connection, Session]


def get_connection_from_hc(hc: HasConnection) -> Connection:
    if type(hc) is Connection:
        return hc
    if type(hc) is Engine:
        return hc.connect()
    if type(hc) is Session:
        return hc.connection()
    else:
        raise Exception('hc must be Connection, Engine, or Session')