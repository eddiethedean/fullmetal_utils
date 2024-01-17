from typing import Iterable, List, Optional

import sqlalchemy as sa
from sqlalchemy import Engine
from sqlalchemy.orm import Session
from fullmetal_utils.column import Column

from fullmetal_utils.insert import insert_records_session
from fullmetal_utils.sa_orm import connection_from_session, get_table


class Table:
    def __init__(
        self,
        engine: Engine,
        name: str,
        schema: Optional[str] = None
    ) -> None:
        self.engine = engine
        self.name = name
        self.schema = schema

    def __repr__(self) -> str:
        return f'<Table {self.name} {tuple()}>'
    
    @property
    def columns(self) -> List[Column]:
        ...

    def column_names(self) -> List[str]:
        with self.engine.connect() as connection:
            table = get_table(self.name, connection, self.schema)
        return get_column_names(table)

    def insert_all(self, rows: Iterable[dict], pks=[]) -> None:
        """
        Create new table from rows if table doesn't exist yet.
        Insert rows into table.
        """
        with Session(self.engine) as session:
            connection = connection_from_session(session)
            table = get_table(self.name, connection, self.schema)
            insert_records_session(table, rows, session)


def get_column_names(
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