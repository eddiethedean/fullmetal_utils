from typing import Iterable, Optional
from sqlalchemy import Engine
from sqlalchemy.orm import Session

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

    def insert_all(self, rows: Iterable[dict], pks=[]) -> None:
        """
        Create new table from rows if table doesn't exist yet.
        Insert rows into table.
        """
        with Session(self.engine) as session:
            connection = connection_from_session(session)
            table = get_table(self.name, connection, self.schema)
            insert_records_session(table, rows, session)