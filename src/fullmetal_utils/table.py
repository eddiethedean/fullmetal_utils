from typing import Iterable, List, Optional

import sqlalchemy as sa

from .fullmetalalchemy.columns import get_column_names
from .fullmetalalchemy.create import create_table_from_rows
from .fullmetalalchemy.insert import insert_records
from .fullmetalalchemy.tables import get_table_names

from fullmetal_utils.column import Column


class Table:
    def __init__(
        self,
        engine: sa.Engine,
        name: str,
        schema: Optional[str] = None
    ) -> None:
        self.engine = engine
        self.name = name
        self.schema = schema

    def __repr__(self) -> str:
        return f'<Table {self.name} {tuple(self.column_names())}>'
    
    @property
    def columns(self) -> List[Column]:
        return [Column()]

    def column_names(self) -> List[str]:
        return get_column_names(self.name, self.engine, self.schema)

    def insert_all(self, rows: Iterable[dict], pks=[]) -> None:
        """
        Create new table from rows if table doesn't exist yet.
        Insert rows into table.
        """
        if self.name not in get_table_names(self.engine, self.schema):
            create_table_from_rows(self.name, rows, pks, self.engine, schema=self.schema)

        insert_records(self.name, rows, self.engine, self.schema)


