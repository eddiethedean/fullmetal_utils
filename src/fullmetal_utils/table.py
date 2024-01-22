from typing import Any, Dict, Generator, List, Optional, Sequence

import sqlalchemy as sa

from .fullmetalalchemy.select import select_records_all_with_engine
from .fullmetalalchemy.columns import get_column_names_with_engine, get_column_types_with_engine
from .fullmetalalchemy.create import create_table_from_rows_with_engine
from .fullmetalalchemy.insert import insert_records_with_engine
from .fullmetalalchemy.tables import get_table_names_with_engine

from fullmetal_utils.column import Column


class Table:
    def __init__(
        self,
        engine: sa.engine.Engine,
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
        return [Column(name, type) for name, type in self.column_types().items()]
    
    @property
    def rows(self) -> Generator[Dict[str, Any], None, None]:
        return select_records_all_with_engine(self.name, self.engine)

    def column_names(self) -> List[str]:
        return get_column_names_with_engine(self.name, self.engine, self.schema)
    
    def column_types(self) -> Dict[str, Any]:
        return get_column_types_with_engine(self.name, self.engine, self.schema)

    def insert_all(self, rows: Sequence[Dict[str, Any]], pks=[]) -> None:
        """
        Create new table from rows if table doesn't exist yet.
        Insert rows into table.
        """
        if self.name not in get_table_names_with_engine(self.engine, self.schema):
            create_table_from_rows_with_engine(self.name, rows, pks, self.engine, schema=self.schema)

        insert_records_with_engine(self.name, rows, self.engine, self.schema)


