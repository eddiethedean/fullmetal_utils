from typing import Generator, List, Optional
import sqlalchemy as sa
from sqlalchemy import Engine, MetaData, Connection, create_engine, text

from fullmetal_utils.row import row_to_dict
from fullmetal_utils.table import Table
from fullmetal_utils.tables import drop_tables, get_table_names


class Database:
    def __init__(
        self,
        engine: Optional[Engine] = None,
        schema: Optional[str] = None,
        recreate: Optional[bool] = None,
        memory: Optional[bool] = None
    ) -> None:
        """
        If you want to recreate a database from scratch
        (first clearing the existing database if it already exists)
        you can use the recreate=True argument:
        db = Database(engine, recreate=True)
        """
        if memory:
            self.engine = create_engine('sqlite://')
        else:
            self.engine = engine

        self.schema = schema

        if recreate:
            drop_tables(engine, schema)

    def __getitem__(self, name: str) -> Table:
        return self.table(name)
    
    @property
    def tables(self) -> List[Table]:
        return [Table(self.engine, name, self.schema) for name in self.table_names()]
    
    def table_names(self, schema=None) -> List[str]:
        return get_table_names(self.engine, self.schema)

    def table(self, name: str) -> Table:
        return Table(self.engine, name, self.schema)

    def query(self, sql: str) -> Generator[dict, None, None]:
        """
        The db.query(sql) function executes a SQL query and returns a generator
        of Python dictionaries representing the resulting rows:
        db = Database(memory=True)
        db["dogs"].insert_all([{"name": "Cleo"}, {"name": "Pancakes"}])
        for row in db.query("select * from dogs"):
            print(row)
        # Outputs:
        # {'name': 'Cleo'}
        # {'name': 'Pancakes'}
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(sql))
            for row in result:
                yield row_to_dict(row)
