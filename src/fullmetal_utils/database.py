from typing import Any, Generator, List, Optional
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from .fullmetalalchemy.rows import row_to_dict
from .fullmetalalchemy.tables import drop_tables, get_table_names

from fullmetal_utils.table import Table


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
            self.engine = sa.create_engine('sqlite://')
        elif type(engine) is Engine:
            self.engine = engine
        else:
            raise Exception('Must pass engine or memory=True')

        self.schema = schema

        if recreate:
            drop_tables(self.engine, schema)

    def __getitem__(self, name: str) -> Table:
        return self.table(name)
    
    @property
    def tables(self) -> List[Table]:
        return [Table(self.engine, name, self.schema) for name in self.table_names()]
    
    def table_names(self) -> List[str]:
        return get_table_names(self.engine, self.schema)
    
    # TODO: view_names method

    def table(self, name: str) -> Table:
        return Table(self.engine, name, self.schema)

    def query(
        self,
        sql: str,
        parameters: Optional[Any] = None,
        *,
        execution_options: Optional[Any] = None
    ) -> Generator[dict, None, None]:
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
        results = self.execute(sql, parameters, execution_options=execution_options)
        for row in results:
            yield row_to_dict(row)

    def execute(
        self,
        sql: str,
        parameters: Optional[Any] = None,
        *,
        execution_options: Optional[Any] = None
    ) -> sa.engine.CursorResult:
        """
        A wrapper around .execute() on the underlying SqlAlchemy engine connection. 
        """
        with self.engine.connect() as connection:
            return connection.execute(sa.text(sql), parameters, execution_options=execution_options)