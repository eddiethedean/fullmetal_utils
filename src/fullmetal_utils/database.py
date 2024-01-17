from typing import Iterator, Optional
from sqlalchemy import Engine, MetaData, Connection, text


class DataBase:
    def __init__(
        self,
        engine: Engine,
        schema: Optional[str] = None,
        recreate: Optional[bool] = None
    ) -> None:
        """
        If you want to recreate a database from scratch
        (first clearing the existing database if it already exists)
        you can use the recreate=True argument:
        db = Database(engine, recreate=True)
        """
        self.engine = engine
        if recreate:
            clear_database(engine, schema)

    def query(self, sql: str) -> Iterator[dict]:
        """
        The db.query(sql) function executes a SQL query and returns an iterator
        over Python dictionaries representing the resulting rows:
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(sql))
            for row in result:
                yield row._mapping


def clear_database(
    connection: Engine | Connection,
    schema: Optional[str]
) -> None:
    my_metadata: MetaData = MetaData(schema=schema)
    my_metadata.reflect(bind=connection, schema=schema, resolve_fks=False)
    my_metadata.drop_all(bind=connection)