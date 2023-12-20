from typing import Optional
from sqlalchemy import Engine, MetaData, Connection


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
        """
        self.engine = engine
        if recreate:
            clear_database(engine, schema)


def clear_database(
    connection: Engine | Connection,
    schema: Optional[str]
) -> None:
    my_metadata: MetaData = MetaData(schema=schema)
    my_metadata.reflect(bind=connection, schema=schema, resolve_fks=False)
    my_metadata.drop_all(bind=connection)