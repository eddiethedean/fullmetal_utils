from typing import List, Optional

from sqlalchemy import Connection, Engine, MetaData, inspect


def drop_tables(
    connection: Engine | Connection,
    schema: Optional[str]
) -> None:
    my_metadata: MetaData = MetaData(schema=schema)
    my_metadata.reflect(bind=connection, schema=schema, resolve_fks=False)
    my_metadata.drop_all(bind=connection)


def get_table_names(
    engine: Engine,
    schema: Optional[str] = None
) ->  List[str]:
    """
    Get a list of the names of tables in the database connected to the given engine.

    Parameters
    ----------
    engine : _sa_engine.Engine
        An SQLAlchemy engine instance connected to a database.
    schema : Optional[str], optional
        The name of the schema to filter by, by default None.

    Returns
    -------
    List[str]
        A list of table names.
    """
    return inspect(engine).get_table_names(schema)