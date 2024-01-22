from typing import List, Optional

from sqlalchemy import MetaData, inspect
from sqlalchemy.engine import Engine


def drop_tables_with_engine(
    engine: Engine,
    schema: Optional[str]
) -> None:
    my_metadata: MetaData = MetaData(schema=schema)
    my_metadata.reflect(bind=engine, schema=schema, resolve_fks=False)
    my_metadata.drop_all(bind=engine)


def get_table_names_with_engine(
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