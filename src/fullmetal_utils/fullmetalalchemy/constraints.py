from typing import List, Tuple, Union
import sqlalchemy as sa


def get_primary_key_constraints_with_table(
    table: sa.Table
) -> Tuple[Union[str, None],  List[str]]:
    """
    Get the primary key constraints of a SQLAlchemy table.

    Parameters
    ----------
    table : sqlalchemy.Table
        The SQLAlchemy table object to get the primary key constraints of.

    Returns
    -------
    Tuple[Optional[str], List[str]]
        A tuple with the primary key constraint name (if it exists) and a list of
        the column names that make up the primary key.

    Example
    -------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.get_primary_key_constraints(table)
    (None, ['id'])
    """
    cons = table.constraints
    for con in cons:
        if isinstance(con, sa.PrimaryKeyConstraint):
            return str(con.name), [col.name for col in con.columns]
    return (None, [])


def missing_primary_key_with_table(
    table: sa.Table,
) -> bool:
    """
    Check if a sqlalchemy table has a primary key.

    Parameters
    ----------
    table : sqlalchemy.Table
        The table to check.

    Returns
    -------
    bool
        True if the table doesn't have a primary key, False otherwise.

    Examples
    --------
    >>> import fullmetalalchemy as fa

    >>> engine, table = fa.get_engine_table('sqlite:///data/test.db', 'xy')
    >>> fa.features.missing_primary_key(table)
    False
    """
    pks = get_primary_key_constraints_with_table(table)
    return pks[1] == []