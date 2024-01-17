import sqlalchemy as sa
from packaging import version


def row_to_dict(row) -> dict:
    if version.parse(sa.__version__) >= version.parse('1.4'):
        return row._mapping
    else:
        return dict(row)