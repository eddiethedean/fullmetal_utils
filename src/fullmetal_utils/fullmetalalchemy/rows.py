from typing import Any, Dict
import sqlalchemy as sa
from packaging import version


def row_to_dict(row: sa.engine.row.Row) -> Dict[str, Any]:
    if version.parse(sa.__version__) >= version.parse('1.4'):
        return dict(row._mapping)
    else:
        return dict(row)