from typing import Any, Optional
from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    notnull: bool = False
    default_value: Optional[Any] = None
    is_pk: bool = False