from typing import Any, Optional
from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    cid: int
    name: str
    type: str
    notnull: bool
    default_value: Optional[Any] = None
    is_pk: bool = False