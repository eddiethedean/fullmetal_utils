from typing import Optional
from sqlalchemy import Engine


class Table:
    def __init__(
        self,
        engine: Engine,
        schema: Optional[str] = None,
        recreate: Optional[bool] = None
    ) -> None:
        ...