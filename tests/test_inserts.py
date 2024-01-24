import unittest

import sqlalchemy as sa
from sqlalchemy import  String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session

from fullmetal_utils.fullmetalalchemy.insert import insert_records_with_engine, insert_records_with_session
from fullmetal_utils.fullmetalalchemy.sa_orm import get_table_from_session
from fullmetal_utils.fullmetalalchemy.select import select_all_rows_with_table_session


class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.name!r})"


class TestBasic(unittest.TestCase):
    def setUp(self):
        self.engine = sa.create_engine('sqlite://')
        Base.metadata.create_all(self.engine)

    def test_insert_using_engine(self):
        records = [
            {'name': 'Odos Matthews'},
            {'name': 'Kayla Matthews'},
            {'name': 'Dexter Matthews'}
        ]
        insert_records_with_engine('users', records, self.engine)
        
        session = Session(self.engine)
        rows = select_all_rows_with_table_session(User, session)
        self.assertDictEqual({'id': 1, 'name': 'Odos Matthews'}, rows[0])
        self.assertDictEqual({'id': 2, 'name': 'Kayla Matthews'}, rows[1])
        self.assertDictEqual({'id': 3, 'name': 'Dexter Matthews'}, rows[2])

    def test_insert_using_session(self):
        records = [
            {'name': 'Olivia'},
            {'name': 'Noah'},
            {'name': 'Emma'}
        ]
        with Session(self.engine) as session:
            insert_records_with_session('users', records, session)
            session.commit()
        
        with Session(self.engine) as session:
            rows = select_all_rows_with_table_session(User, session)
        self.assertDictEqual({'id': 1, 'name': 'Olivia'}, rows[0])
        self.assertDictEqual({'id': 2, 'name': 'Noah'}, rows[1])
        self.assertDictEqual({'id': 3, 'name': 'Emma'}, rows[2])