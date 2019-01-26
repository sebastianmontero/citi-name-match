from sqlalchemy import Column, Date, Numeric, Integer, String, SmallInteger
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BaseTable():
    @classmethod
    def get_table(cls):
        return cls.__table__


class Progress(Base, BaseTable):
    __tablename__ = 'progress'
    progress_id = Column(Integer(), primary_key=True)
    internos_sn_id_start = Column(Integer(), nullable=False)
    internos_sn_id_end = Column(Integer(), nullable=False)
    externos_sn_id = Column(Integer(), nullable=False)


class PossibleMatches(Base, BaseTable):
    __tablename__ = 'possible_matches'
    internos_sn_id = Column(Integer(), primary_key=True)
    externos_sn_id = Column(Integer(), primary_key=True)
    match_score = Column(Integer(), nullable=False)
