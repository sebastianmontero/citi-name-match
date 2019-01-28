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
    externos_sn_id_start = Column(Integer(), nullable=False)
    externos_sn_id_end = Column(Integer(), nullable=False)
    internos_sn_id = Column(Integer(), nullable=False)


class PossibleMatches(Base, BaseTable):
    __tablename__ = 'possible_matches'
    internos_sn_id = Column(Integer(), primary_key=True)
    externos_sn_id = Column(Integer(), primary_key=True)
    match_score = Column(Integer(), nullable=False)


class PossibleMatchesDoc(Base, BaseTable):
    __tablename__ = 'possible_matches_doc'
    clabe_externo = Column(String(), primary_key=True)
    nombre_interno = Column(String(), primary_key=True)
    agrupadores = Column(String(), nullable=False)
    score = Column(Numeric(precision=15, scale=14), nullable=False)


class InternosSN(Base, BaseTable):
    __tablename__ = 'internos_sn'
    internos_sn_id = Column(Integer(), primary_key=True)
    nombre = Column(String(), nullable=False)
    agrupadores = Column(String(), nullable=False)
    tipos = Column(String(), nullable=False)


class ExternosSN(Base, BaseTable):
    __tablename__ = 'externos_sn'
    externos_sn_id = Column(Integer(), primary_key=True)
    nombre = Column(String(), nullable=False)
    clabes = Column(String(), nullable=False)
