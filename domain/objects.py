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


class PossibleMatchesDocIntIdx(Base, BaseTable):
    __tablename__ = 'possible_matches_doc_int_idx'
    nombre_interno = Column(String(), primary_key=True)
    nombre_externo = Column(String(), primary_key=True)
    clabe_externo = Column(String(), nullable=False)
    agrupador_ids = Column(String(), nullable=False)
    token_set_ratio = Column(SmallInteger(), nullable=False)
    partial_ratio = Column(SmallInteger(), nullable=False)
    stripped_length = Column(SmallInteger(), nullable=False)
    total = Column(Integer(), nullable=False)
    similarity = Column(Numeric(precision=10, scale=9), nullable=False)
    in_dict_percentage = Column(Numeric(precision=5, scale=4), nullable=False)


class PossibleMatchesDocExtIdx(Base, BaseTable):
    __tablename__ = 'possible_matches_doc_ext_idx'
    nombre_interno = Column(String(), primary_key=True)
    nombre_externo = Column(String(), primary_key=True)
    clabe_externo = Column(String(), nullable=False)
    agrupador_ids = Column(String(), nullable=False)
    token_set_ratio = Column(SmallInteger(), nullable=False)
    partial_ratio = Column(SmallInteger(), nullable=False)
    stripped_length = Column(SmallInteger(), nullable=False)
    total = Column(Integer(), nullable=False)
    similarity = Column(Numeric(precision=10, scale=9), nullable=False)
    in_dict_percentage = Column(Numeric(precision=5, scale=4), nullable=False)


class InternosSN(Base, BaseTable):
    __tablename__ = 'internos_sn'
    internos_sn_id = Column(Integer(), primary_key=True)
    nombre = Column(String(), nullable=False)
    agrupador_ids = Column(String(), nullable=False)
    tipos = Column(String(), nullable=False)


class ExternosSN(Base, BaseTable):
    __tablename__ = 'externos_sn'
    externos_sn_id = Column(Integer(), primary_key=True)
    nombre = Column(String(), nullable=False)
    clabes = Column(String(), nullable=False)
