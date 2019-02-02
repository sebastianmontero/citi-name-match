from sqlalchemy.sql import text
from domain.objects import PossibleMatchesDocExtIdx


class PossibleMatchesDocExtIdxDao(object):

    @staticmethod
    def insert(conn, inserts):
        conn.execute(PossibleMatchesDocExtIdx.get_table().insert(), inserts)
