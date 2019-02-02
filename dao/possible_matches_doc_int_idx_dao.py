from sqlalchemy.sql import text
from domain.objects import PossibleMatchesDocIntIdx


class PossibleMatchesDocIntIdxDao(object):

    @staticmethod
    def insert(conn, inserts):
        conn.execute(PossibleMatchesDocIntIdx.get_table().insert(), inserts)
