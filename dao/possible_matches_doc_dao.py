from sqlalchemy.sql import text
from domain.objects import PossibleMatchesDoc


class PossibleMatchesDocDao(object):

    @staticmethod
    def insert(conn, inserts):
        conn.execute(PossibleMatchesDoc.get_table().insert(), inserts)
