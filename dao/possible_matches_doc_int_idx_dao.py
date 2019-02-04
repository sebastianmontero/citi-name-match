from sqlalchemy.sql import text
from domain.objects import PossibleMatchesDocIntIdx


class PossibleMatchesDocIntIdxDao(object):

    @staticmethod
    def insert(conn, inserts):
        conn.execute(PossibleMatchesDocIntIdx.get_table().insert(), inserts)

    @staticmethod
    def truncate(conn):
        print('Truncating possible matches doc int idx...')
        sql = text("truncate possible_matches_doc_int_idx")
        conn.execute(sql)
