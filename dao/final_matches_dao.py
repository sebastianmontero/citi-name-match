from sqlalchemy.sql import text
from domain.objects import FinalMatches


class FinalMatchesDao(object):

    @staticmethod
    def insert(conn, inserts):
        print('Inserting final matches...')
        conn.execute(FinalMatches.get_table().insert(), inserts)

    @staticmethod
    def truncate(conn):
        print('Truncating final matches...')
        sql = text("truncate final_matches")
        conn.execute(sql)
