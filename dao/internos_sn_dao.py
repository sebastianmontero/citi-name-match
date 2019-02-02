from sqlalchemy.sql import text
from domain.objects import InternosSN


class InternosSNDao(object):

    @staticmethod
    def select_names(conn):
        sql = text("select nombre "
                   "from internos_sn ")
        return conn.execute(sql).fetchall()

    @staticmethod
    def select(conn):
        sql = text("select nombre, "
                   "agrupadores, "
                   "tipos "
                   "from internos_sn ")
        return conn.execute(sql).fetchall()

    @staticmethod
    def delete(conn):
        print('Deleting internos_sn...')
        sql = text("delete from internos_sn")
        conn.execute(sql)

    @staticmethod
    def insert(conn, inserts):
        print('Inserting internos_sn...')
        conn.execute(InternosSN.get_table().insert(), inserts)
