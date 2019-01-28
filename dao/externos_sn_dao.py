import math
from sqlalchemy.sql import text
from domain.objects import ExternosSN


class ExternosSNDao(object):

    @staticmethod
    def select_names(conn):
        sql = text("select nombre "
                   "from externos_sn ")
        return conn.execute(sql).fetchall()

    @staticmethod
    def delete(conn):
        print('Deleting externos_sn...')
        sql = text("delete from externos_sn")
        conn.execute(sql)

    @staticmethod
    def insert(conn, inserts):
        print('Inserting externos_sn...')
        slice_size = 50000
        num_slices = math.ceil(len(inserts)/slice_size)
        for i in range(num_slices):
            conn.execute(ExternosSN.get_table().insert(),
                         inserts[i*slice_size:(i + 1)*slice_size])
