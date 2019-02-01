from sqlalchemy.sql import text


class ExternosAllDao(object):

    @staticmethod
    def select(conn):
        sql = text("select clabe, "
                   "nombre "
                   "from externos_all ")
        return conn.execute(sql).fetchall()
