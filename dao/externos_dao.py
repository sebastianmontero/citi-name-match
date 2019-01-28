from sqlalchemy.sql import text


class ExternosDao(object):

    @staticmethod
    def select(conn):
        sql = text("select clabe, "
                   "nombre_largo, "
                   "nombre_corto "
                   "from externos ")
        return conn.execute(sql).fetchall()

    @staticmethod
    def update(conn, updates):
        sql = text("update externos set "
                   "nombre_largo = :nombre_largo, "
                   "nombre_corto = :nombre_corto "
                   "where clabe = :clabe ")
        return conn.execute(sql, updates)
