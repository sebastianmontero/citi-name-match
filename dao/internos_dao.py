from sqlalchemy.sql import text


class InternosDao(object):

    @staticmethod
    def select(conn):
        sql = text("select num_cliente, "
                   "nombre, "
                   "grupo, "
                   "agrupador, "
                   "agrupador_id "
                   "from internos "
                   "where agrupador_id != 1")
        return conn.execute(sql).fetchall()

    @staticmethod
    def update(conn, updates):
        sql = text("update internos set "
                   "nombre_p = :nombre, "
                   "grupo_p = :grupo, "
                   "agrupador_p = :agrupador "
                   "where num_cliente = :num_cliente ")
        return conn.execute(sql, updates)
