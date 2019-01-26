import os
import time
import math
import re
from db_manager import DBManager
from sqlalchemy.sql import text


class DataCleaner(object):

    def _clean_name(self, name):
        return name.strip(' .,/').lower()

    def _remove_common_terms(self, name):
        terms = [r'\s*s\.?a\.?\s*de\s*c\.?v\.?']

        for term in terms:
            name = re.sub(term, '', name)
        return name

    def _process_name(self, name):
        return self._remove_common_terms(self._clean_name(name))

    def _load_raw_internal_clients(self, conn):
        sql = text("select num_cliente, "
                   "nombre, "
                   "grupo, "
                   "agrupador "
                   "from internos ")
        return conn.execute(sql).fetchall()

    def _update_internal_clients(self, conn, num_cliente, nombre, grupo, agrupador):
        sql = text("update internos set "
                   "nombre_p = :nombre, "
                   "grupo_p = :grupo, "
                   "agrupador = :agrupador "
                   "where num_cliente = :num_cliente ")
        return conn.execute(sql, num_cliente=num_cliente, nombre=nombre, grupo=grupo, agrupador=agrupador)

    def _clean_internal_clients(self, conn):
        internal = self._load_raw_internal_clients(conn)
        for row in internal:
            nombre = self._process_name(row['nombre'])
            nombre = re.sub(r'^agrupador:?\s*', '', nombre)
            grupo = self._process_name(row['grupo'])
            agrupador = self._process_name(row['agrupador'])
            if agrupador == 'por clasificar':
                if grupo == 'por clasificar':
                    agrupador = grupo = nombre
                else:
                    agrupador = grupo
            self._update_internal_clients(
                conn, row['num_cliente'], nombre, grupo, agrupador)

    def clean(self):
        with DBManager.get_engine().connect() as conn:
            self._clean_internal_clients(conn)


cleaner = DataCleaner()
cleaner.clean()
