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
        """ s de rl de cv """
        """ sa[v,api,b] de cv """
        """ s en nc de cv """
        """de cv """
        """sa """
        terms = [
            r'\s*(cia)?(compa.?ia)?(\s*s[\.\s]?(c[\.\s]?)?(p[\.\s]?r[\.\s]?)?)?\s*(de?)?\s*r[\.\s]?l[\.\s]?\s*de?\s*c[\.\s]?v[\.\s]?',
            r'\s*(cia)?(compa.?ia)?\s*s[\.\s]?[acs][\.\s]?(b[\.\s]?)?(p[\.\s]?i[\.\s]?)?(v[\.\s]?)?\s*de?\s*c[\.\s]?v[\.\s]?',
            r'\s*(cia)?(compa.?ia)?\s*(s[\.\s]?)?(en)?\s*n[\.\s]?c[\.\s]?\s*de?\s*c[\.\s]?v[\.\s]?',
            r'\s*(cia)?(compa.?ia)?\s+de?\s+c[\.\s]?v[\.\s]?\b',
            r'\b(cia)?(compa.?ia)?\bs[\.\s]?a[\.\s]?\b',
            r'\bfidei?c?(omiso)?\b',
            r'\b(gpo|grupo)\s*finan?(ciero)?\b'
        ]

        for term in terms:
            name = re.sub(term, '', name)
        return name.strip()

    def _process_name(self, name):
        return self._remove_common_terms(self._clean_name(name))

    def _load_raw_internal_clients(self, conn):
        sql = text("select num_cliente, "
                   "nombre, "
                   "grupo, "
                   "agrupador "
                   "from internos ")
        return conn.execute(sql).fetchall()

    def _update_internal_clients(self, conn, updates):
        sql = text("update internos set "
                   "nombre_p = :nombre, "
                   "grupo_p = :grupo, "
                   "agrupador_p = :agrupador "
                   "where num_cliente = :num_cliente ")
        return conn.execute(sql, updates)

    def _clean_internal_clients(self, conn):
        print('Cleaning internal clients...')
        internal = self._load_raw_internal_clients(conn)
        updates = []
        for row in internal:
            nombre = self._process_name(row['nombre'])
            nombre = re.sub(r'^agrupador:?\s*', '', nombre)
            grupo = self._process_name(row['grupo'])
            agrupador = self._process_name(row['agrupador'])

            if grupo == 'por clasificar' or grupo == '':
                grupo = nombre
            if agrupador == 'por clasificar' or agrupador == '':
                agrupador = grupo
            updates.append({
                'num_cliente': row['num_cliente'],
                'nombre': nombre,
                'grupo': grupo,
                'agrupador': agrupador
            })
        print('Finished cleaning data. Updating internal clients table...')
        self._update_internal_clients(
            conn, updates)

    def clean(self):
        with DBManager.get_engine().connect() as conn:
            self._clean_internal_clients(conn)


cleaner = DataCleaner()
cleaner.clean()
