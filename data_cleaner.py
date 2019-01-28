import os
import time
import math
import re
from db_manager import DBManager
from sqlalchemy.sql import text
from objects import InternosSN, ExternosSN


class DataCleaner(object):

    def clean_name(self, name):
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
            r'\s*(cia)?(compa.?ia)?\b(de?)?\bc[\.\s]?v[\.\s]?\b',
            r'\b(cia)?(compa.ia)?\bs[\.\s]?a[\.\s]?\b',
            r'\bfidei?c?(om)?(omiso(s)?)?\b',
            r'\b(gpo|grupo)\s*finan?(ciero)?\b',
            r'^[0-9\s]+$',
            r'\bm.xico\b',
            r'\bde\b',
            r'\bdel\b',
            r'\bla\b',
            r'\by\b'
        ]

        for term in terms:
            name = re.sub(term, ' ', name)
        return name.strip()

    def _process_name(self, name):
        return self._remove_common_terms(self.clean_name(name))

    def _load_raw_internal_clients(self, conn):
        sql = text("select num_cliente, "
                   "nombre, "
                   "grupo, "
                   "agrupador "
                   "from internos ")
        return conn.execute(sql).fetchall()

    def _load_raw_external_clients(self, conn):
        sql = text("select clabe, "
                   "nombre_largo, "
                   "nombre_corto "
                   "from externos ")
        return conn.execute(sql).fetchall()

    def _load_internal_sn_names(self, conn):
        sql = text("select nombre "
                   "from internos_sn ")
        return conn.execute(sql).fetchall()

    def _load_external_sn_names(self, conn):
        sql = text("select nombre "
                   "from externos_sn ")
        return conn.execute(sql).fetchall()

    def _load_all_names(self, conn):
        names = self._load_internal_sn_names(
            conn) + self._load_external_sn_names(conn)
        return [name['nombre'] for name in names]

    def _count_terms(self, names):
        word_count = {}
        for name in names:
            terms = re.split(r'[ \.,]', name)
            for term in terms:
                if len(term.strip()) > 0:
                    if term not in word_count:
                        word_count[term] = {
                            'term': term,
                            'count': 0
                        }
                    word_count[term]['count'] += 1
        return word_count

    def display_common_terms(self):
        with DBManager.get_engine().connect() as conn:
            names = self._load_all_names(conn)
            print('Number of names: ', len(names))
        word_count = list(self._count_terms(names).values())
        word_count.sort(key=lambda x: x['count'], reverse=True)
        for i in range(200):
            print(word_count[i])

    def _update_internal_clients(self, conn, updates):
        sql = text("update internos set "
                   "nombre_p = :nombre, "
                   "grupo_p = :grupo, "
                   "agrupador_p = :agrupador "
                   "where num_cliente = :num_cliente ")
        return conn.execute(sql, updates)

    def _update_external_clients(self, conn, updates):
        sql = text("update externos set "
                   "nombre_largo = :nombre_largo, "
                   "nombre_corto = :nombre_corto "
                   "where clabe = :clabe ")
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
        self._update_single_name_internals(conn, updates)

    def _clean_external_clients(self, conn):
        print('Cleaning external clients...')
        external = self._load_raw_external_clients(conn)
        updates = []
        for row in external:
            nombre_largo = row['nombre_largo']
            nombre_corto = row['nombre_corto']

            nombre_largo = self._process_name(nombre_largo)
            if nombre_corto != row['nombre_largo']:
                nombre_corto = self._process_name(nombre_corto)
            else:
                nombre_corto = nombre_largo
            updates.append({
                'clabe': row['clabe'],
                'nombre_largo': nombre_largo,
                'nombre_corto': nombre_corto
            })
        print('Finished cleaning data. Updating external clients table...')
        """ self._update_external_clients(
            conn, updates) """
        self._update_single_name_externals(conn, updates)

    def _update_single_name_internals(self, conn, internals):
        print('Updating internos sn')
        internal_map = {}
        for internal in internals:
            nombre = internal['nombre']
            grupo = internal['grupo']
            agrupador = internal['agrupador']
            levels = [
                {
                    'nombre': nombre,
                    'agrupador': agrupador,
                    'tipo': 'nombre'
                },
                {
                    'nombre': grupo,
                    'agrupador': agrupador,
                    'tipo': 'grupo'
                },
                {
                    'nombre': agrupador,
                    'agrupador': agrupador,
                    'tipo': 'agrupador'
                },
            ]
            for level in levels:
                lnombre = level['nombre']
                lagrupador = level['agrupador']
                ltipo = level['tipo']
                if lnombre not in internal_map:
                    internal_map[lnombre] = {
                        'nombre': lnombre,
                        'agrupadores': lagrupador,
                        'tipos': ltipo
                    }
                else:
                    if internal_map[lnombre]['agrupadores'].find(lagrupador) == -1:
                        internal_map[lnombre]['agrupadores'] += ';' + \
                            lagrupador
                    if internal_map[lnombre]['tipos'].find(ltipo) == -1:
                        internal_map[lnombre]['tipos'] += ';' + ltipo
        self._truncate_single_name_internals(conn)
        self._insert_single_name_internals(conn, list(internal_map.values()))

    def _update_single_name_externals(self, conn, externals):
        print('Updating externos sn')
        external_map = {}
        for external in externals:
            nombre_largo = external['nombre_largo']
            nombre_corto = external['nombre_corto']
            clabe = external['clabe']
            levels = [
                {
                    'nombre': nombre_largo,
                    'clabe': clabe
                },
                {
                    'nombre': nombre_corto,
                    'clabe': clabe
                }
            ]
            for level in levels:
                lnombre = level['nombre']
                lclabe = level['clabe']
                if len(lnombre) > 0:
                    if lnombre not in external_map:
                        external_map[lnombre] = {
                            'nombre': lnombre,
                            'clabes': lclabe
                        }
                    else:
                        external_map[lnombre]['clabes'] += ';' + clabe
        self._truncate_single_name_externals(conn)
        self._insert_single_name_externals(conn, list(external_map.values()))

    def _truncate_single_name_internals(self, conn):
        print('Deleting internos_sn...')
        sql = text("delete from internos_sn")
        conn.execute(sql)

    def _truncate_single_name_externals(self, conn):
        print('Deleting externos_sn...')
        sql = text("delete from externos_sn")
        conn.execute(sql)

    def _insert_single_name_internals(self, conn, inserts):
        print('Inserting internos_sn...')
        conn.execute(InternosSN.get_table().insert(), inserts)

    def _insert_single_name_externals(self, conn, inserts):
        print('Inserting externos_sn...')
        slice_size = 50000
        num_slices = math.ceil(len(inserts)/slice_size)
        for i in range(num_slices):
            conn.execute(ExternosSN.get_table().insert(),
                         inserts[i*slice_size:(i + 1)*slice_size])

    def clean(self):
        with DBManager.get_engine().connect() as conn:
            self._clean_internal_clients(conn)
            self._clean_external_clients(conn)


""" cleaner = DataCleaner()
cleaner.clean() """
""" cleaner.display_common_terms() """
