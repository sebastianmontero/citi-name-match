import re
import math
from sqlalchemy import text
from db_manager import DBManager
from domain.objects import InternosSN, ExternosSN
from dao.internos_dao import InternosDao
from dao.externos_all_dao import ExternosAllDao
from dao.internos_sn_dao import InternosSNDao
from dao.externos_sn_dao import ExternosSNDao

class DataCleaner(object):


    fix_terms = [
        (r'(@|µ|ñ|;|#|\?)', 'n'),
        ('æ', 'i'),
        ('("|=)', ' '),
        ("&", ' and '),
        (r"\bbanco\b", 'bank'),
        (r"\bservicios\b", 'services'),
        (r"\bnacional\b", 'national'),
        (r"\binternacional\b", 'international'),
        (r"\b(corporacion|co)\b", 'corporation'),
        (r"\bcompañia\b", 'company'),
        (r"\bfid(e|ei|eicom)?\b", 'fideicomiso'),
        (r"\bgpo\b", 'grupo'),
        (r"(\(|\))", ' '),
        (r'/|-|,|\*|:|\|', r' '),
        (r'\.(\s|\.)*\.', r'.'),
        (r"\.(\w{2})", r' \1'),
        (r'\.(\w\b)', r'\1'),
        (r'((^|\s)\.|\.(\s|$))', r' '),
        (r"\b(\w+)\s+and\s+(\w{1})\b", r'\1and\2'),
        (r"\b(\w{1})\s+and\s+(\w+)\b", r'\1and\2'),
        (r"(\w*)\s*'\s*([\w']{2,})", r'\1\2 \1 \2 '),
        (r"(\w*)\s*'\s*(\w{1})\b", r'\1\2 \1 '),
        ("'", '')
    ]

    final_fix_terms = [
        (r"(?<=\b\w{1})\s+(?=\w{1}\b)", r'')
    ]

    """ s de rl de cv """
    """ sa[v,api,b] de cv """
    """ s en nc de cv """
    """de cv """
    """sa """
    common_terms = [
            r'\s*(cia)?(compa.?ia)?(\s*s[\.\s]?(c[\.\s]?)?(p[\.\s]?r[\.\s]?)?)?\s*(de?)?\s*r[\.\s]?l[\.\s]?\s*de?\s*c[\.\s]?v[\.\s]?',
            r'\s*(cia)?(compa.?ia)?\s*s[\.\s]?[acs][\.\s]?(b[\.\s]?)?(p[\.\s]?i[\.\s]?)?(v[\.\s]?)?\s*de?\s*c[\.\s]?v[\.\s]?',
            r'\s*(cia)?(compa.?ia)?\s*(s[\.\s]?)?(en)?\s*n[\.\s]?c[\.\s]?\s*de?\s*c[\.\s]?v[\.\s]?',
            r'\s*(cia)?(compa.?ia)?\b(de?)?\bc[\.\s]?v[\.\s]?\b',
            r'\b(cia)?(compa.ia)?\bs[\.\s]?a[\.\s]?\b',
            r'\bs\s*a\s*c\s*v\b',
            r'\bs\s*(de?)?\s*r\s*l(\s*m\s*i)?\s*(de?)?\s*(cv)?\b',
            r'\w{1}\s+\bs\s*n?\s*c\b',
            r'\bs\s*(de?)?\s*p\s*r\s*(de?)?\s*r\s*(i|l)\s*(cv)?\b',
            r'\bs\s*(de?)?\s*s\s*s\b',
            r'\bs\s*i\s*i\s*d\b',
            r'\bs\s*i\s*r\s*v\b',
            r'\bs\s+p\s+a\b',
            r'\bfidei?c?(om)?(omiso(s)?)?\b',
            r'\b(gpo|grupo)\s*finan?(ciero)?\b',
            r'^[0-9\s]+$',
            r'\bm.xico\b',
            r'\bde\b',
            r'\bdel\b',
            r'\bla\b',
            r'\by\b',
            r'\w{1}\s+\bof\b',
            r'\bltd\b',
            r'\bthe\b',
            r'\bempleados\b',
            r'\bxborder\b',
            r'\s+'
        ]

    def __init__(self):
        self._common_terms = [(re.compile(term), ' ')
                              for term in DataCleaner.common_terms]
        self._fix_terms = [(re.compile(term[0]), term[1])
                              for term in DataCleaner.fix_terms]
        self._final_fix_terms = [(re.compile(term[0]), term[1])
                              for term in DataCleaner.final_fix_terms]

    def _clean_name(self, name):
        return name.strip(' .,/').lower()

    def replace_terms(self, name, terms):
        for term in terms:
            name = term[0].sub(term[1], name)
        return name.strip()

    def _remove_common_terms(self, name):
        return self.replace_terms(name, self._common_terms)

    def _apply_fix_terms(self, name):
        return self.replace_terms(name, self._fix_terms)

    def _apply_final_fix_terms(self, name):
        return self.replace_terms(name, self._final_fix_terms)

    def process_name(self, name):
        name = self._clean_name(name)
        name = self._apply_fix_terms(name)
        name = self._remove_common_terms(name)
        name = self._apply_final_fix_terms(name)

        return name

    def extract_names(self, names_sn):
        return [name_sn['nombre'] for name_sn in names_sn]

    def clean(self):
        with DBManager.get_engine().connect() as conn:
            print('Selecting internal clients...')
            internals = InternosDao.select(conn)
            internal_sn = self.clean_internal_clients(internals)
            self._truncate_single_name_internals(conn)
            self._insert_single_name_internals(conn, internal_sn)
            """ print('Selecting external clients...')
            externals = ExternosAllDao.select(conn)
            external_sn = self.clean_external_clients(externals)
            self._truncate_single_name_externals(conn)
            self._insert_single_name_externals(conn, external_sn) """

    def clean_internal_clients(self, internal):
        print('Cleaning internal clients...')
        updates = []
        agrupador_re = re.compile(r'agrupador:?',flags=re.I)
        for row in internal:
            nombre = agrupador_re.sub('', row['nombre'])
            nombre = self.process_name(nombre)
            grupo = self.process_name(row['grupo'])
            agrupador = self.process_name(row['agrupador'])

            if agrupador != 'por clasificar' and grupo != 'por clasificar':
                if grupo == 'xborder' or grupo == 'fideicomiso':
                    grupo = ''
                updates.append({
                    'num_cliente': row['num_cliente'],
                    'nombre': nombre,
                    'grupo': grupo,
                    'agrupador': agrupador,
                    'agrupador_id': row['agrupador_id']
                })
        print('Finished cleaning data. ')
        return self._create_single_name_internals(updates)

    def clean_external_clients(self, external):
        print('Cleaning external clients...')
        updates = []
        for row in external:
            nombre = row['nombre']
            nombre = self.process_name(nombre)
            updates.append({
                'clabe': row['clabe'],
                'nombre': nombre
            })
        print('Finished cleaning data.')
        return self._create_single_name_externals(updates)

    def _create_single_name_internals(self, internals):
        print('Creating single name structure...')
        internal_map = {}
        for internal in internals:
            nombre = internal['nombre']
            grupo = internal['grupo']
            agrupador = internal['agrupador']
            agrupador_id = internal['agrupador_id']
            levels = [
                {
                    'nombre': nombre,
                    'agrupador_id': agrupador_id,
                    'tipo': 'nombre'
                },
                {
                    'nombre': grupo,
                    'agrupador_id': agrupador_id,
                    'tipo': 'grupo'
                },
                {
                    'nombre': agrupador,
                    'agrupador_id': agrupador_id,
                    'tipo': 'agrupador'
                },
            ]
            for level in levels:
                lnombre = level['nombre']
                lagrupador_id = str(level['agrupador_id'])
                ltipo = level['tipo']
                if len(lnombre) > 0:
                    if lnombre not in internal_map:
                        internal_map[lnombre] = {
                            'nombre': lnombre,
                            'agrupador_ids': lagrupador_id,
                            'tipos': ltipo
                        }
                    else:
                        if internal_map[lnombre]['agrupador_ids'].find(lagrupador_id) == -1:
                            internal_map[lnombre]['agrupador_ids'] += ';' + \
                                lagrupador_id
                        if internal_map[lnombre]['tipos'].find(ltipo) == -1:
                            internal_map[lnombre]['tipos'] += ';' + ltipo

        return list(internal_map.values())

    def _create_single_name_externals(self, externals):
        print('Creating single name externos...')
        external_map = {}
        for external in externals:
            nombre = external['nombre']
            clabe = external['clabe']
            if len(nombre) > 0:
                if nombre not in external_map:
                    external_map[nombre] = {
                        'nombre': nombre,
                        'clabes': clabe
                    }
                else:
                    if external_map[nombre]['clabes'].find(clabe) == -1:
                        external_map[nombre]['clabes'] += ';' + clabe

        return list(external_map.values())


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


""" cleaner = DataCleaner()
cleaner.clean() """

