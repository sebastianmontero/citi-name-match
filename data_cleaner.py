import re


class DataCleaner(object):

    common_terms = ['a', 'ac', 'and', 'b', 'c', 'cv', 'd', 'de',
                    'del', 'e', 'en', 'f', 'g', 'h', 'i', 'l', 'la', 'los', 'ltd',
                    'm', 'n' 'o', 'of', 'p', 'plc', 's', 'sa', 'sab',
                    'sc', 'r', 'rl', 't', 'the', 'u', 'v', 'y', ',', r'\.', '&', r'\(', r'\)']

    fix_terms = [('@', 'ñ'), ("'", '')]

    def __init__(self):
        self._common_terms = [(r'\b'+term+r'\b', ' ')
                              for term in DataCleaner.common_terms]

    def _clean_name(self, name):
        return name.strip(' .,/').lower()

    def _replace_terms(self, name, terms):
        for term in terms:
            name = re.sub(term[0], term[1], name)
        return name.strip()

    def _remove_common_terms(self, name):
        return self._replace_terms(name, self._common_terms)

    def _fix_terms(self, name):
        return self._replace_terms(name, DataCleaner.fix_terms)

    def process_name(self, name):
        name = self._clean_name(name)
        name = self._fix_terms(name)
        name = self._remove_common_terms(name)

        return name

    def extract_names(self, names_sn):
        return [name_sn['nombre'] for name_sn in names_sn]

    def clean_internal_clients(self, internal):
        print('Cleaning internal clients...')
        updates = []
        for row in internal:
            nombre = self.process_name(row['nombre'])
            nombre = re.sub(r'^agrupador:?\s*', '', nombre)
            grupo = self.process_name(row['grupo'])
            agrupador = self.process_name(
                row['agrupador'])

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
        print('Finished cleaning data. ')
        return self._create_single_name_internals(updates)

    def clean_external_clients(self, external):
        print('Cleaning external clients...')
        updates = []
        for row in external:
            nombre_largo = row['nombre_largo']
            nombre_corto = row['nombre_corto']

            nombre_largo = self.process_name(nombre_largo)
            if nombre_corto != row['nombre_largo']:
                nombre_corto = self.process_name(nombre_corto)
            else:
                nombre_corto = nombre_largo
            updates.append({
                'clabe': row['clabe'],
                'nombre_largo': nombre_largo,
                'nombre_corto': nombre_corto
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

        return list(internal_map.values())

    def _create_single_name_externals(self, externals):
        print('Creating single name externos...')
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

        return list(external_map.values())
