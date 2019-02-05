import pathlib
import random
import re
from db_manager import DBManager
from dao.possible_matches_doc_int_idx_dao import PossibleMatchesDocIntIdxDao
from dao.final_matches_dao import FinalMatchesDao


class FinalFilter(object):

    def filter(self):
        with DBManager.get_engine().connect() as conn:
            FinalMatchesDao.truncate(conn)
            matches = PossibleMatchesDocIntIdxDao.select(conn)
            final_map = self._create_final_map(matches)

            print('Picking final matches...')
            final_matches = []
            for _, clabe_map in final_map.items():
                best = None
                for _, agrupador_obj in clabe_map.items():
                    if not best:
                        best = agrupador_obj
                    else:
                        best = self._pick_best(best, agrupador_obj)
                final_matches.append(best)

            FinalMatchesDao.insert(conn, final_matches)

    def _create_final_map(self, matches):
        print('Creating final map...')
        final_map = {}
        for match in matches:
            clabe = match['clabe_externo']
            agrupador_id = match['agrupador_ids'].split(';')[0]
            total = match['total']

            if clabe not in final_map:
                clabe_map = {}
                final_map[clabe] = clabe_map
            else:
                clabe_map = final_map[clabe]

            if agrupador_id not in clabe_map:
                agrupador_obj = {
                    'clabe': clabe,
                    'agrupador_id': int(agrupador_id),
                    'total': 0,
                    'count': 0
                }
                clabe_map[agrupador_id] = agrupador_obj
            else:
                agrupador_obj = clabe_map[agrupador_id]

            if total > agrupador_obj['total']:

                agrupador_obj['nombre_interno'] = match['nombre_interno']
                agrupador_obj['nombre_externo'] = match['nombre_externo']
                agrupador_obj['token_set_ratio'] = match['token_set_ratio']
                agrupador_obj['similarity'] = match['similarity']
                agrupador_obj['ratio'] = match['ratio']
                agrupador_obj['partial_ratio'] = match['partial_ratio']
                agrupador_obj['in_dict_percentage'] = match['in_dict_percentage']
                agrupador_obj['stripped_length'] = match['stripped_length']
                agrupador_obj['total'] = match['total']

            agrupador_obj['count'] += 1
        return final_map

    def _pick_best(self, obj1, obj2):
        if obj1['total'] == obj2['total']:
            return obj1 if obj1['count'] > obj2['count'] else obj2
        else:
            return obj1 if obj1['total'] > obj2['total'] else obj2


ff = FinalFilter()
ff.filter()
