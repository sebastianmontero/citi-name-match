import os
import time
import math
from time import sleep
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from db_manager import DBManager
from sqlalchemy.sql import text
from fuzzywuzzy import fuzz
from dao_objects import PossibleMatches, Progress


class MatchWorker(object):

    def load_internal_clients(self, conn):
        sql = text("select internos_sn_id, "
                   "nombre "
                   "from internos_sn ")
        return conn.execute(sql).fetchall()

    def load_external_clients(self, conn, limit, offset):
        sql = text("select externos_sn_id, "
                   "nombre "
                   "from externos_sn "
                   "limit :limit offset :offset")
        return conn.execute(sql, limit=limit, offset=offset).fetchall()

    def test(self, msg):
        print(msg)
        return msg

    def match(self, start_external, end_external, start_internal=0):
        print('Matching from start: {} end: {}'.format(
            start_external, end_external))
        with DBManager.get_engine().connect() as conn:
            internal = self.load_internal_clients(conn)
            external = self.load_external_clients(
                conn,
                end_external - start_external,
                start_external)
            start_time = time.time()
            for i in range(len(external)):
                for j in range(start_internal, len(internal)):
                    external_name = external[i]['nombre']
                    internal_name = internal[j]['nombre']
                    score = fuzz.token_set_ratio(
                        internal_name, external_name)
                    if score > 85:
                        print('Possible match, score: {} internal: {} external: {}'.format(
                            score, internal_name, external_name))
                        conn.execute(PossibleMatches.get_table().insert().values(
                            internos_sn_id=internal[j]['internos_sn_id'],
                            externos_sn_id=external[i]['externos_sn_id'],
                            match_score=score))
                print('External progress: {}/{} ({:.4%}) Start: {} End: {} Running time: {}'.format(
                    i, len(external), i/len(external), start_external, end_external, self._get_elapsed_time(start_time)))
                start_internal = 0

    def _get_elapsed_time(self, start_time):
        seconds = int(time.time() - start_time)
        hrs = seconds // 3600
        r = seconds % 3600
        mins = r // 60
        secs = r % 60
        return '{} hrs {} mins {} secs'.format(hrs, mins, secs)


class WorkerManager(object):

    def __init__(self, num_workers=8):
        self._num_workers = num_workers

    def _count_externals(self, conn):
        sql = text("select count(*) externals_count "
                   "from externos_sn ")
        return conn.execute(sql).fetchone()['externals_count']

    def run(self):
        num_externals = None
        with DBManager.get_engine().connect() as conn:
            num_externals = self._count_externals(conn)
        matcher = MatchWorker()
        externals_per_worker = math.ceil(num_externals / self._num_workers)
        workers = []
        with ProcessPoolExecutor(max_workers=self._num_workers) as executor:
            for i in range(self._num_workers):
                start = i * externals_per_worker
                workers.append(executor.submit(
                    matcher.match, start, start + externals_per_worker))


manager = WorkerManager(2)
manager.run()

""" matcher = MatchWorker()
matcher.match(500, 550) """
""" with ProcessPoolExecutor(max_workers=2) as executor:
    future = executor.submit(matcher.match)
    future2 = executor.submit(matcher.match)
    print('Is running', future.running())
    print('Is running', future2.running())
    sleep(5)
 """
""" test = Test()
test.run() """

""" data_df = matcher.load_internal_clients()
print(data_df)

data_df = matcher.load_external_clients()
print(data_df) """
