import time
from time import sleep
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from db_manager import DBManager
from fuzzywuzzy import fuzz
from dao_objects import PossibleMatches, Progress


class MatchWorker(object):

    def load_internal_clients(self):
        sql = ("select internos_sn_id, " +
               "nombre " +
               "from internos_sn ")
        return pd.read_sql(sql, con=DBManager.get_engine())

    def load_external_clients(self, limit, offset):
        sql = ("select externos_sn_id, " +
               "nombre " +
               "from externos_sn " +
               "limit {} offset {}".format(limit, offset))
        return pd.read_sql(sql, con=DBManager.get_engine())

    def test(self, msg):
        print(msg)
        return msg

    def match(self, start_external, end_external, start_internal=0):
        print('Matching...')
        internal = self.load_internal_clients()
        external = self.load_external_clients(
            end_external - start_external, start_external)
        with DBManager.get_engine().connect() as conn:
            start_time = time.time()
            for i in range(0, len(external)):
                for j in range(start_internal, len(internal)):
                    external_name = external.iloc[i]['nombre']
                    internal_name = internal.iloc[j]['nombre']
                    score = fuzz.token_set_ratio(
                        internal_name, external_name)
                    if score > 85:
                        print(score, internal_name, external_name)
                        conn.execute(PossibleMatches.get_table().insert().values(
                            internos_sn_id=internal.iloc[j]['internos_sn_id'].item(
                            ),
                            externos_sn_id=external.iloc[i]['externos_sn_id'].item(
                            ),
                            match_score=score))
                print('External progress: {}/{} ({:.4%}) Running time: {}'.format(
                    i, len(external), i/len(external), self._get_elapsed_time(start_time)))
                start_internal = 0

    def _get_elapsed_time(self, start_time):
        seconds = int(time.time() - start_time)
        hrs = seconds // 3600
        r = seconds % 3600
        mins = r // 60
        secs = r % 60
        return '{} hrs {} mins {} secs'.format(hrs, mins, secs)


""" class MatchWorker(object):

    def run(self):
        with ProcessPoolExecutor(max_workers=2) as executor:
            future = executor.submit(self.test, 'hola')
            future2 = executor.submit(self.test, 'adios')
            print('Is running', future.running())
            sleep(5)
            print(future.result())
            print(future2.result())

    def test(self, msg):
        print(msg)
        return msg """


matcher = MatchWorker()
matcher.match(500, 550)
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
