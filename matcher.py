import time
from time import sleep
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from db_manager import DBManager
from fuzzywuzzy import fuzz
from dao_objects import PossibleMatches, Progress


def match(self, internal, external, start_external=0):
    print('Matching...')
    start_time = time.time()
    for i in range(0, len(internal)):
        for j in range(start_external, len(external)):
            internal_name = internal.iloc[i]['nombre']
            external_name = external.iloc[j]['nombre']
            score = fuzz.token_set_ratio(
                internal_name, external_name)
            if j % 20000 == 0:
                print('Internal Progress: {}/{} ({:.0%}) External progress: {}/{} ({:.4%}) Running time: {}'.format(
                    i, len(internal), i/len(internal), j, len(external), j/len(external), self._get_elapsed_time(start_time)))
            if score > 0:
                print(score, internal_name, external_name)
        start_external = 0


class Matcher(object):

    def __init__(self):
        self._engine = DBManager.get_engine()

    def load_internal_clients(self):
        sql = ("select internos_sn_id, " +
               "nombre " +
               "from internos_sn ")
        return pd.read_sql(sql, con=self._engine)

    def load_external_clients(self):
        sql = ("select externos_sn_id, " +
               "nombre " +
               "from externos_sn")
        return pd.read_sql(sql, con=self._engine)

    def match(self):
        internal = self.load_internal_clients()
        external = self.load_external_clients()
        with ProcessPoolExecutor(max_workers=2) as executor:
            future = executor.submit(match, internal, external)
            """ future2 = executor.submit(test, 'adios') """
            print('Is running', future.running())
            sleep(5)
            """ print(future.result())
            print(future2.result()) """

    def test(self, msg):
        print(msg)
        return msg

    def _match(self, internal, external, start_external=0):
        print('Matching...')
        with self._engine.connect() as conn:
            start_time = time.time()
            for i in range(0, len(internal)):
                for j in range(start_external, len(external)):
                    internal_name = internal.iloc[i]['nombre']
                    external_name = external.iloc[j]['nombre']
                    score = fuzz.token_set_ratio(
                        internal_name, external_name)
                    if j % 20000 == 0:
                        print('Internal Progress: {}/{} ({:.0%}) External progress: {}/{} ({:.4%}) Running time: {}'.format(
                            i, len(internal), i/len(internal), j, len(external), j/len(external), self._get_elapsed_time(start_time)))
                    if score > 0:
                        print(score, internal_name, external_name)
                        conn.execute(PossibleMatches.get_table().insert().values(
                            internos_sn_id=internal.iloc[i]['internos_sn_id'].item(
                            ),
                            externos_sn_id=external.iloc[j]['externos_sn_id'].item(
                            ),
                            match_score=score))
                start_external = 0

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


matcher = Matcher()
matcher.match()

""" test = Test()
test.run() """

""" data_df = matcher.load_internal_clients()
print(data_df)

data_df = matcher.load_external_clients()
print(data_df) """
